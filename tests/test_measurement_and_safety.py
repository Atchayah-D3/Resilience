"""Regression tests for measurement and safety defects found in review. Each test names the
wrong result the defect produced, so a regression reads as the failure it would cause."""

import asyncio
import math
import re
import shlex
import tempfile
import time
from pathlib import Path

import pytest
import yaml

from catalog.schema import CATALOG_ROOT, load_catalog, load_reference, run_checks
from resilience_tests.adapters.postgresql import adapter as pg_mod
from resilience_tests.adapters.postgresql.adapter import checksum_failures_since
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.analysis.rto_decomposer import Baseline, decompose, first_write_after, write_recovery
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.injectors import process as process_mod
from resilience_tests.execution.injectors.base import DriverNotAvailable
from resilience_tests.execution.injectors.process import (
    OsSshProcessDriver,
    parse_proc_stat,
    process_gone,
    unit_stop_mode,
)
from resilience_tests.execution.remote import RemoteResult
from resilience_tests.execution.workload.driver import WorkloadDriver
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.observability.event_stream import Event, EventStream
from tests.test_adapter_seam import WORKLOAD, FakeAdapter, FakeSession

PROFILE = load_profile("e2-dedicated-vm")
NODE = PROFILE.nodes[0]
S = 1_000_000_000
T0 = 100 * S
BASE = Baseline(tps=1000.0, p99_ms=10.0)


def probe(t_end_s, ok, t_start_s=None):
    data = {"ok": ok}
    if t_start_s is not None:
        data["t_start_mono_ns"] = int(t_start_s * S)
    return Event(int(t_end_s * S), 0.0, "write_prober", "write_probe", data)


# --- rto_first_write_s: in-flight probes and unobserved outages --------------------------


def test_probe_in_flight_at_t0_is_not_a_recovery():
    """Was: a probe started 5 ms before T0 and completing 3 ms after it gave an RTO of 0.003 s
    for an 8.4 s outage."""
    ev = [probe(99.8, True, 99.79), probe(100.003, True, 99.995)]
    ev += [probe(100.2 + 0.2 * i, False, 100.2 + 0.2 * i - 0.1) for i in range(40)]
    ev += [probe(108.4, True, 108.39)]
    r = write_recovery(ev, T0)
    assert r.outage_observed and r.first_write_s == pytest.approx(8.4)


def test_recovery_probe_must_start_after_the_first_failure():
    # the ok at 101.0 started before the failure at 100.5 completed: it predates the outage
    ev = [probe(100.5, False, 100.3), probe(101.0, True, 100.4), probe(102.0, True, 101.9)]
    assert write_recovery(ev, T0).first_write_s == pytest.approx(2.0)


def test_expected_outage_never_observed_is_not_measured():
    """A kill the probes never saw interrupt service yields no gap, not a ~0 s pass."""
    ev = [probe(100.2, True, 100.05), probe(100.4, True, 100.25)]
    d = decompose(ev, T0, BASE, clustered=False, expect_outage=True)
    assert d.rto_first_write_s is NOT_MEASURED and not d.outage_observed


def test_no_outage_expected_reports_the_first_write():
    """A config reload should not interrupt service; its first write is the (tiny) gap."""
    ev = [probe(100.2, True, 100.05)]
    d = decompose(ev, T0, BASE, clustered=False, expect_outage=False)
    assert d.rto_first_write_s == pytest.approx(0.2) and not d.outage_observed


def test_first_write_after_uses_attempt_start_derived_from_latency():
    # older streams carry only latency_ms: start = completion - latency
    ev = [Event(int(100.01 * S), 0.0, "write_prober", "write_probe", {"ok": True, "latency_ms": 50.0}),
          Event(int(100.3 * S), 0.0, "write_prober", "write_probe", {"ok": True, "latency_ms": 2.0})]
    assert first_write_after(ev, T0) == pytest.approx(0.3)


# --- process kill confirmation ------------------------------------------------------------

STAT = "4242 (postgres) S 1 4242 4242 0 -1 4194560 " + " ".join(["0"] * 12) + " 123456 0 0"


def stat_with(state="S", start="123456", comm="postgres"):
    return f"4242 ({comm}) {state} 1 4242 4242 0 -1 4194560 " + " ".join(["0"] * 12) + f" {start} 0 0"


def test_proc_stat_parsing_survives_odd_command_names():
    assert parse_proc_stat(stat_with(comm="post gres) x")) == ("S", "123456")
    assert parse_proc_stat("") is None


@pytest.mark.parametrize("after,gone", [
    ("", True),                               # no such process
    (stat_with(state="Z"), True),             # zombie awaiting reap
    (stat_with(start="999999"), True),        # PID reused by a new process
    (stat_with(), False),                     # still the same, running process
])
def test_process_gone(after, gone):
    assert process_gone(("S", "123456"), after) is gone


class ScriptedHost:
    """Fake SSH host: answers by substring, records every command."""

    def __init__(self, answers, calls):
        self.answers, self.calls = answers, calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def connect(self):
        self.calls.append("<connect>")

    async def close(self):
        self.calls.append("<close>")

    async def run(self, command, *, timeout_s, check=True):
        self.calls.append(command)
        for key, value in self.answers.items():
            if key in command:
                out = value() if callable(value) else value
                return RemoteResult(0, out, "")
        return RemoteResult(0, "", "")


def use_host(monkeypatch, module, answers):
    calls: list[str] = []
    monkeypatch.setattr(module, "RemoteHost", lambda *a, **k: ScriptedHost(answers, calls))
    return calls


def test_kill_that_did_not_land_is_an_error(monkeypatch):
    """Was: `kill -0` as the SSH user fails with EPERM on a postgres-owned process and printed
    'gone' whether or not the postmaster had died."""
    monkeypatch.setattr(process_mod, "KILL_CONFIRM_TIMEOUT_S", 0.3)
    monkeypatch.setattr(process_mod, "KILL_CONFIRM_POLL_S", 0.05)
    use_host(monkeypatch, process_mod, {"postmaster.pid": "4242", "/proc/4242/stat": STAT})
    with pytest.raises(RuntimeError, match="still alive"):
        asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").inject(NODE))


def test_kill_is_confirmed_as_root(monkeypatch):
    state = {"killed": False}

    def kill():
        state["killed"] = True
        return ""

    calls = use_host(monkeypatch, process_mod, {
        "postmaster.pid": "4242", "systemctl kill": kill,
        "/proc/4242/stat": lambda: "" if state["killed"] else STAT,
    })
    detail = asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").inject(NODE))
    assert detail["pid"] == 4242 and "death_confirmed_s" in detail
    assert all(c.startswith("sudo -n --") for c in calls if "/proc/4242/stat" in c)


# --- NL-M-06: fast shutdown ---------------------------------------------------------------

PG_CTL = "/usr/lib/postgresql/17/bin/pg_ctl"


@pytest.mark.parametrize("exec_stop,kill_signal,mode", [
    (f"{{ path={PG_CTL} ; argv[]={PG_CTL} -D ${{PGDATA}} stop ; ignore_errors=no }}", "15", "fast"),
    (f"{{ path={PG_CTL} ; argv[]={PG_CTL} -D ${{PGDATA}} stop -m smart ; ignore_errors=no }}", "15", "smart"),
    (f"{{ path={PG_CTL} ; argv[]={PG_CTL} stop --mode=immediate ; x }}", "15", "immediate"),
    (f"{{ path={PG_CTL} ; argv[]={PG_CTL} stop -mf ; x }}", "15", "fast"),
    ("{ path=/opt/wrapper.sh ; argv[]=/opt/wrapper.sh stop ; x }", "15", None),
    ("", "15", "smart"),   # no ExecStop: SIGTERM is a smart shutdown
    ("", "2", "fast"),     # SIGINT is a fast shutdown
])
def test_unit_stop_mode(exec_stop, kill_signal, mode):
    assert unit_stop_mode(exec_stop, kill_signal) == mode


def test_restart_preflight_refuses_a_non_fast_shutdown(monkeypatch):
    use_host(monkeypatch, process_mod, {
        "is-active": "active", "postmaster.pid": "4242", "show -p Restart": "on-failure",
        "show -p ExecStop": "", "show -p KillSignal": "15",
    })
    with pytest.raises(DriverNotAvailable, match="smart shutdown.*-m fast"):
        asyncio.run(OsSshProcessDriver(PROFILE, "service_restart").preflight(NODE))


# --- NL-M-07: the reload is reverted to the pre-fault value -------------------------------


def auto_conf_host(monkeypatch, value):
    """Fake host holding the parameter's postgresql.auto.conf value; ALTER SYSTEM updates it."""
    state = {"value": value}

    class Host(ScriptedHost):
        async def run(self, command, *, timeout_s, check=True):
            self.calls.append(command)
            if "is-active" in command:
                return RemoteResult(0, "active", "")
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            if "show -p Restart" in command:
                return RemoteResult(0, "on-failure", "")
            if "pg_file_settings" in command:
                return RemoteResult(0, state["value"] or "", "")
            if "ALTER SYSTEM RESET" in command:
                state["value"] = None
            elif "ALTER SYSTEM SET" in command:
                # the literal arrives wrapped in several layers of shell quoting
                state["value"] = re.search(r"ALTER SYSTEM SET \w+ =\W*([0-9]+[a-z]*)", command).group(1)
            return RemoteResult(0, "t", "")

    calls: list[str] = []
    monkeypatch.setattr(process_mod, "RemoteHost", lambda *a, **k: Host({}, calls))
    return state, calls


def test_reload_revert_restores_the_customers_own_value(monkeypatch):
    """Was: RESET removed a value the customer had set with ALTER SYSTEM."""
    state, _ = auto_conf_host(monkeypatch, "1s")
    drv = OsSshProcessDriver(PROFILE, "config_reload")
    inject = asyncio.run(drv.inject(NODE))
    assert state["value"] == "250ms" and inject["auto_conf_prior"] == "1s"
    out = asyncio.run(drv.revert(NODE, {"preflight": {"auto_conf_prior": "1s"}, "inject": inject}))
    assert state["value"] == "1s" and out["restored"] == "1s"


def test_reload_psql_names_the_harness_database(monkeypatch):
    """Was: no -d, so psql connected to a database named after the OS user -- which fails on
    any target where that database does not exist."""
    _, calls = auto_conf_host(monkeypatch, None)
    asyncio.run(OsSshProcessDriver(PROFILE, "config_reload").inject(NODE))
    psql = [c for c in calls if "psql" in c]
    assert psql and all(f"-d {NODE.db.dbname}" in c for c in psql)


def test_reload_revert_resets_when_nothing_was_set_before(monkeypatch):
    state, _ = auto_conf_host(monkeypatch, None)
    drv = OsSshProcessDriver(PROFILE, "config_reload")
    inject = asyncio.run(drv.inject(NODE))
    asyncio.run(drv.revert(NODE, {"preflight": {"auto_conf_prior": None}, "inject": inject}))
    assert state["value"] is None


def test_reload_revert_is_idempotent_and_never_touches_foreign_values(monkeypatch):
    state, calls = auto_conf_host(monkeypatch, "5s")
    drv = OsSshProcessDriver(PROFILE, "config_reload")
    # already at the pre-fault value: nothing to do
    assert asyncio.run(drv.revert(NODE, {"preflight": {"auto_conf_prior": "5s"}}))["action"] == "none"
    # pre-fault value unknown and the value present is not ours: leave it alone
    assert asyncio.run(drv.revert(NODE, {}))["action"] == "none"
    assert state["value"] == "5s" and not any("ALTER SYSTEM" in c for c in calls)


# --- pg_amcheck scope, checksum delta -----------------------------------------------------


def test_integrity_check_never_installs_and_never_scans_every_database(monkeypatch):
    """Was: `pg_amcheck --all --install-missing` created amcheck in every customer database."""
    calls = use_host(monkeypatch, pg_mod, {"pg_extension": "1", "pg_amcheck": ""})

    async def stats(self):
        return {"resilience": (0, "")}

    monkeypatch.setattr(pg_mod.PostgreSQLAdapter, "_checksum_stats", stats)
    result = asyncio.run(pg_mod.PostgreSQLAdapter(NODE).integrity_check(timeout_s=5))
    amcheck = next(c for c in calls if "pg_amcheck" in c)
    assert "--install-missing" not in amcheck and "--all" not in amcheck and "-d resilience" in amcheck
    assert result.structural_errors == 0 and result.detail["databases"] == ["resilience"]


def test_integrity_check_refuses_when_amcheck_is_missing(monkeypatch):
    use_host(monkeypatch, pg_mod, {"pg_extension": "0"})
    with pytest.raises(RuntimeError, match="amcheck is not installed"):
        asyncio.run(pg_mod.PostgreSQLAdapter(NODE).integrity_check(timeout_s=5))


def test_skipped_database_is_not_a_clean_result(monkeypatch):
    use_host(monkeypatch, pg_mod, {"pg_extension": "1",
                                   "pg_amcheck": 'pg_amcheck: warning: skipping database "resilience"'})
    with pytest.raises(RuntimeError, match="skipped"):
        asyncio.run(pg_mod.PostgreSQLAdapter(NODE).integrity_check(timeout_s=5))


def test_checksum_failures_are_a_per_run_delta():
    """Was: the cumulative counter, so one historic failure failed every later run."""
    before = {"a": (3, "2026-01-01"), "b": (0, "2026-01-01")}
    assert checksum_failures_since(before, {"a": (3, "2026-01-01"), "b": (0, "2026-01-01")}) == 0
    assert checksum_failures_since(before, {"a": (5, "2026-01-01"), "b": (0, "2026-01-01")}) == 2
    # statistics discarded by crash recovery: everything now shown happened during the run
    assert checksum_failures_since(before, {"a": (1, "2026-09-22"), "b": (0, "2026-09-22")}) == 1
    assert checksum_failures_since(before, {"a": (1, "2026-01-01"), "b": (0, "2026-01-01")}) == 1
    # no baseline: count everything (fail closed)
    assert checksum_failures_since(None, {"a": (3, "")}) == 3


# --- workload driver: fatal journal errors, real connection drops -------------------------


def drive(adapter, seconds, journals_cls=MarkerJournals):
    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream = EventStream(d / "events.jsonl")
            journals = journals_cls(d)
            driver = WorkloadDriver(adapter, WORKLOAD, journals, stream)
            driver.begin_window()
            await driver.start()
            await asyncio.sleep(seconds)
            window = driver.end_window()
            await driver.stop()
            journals.close()
            stream.close()
            return driver, window, stream.events()
    return asyncio.run(go())


def test_journal_failure_stops_the_driver_and_is_reported():
    """Was: the exception killed the worker task silently; the run carried on."""
    class FailingJournals(MarkerJournals):
        async def written(self, seq, uuid, t_pre):
            if seq > 20:
                raise OSError(28, "No space left on device")
            await super().written(seq, uuid, t_pre)

    driver, window, events = drive(FakeAdapter(NODE), 0.5, FailingJournals)
    assert driver.failure and "No space left" in driver.failure
    assert any(e.kind == "fatal" for e in events)


def test_idle_connection_closed_by_the_server_counts_as_a_drop():
    """Was: a connection lost between transactions was silently re-opened; NL-M-07's
    'zero dropped connections' could not see it."""
    class Flaky(FakeAdapter):
        engine = "flaky"

        def __init__(self, node):
            super().__init__(node)
            self.opened = 0

        async def session(self, endpoint=None, timeout_s=5.0):
            self.opened += 1
            if self.opened in (70, 71, 72):
                raise ConnectionRefusedError("the database system is starting up")
            s = FakeSession(self.store)
            if self.opened <= 64:
                asyncio.get_running_loop().call_later(0.2, lambda: setattr(s, "_closed", True))
            return s

    _, window, _ = drive(Flaky(NODE), 2.0)
    assert window.drops == 64 and window.reconnects == 64 and window.connect_failures == 3


# --- catalog checks during build-out ------------------------------------------------------


def test_partial_mode_passes_the_current_catalog_but_not_the_release_gate():
    partial = {r.number: r for r in run_checks(load_catalog(), load_reference(), complete=False)}
    full = {r.number: r for r in run_checks(load_catalog(), load_reference())}
    assert all(r.passed for r in partial.values())
    assert not full[6].passed


def test_partial_mode_still_fails_on_a_contradiction():
    from catalog.schema import Catalog

    catalog = load_catalog()
    ref = load_reference()
    shrunk = ref.model_copy(update={"categories": {**ref.categories, "NL-M": {"count": 1, "P0": 0}}})
    results = {r.number: r for r in run_checks(Catalog(catalog.scenarios), shrunk, complete=False)}
    assert not results[6].passed and any(d.startswith("NL-M:") for d in results[6].detail)


# --- revert budget fits inside the timeout that wraps it ----------------------------------


def test_revert_budget_is_smaller_than_the_timeout_wrapping_it():
    """Was: a revert could need ~730 s inside a 300 s timeout inside a 600 s cleanup phase, so
    a node that was merely slow to start was journalled as revert_failed."""
    from resilience_tests.control.killswitch import REVERT_TIMEOUT_S
    from resilience_tests.control.profile import ENVS_ROOT, load_profile
    from resilience_tests.execution.injectors.process import REVERT_TOTAL_BUDGET_S, SSH_TIMEOUT_S

    assert REVERT_TOTAL_BUDGET_S + SSH_TIMEOUT_S <= REVERT_TIMEOUT_S
    for path in sorted(ENVS_ROOT.glob("*.yaml")):
        profile = load_profile(path)
        assert REVERT_TIMEOUT_S <= profile.phase_timeouts_s["cleanup"], path.name


def test_revert_of_a_service_stuck_activating_stops_inside_its_budget(monkeypatch):
    monkeypatch.setattr(process_mod, "REVERT_TOTAL_BUDGET_S", 0.6)
    monkeypatch.setattr(process_mod, "REVERT_SETTLE_POLL_S", 0.05)
    use_host(monkeypatch, process_mod, {"is-active": "activating"})   # never settles
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="activating"):
        asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").revert(NODE))
    assert time.monotonic() - started < 3.0   # one shared budget, not one per step


# --- sampler honesty and the percentile rank ----------------------------------------------


def test_p99_is_nearest_rank():
    """Was: int(0.99 * n), one rank high -- at n = 100 it returned the maximum. An inflated
    baseline p99 widens the recovery SLO ceiling, making rto_to_slo_s optimistic."""
    from resilience_tests.execution.workload.driver import p99

    for n in (100, 200, 1000, 24000):
        assert p99([float(i) for i in range(1, n + 1)]) == math.ceil(0.99 * n)
    assert p99([5.0]) == 5.0 and p99([]) is None


def test_samples_report_the_interval_they_actually_measured(monkeypatch):
    """Was: tps divided by the nominal 1 s while the real interval stretched under load, so
    reported throughput was inflated by exactly the overrun."""
    from resilience_tests.execution.workload import driver as driver_mod

    monkeypatch.setattr(driver_mod, "SAMPLE_INTERVAL_S", 0.2)

    class Stalling(FakeAdapter):
        engine = "stalling"

        async def session(self, endpoint=None, timeout_s=5.0):
            return FakeSession(self.store)

    async def stall():
        await asyncio.sleep(0.5)
        time.sleep(0.4)          # block the event loop, as a recovery storm does

    def drive_with_stall():
        async def go():
            with tempfile.TemporaryDirectory() as tmp:
                d = Path(tmp)
                stream = EventStream(d / "events.jsonl")
                journals = MarkerJournals(d)
                driver = WorkloadDriver(Stalling(NODE), WORKLOAD, journals, stream)
                await driver.start()
                asyncio.get_running_loop().create_task(stall())
                await asyncio.sleep(1.4)
                await driver.stop()
                journals.close()
                stream.close()
                return [e for e in stream.events() if e.kind == "sample"]
        return asyncio.run(go())

    samples = drive_with_stall()
    assert len(samples) >= 3
    for previous, sample in zip(samples, samples[1:]):
        real = (sample.t_mono_ns - previous.t_mono_ns) / 1e9
        assert sample.data["interval_s"] == pytest.approx(real, abs=0.02)
        assert sample.data["tps"] == pytest.approx(sample.data["commits"] / sample.data["interval_s"], rel=1e-6)
    assert max(s.data["interval_s"] for s in samples) > 0.35   # the stall was reported, not hidden


def test_honest_sample_metadata_does_not_certify_a_recovery():
    """The end the decomposer sees: at 71% of baseline no SLO window may be found."""
    from resilience_tests.analysis.rto_decomposer import slo_recovery

    base = Baseline(tps=200.0, p99_ms=10.0)
    honest = [Event(int((100 + 1.4 * i) * S), 0.0, "workload", "sample",
                    {"interval_s": 1.4, "commits": 200, "tps": 200 / 1.4, "p99_ms": 10.0}) for i in range(1, 81)]
    assert slo_recovery(honest, T0, base) == (None, None)
    # the same run mislabelled with the nominal interval is what used to pass
    inflated = [Event(e.t_mono_ns, 0.0, "workload", "sample", {**e.data, "interval_s": 1.0, "tps": 200.0})
                for e in honest]
    assert slo_recovery(inflated, T0, base) != (None, None)


# --- the outage must be attributable to the fault (M1) ------------------------------------


def started(t_start, t_end, ok):
    return Event(int(t_end * S), 0.0, "write_prober", "write_probe",
                 {"ok": ok, "t_start_mono_ns": int(t_start * S)})


def test_a_probe_failing_from_before_t0_does_not_open_the_outage():
    """Was: the first failure was picked by completion time, so a probe already in flight at
    T0 started the clock -- a 40 s outage was reported as 0.22 s, and NL-M-06 passed."""
    ev = [started(99.98, 100.02, False)]                                   # in flight at T0
    ev += [started(100.2, 100.22, True), started(100.4, 100.62, True)]     # still serving
    ev += [started(100.8 + 0.2 * i, 101.0 + 0.2 * i, False) for i in range(200)]   # the real outage
    ev += [started(141.0, 141.02, True)]
    r = write_recovery(ev, T0)
    assert r.outage_observed and r.first_write_s == pytest.approx(41.02, abs=0.01)


def test_a_failure_predating_the_fault_does_not_satisfy_the_outage_guard():
    ev = [started(99.9, 100.05, False)] + [started(100.2 + 0.2 * i, 100.22 + 0.2 * i, True) for i in range(20)]
    d = decompose(ev, T0, BASE, clustered=False, expect_outage=True)
    assert not d.outage_observed and d.rto_first_write_s is NOT_MEASURED


# --- a connect timeout is not a statement timeout (M3) ------------------------------------


def test_client_sessions_do_not_inherit_the_connect_timeout_as_a_statement_timeout(monkeypatch):
    """Was: session(timeout_s=2.0) set command_timeout=2 s, so a commit slower than that became
    indeterminate -- dropping real losses out of rpo_txn and counting a latency spike as a
    dropped connection."""
    captured = {}

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(pg_mod.asyncpg, "connect", fake_connect)
    adapter = pg_mod.PostgreSQLAdapter(NODE)
    asyncio.run(adapter.session(timeout_s=2.0))
    assert captured["timeout"] == 2.0 and captured["command_timeout"] is None
    asyncio.run(adapter._connect(timeout_s=30.0))          # the harness's own queries stay bounded
    assert captured["command_timeout"] == 30.0


# --- the kill covers the unit's cgroup (Arch §5) ------------------------------------------


def test_kill_targets_the_whole_unit_not_just_the_postmaster(monkeypatch):
    """Was: `kill -9 <postmaster>` left backends in the cgroup, so the service manager held
    the restart until they exited -- that wait lands inside NL-C-01's measured RTO."""
    state = {"killed": False}

    def kill():
        state["killed"] = True
        return ""

    calls = use_host(monkeypatch, process_mod, {
        "postmaster.pid": "4242", "systemctl kill": kill,
        "/proc/4242/stat": lambda: "" if state["killed"] else STAT,
    })
    detail = asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").inject(NODE))
    kill_cmd = next(c for c in calls if "systemctl kill" in c)
    assert f"-s SIGKILL {NODE.service}" in kill_cmd and "sudo -n --" in kill_cmd
    assert not any(c for c in calls if "kill -9" in c)
    assert detail["pid"] == 4242 and "death_confirmed_s" in detail


# --- the kill switch never claims a site is clean it did not inspect (S1) -----------------


def test_kill_switch_reports_a_missing_ledger_instead_of_no_injections(tmp_path, capsys):
    from resilience_tests.control import killswitch

    profile = PROFILE.model_copy(
        update={"driver_host": PROFILE.driver_host.model_copy(update={"run_dir": str(tmp_path / "elsewhere")})})
    ledger = killswitch.ledger_for(profile)
    assert not ledger.path.parent.exists()    # reading a ledger creates nothing
    monkey = killswitch.load_profile
    killswitch.load_profile = lambda env: profile
    try:
        code = killswitch.main(["--env", "whatever"])
    finally:
        killswitch.load_profile = monkey
    err = capsys.readouterr().err
    assert code == 2 and "NO LEDGER" in err and "nothing has been verified as clean" in err


# --- citations name their document (C1) ---------------------------------------------------


def test_check5_distinguishes_the_two_documents():
    """Was: the document was stripped, so 'Arch §5' was validated against the Framework's
    contents and a real 'Arch §3.4' was reported as broken."""
    from catalog.schema import Catalog, Scenario

    base = yaml.safe_load((CATALOG_ROOT / "NL" / "NL-C-01.yaml").read_text())

    def check(description):
        sc = Scenario.model_validate({**base, "description": description})
        results = {r.number: r for r in run_checks(Catalog({sc.id: sc}), load_reference())}
        return results[5]

    assert check("see Arch §4.3 for the compound rule").passed          # real Architecture heading
    assert check("see Framework §13.1").passed
    bad_arch = check("see Arch §13.7")                                   # Framework heading, not Arch
    assert not bad_arch.passed and "Arch §13.7" in bad_arch.detail[0]
    ambiguous = check("see §13.1")
    assert not ambiguous.passed and "does not say which document" in ambiguous.detail[0]


# --- a sample that averages over a dip cannot certify compliance (M2) ---------------------


def wsample(t_s, interval_s, tps, p99=10.0):
    return Event(int(t_s * S), 0.0, "workload", "sample",
                 {"interval_s": interval_s, "tps": tps, "p99_ms": p99})


def test_a_starved_sampler_cannot_certify_a_window_it_averaged_over():
    """Was: one 6 s sample averaging above the floor certified a 60 s window that contained a
    4 s outage the sample had averaged away."""
    from resilience_tests.analysis.rto_decomposer import slo_recovery

    base = Baseline(tps=200.0, p99_ms=10.0)
    ev = [wsample(100 + i, 1.0, 200.0) for i in range(1, 21)]
    ev += [wsample(126.0, 6.0, 170.0)]                       # hides a dip inside it
    ev += [wsample(126 + i, 1.0, 200.0) for i in range(1, 41)]
    assert slo_recovery(ev, T0, base) == (None, None)
    # samples missing from the record still break the run, as before
    gapped = [wsample(100 + i, 1.0, 200.0) for i in range(1, 31)]
    gapped += [wsample(135 + i, 1.0, 200.0) for i in range(1, 41)]
    assert slo_recovery(gapped, T0, base) == (None, None)


def test_rto_components_sum_to_the_rto():
    """Was: t_warm_s was published cumulatively from T0, so T_reconnect + T_warm double-counted
    the reconnect and the five Framework §6.3 components did not sum."""
    base = Baseline(tps=200.0, p99_ms=10.0)
    ev = [started(100.1, 100.2, False), started(124.58, 124.6, True)]
    ev += [wsample(125 + i, 1.0, 200.0) for i in range(1, 62)]
    m = decompose(ev, T0, base, clustered=False).as_measured()
    assert m["t_reconnect_s"] + m["t_warm_s"] == pytest.approx(m["rto_to_slo_s"])
    # an unmeasured reconnect leaves the warm-up unmeasurable, never a bare subtraction
    nothing = decompose([wsample(100 + i, 1.0, 200.0) for i in range(1, 62)], T0, base,
                        clustered=False, expect_outage=True)
    assert nothing.as_measured()["t_warm_s"] is NOT_MEASURED


# --- detection is not the replacement's startup (mttd_s) ----------------------------------


def test_a_killed_process_reports_no_detection_time():
    """Was: the replacement postmaster's 'automatic recovery in progress' was timed as if the
    killed process had detected the fault."""
    from resilience_tests.analysis.rto_decomposer import mttd_from_log

    adapter = pg_mod.PostgreSQLAdapter(NODE)
    log = [Event(int(104.0 * S), 0.0, "log_tailer", "log_line",
                 {"line": "database system was not properly shut down; automatic recovery in progress"})]
    # a SIGKILL leaves no line from the victim: detection is absent, restart is reported
    assert mttd_from_log(log, T0, adapter.fault_detection_log_patterns()) is None
    assert mttd_from_log(log, T0, adapter.recovery_start_log_patterns()) == pytest.approx(4.0)
    d = decompose(log, T0, BASE, clustered=False,
                  detection_patterns=adapter.fault_detection_log_patterns(),
                  recovery_patterns=adapter.recovery_start_log_patterns())
    assert d.components["mttd_s"] is NOT_MEASURED
    assert d.components["recovery_started_s"] == pytest.approx(4.0)
    # a restart DOES let the engine log that it noticed
    shutdown = [Event(int(101.0 * S), 0.0, "log_tailer", "log_line",
                      {"line": "received fast shutdown request"})]
    assert mttd_from_log(shutdown, T0, adapter.fault_detection_log_patterns()) == pytest.approx(1.0)


# --- a latched unit, and profile fields as literals ---------------------------------------


def test_revert_clears_a_latched_unit_before_starting_it(monkeypatch):
    """Was: after repeated crashes systemd leaves the unit failed on its start limit and
    refuses `start`, so neither revert nor the kill switch could put the service back."""
    state = {"reset": False}

    def is_active():
        return "active" if state["reset"] else "failed"

    def reset():
        state["reset"] = True
        return ""

    calls = use_host(monkeypatch, process_mod, {
        "reset-failed": reset, "is-active": is_active, "systemctl start": "",
    })
    detail = asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").revert(NODE))
    assert detail["reset_failed_issued"] is True and detail["state"] == "active"
    assert calls.index(next(c for c in calls if "reset-failed" in c)) \
        < calls.index(next(c for c in calls if "systemctl start" in c))


def test_profile_fields_are_quoted_in_remote_commands(monkeypatch):
    """A profile field is data. Interpolated raw into a root shell command, a path with a
    space (or a ';') becomes syntax."""
    node = NODE.model_copy(update={"service": "odd name.service", "pgdata": "/var/lib/odd dir/pgdata",
                                   "pg_bin": "/opt/odd dir/bin", "log_file": "/var/log/odd dir/pg.log"})
    calls = use_host(monkeypatch, process_mod, {"postmaster.pid": "4242", "/proc/4242/stat": STAT,
                                                "show -p Restart": "on-failure", "is-active": "active",
                                                "pg_file_settings": ""})
    driver = OsSshProcessDriver(PROFILE, "config_reload")
    asyncio.run(driver.preflight(node))
    asyncio.run(driver.revert(node, {}))
    for command in calls:
        assert "odd name.service" not in command or "'odd name.service'" in command
        assert "odd dir" not in command or "'" in command
    # the inner command of each sudo wrapper still parses as one argv
    for command in calls:
        argv = shlex.split(command)
        if argv[:2] == ["sudo", "-n"] and "-c" in argv:
            shlex.split(argv[argv.index("-c") + 1])


def test_a_leaked_probe_value_is_not_adopted_as_the_operators_setting(monkeypatch):
    """Seen in the field: a previous run left log_min_duration_statement=250ms in
    postgresql.auto.conf. Preflight must not record that as the value to restore, or the
    harness's own leftover becomes permanent."""
    state, _ = auto_conf_host(monkeypatch, process_mod.RELOAD_PROBE_VALUE)   # the leftover
    driver = OsSshProcessDriver(PROFILE, "config_reload")
    pre = asyncio.run(driver.preflight(NODE))
    assert pre["auto_conf_prior"] is None and pre["auto_conf_leftover"] == process_mod.RELOAD_PROBE_VALUE
    inject = asyncio.run(driver.inject(NODE))
    asyncio.run(driver.revert(NODE, {"preflight": pre, "inject": inject}))
    assert state["value"] is None      # cleared, not restored to the leftover

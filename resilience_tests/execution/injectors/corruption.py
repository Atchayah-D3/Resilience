"""OS/SSH driver for data-file corruption (Arch §5 row "Data / WAL corruption: targeted byte
writes ... build -- must be precise"; Framework NL-I).

Corrupts ONE byte in ONE page of the harness-owned corruption target, then hands the database
back for the run to read. Precision and proof come before anything is measured:

  1. preflight   the adapter recreates the target and names a populated page in its file; the
                 path is checked to be a relation file inside the data directory
  2. stop        the service is stopped through its manager, with a FAST shutdown, and
                 pg_controldata must report a CLEAN shutdown. After an unclean stop, crash
                 recovery could restore the page from a full-page image in the WAL and quietly
                 undo the fault -- a run that then "passes" would have tested nothing.
  3. corrupt     the byte is read, inverted, written in place and forced to disk, then read
                 back: it must have changed
  4. verify      pg_checksums (Arch §10.1, offline checksum verification) must report a
                 checksum failure at exactly that block of exactly that relation; its own output
                 is kept in the evidence, so an abort here always says what the tool said
  5. start       the service is started again; T0 is when it is back, with the damaged page on
                 disk and nothing yet having read it

Any step that does not hold raises FaultNotLanded: the run is aborted, never scored. Revert
restarts the service if it is down and drops the target -- DROP never reads the pages, so a
damaged relation is removed safely, also by the kill switch after a harness crash.
"""

from __future__ import annotations

import re
import shlex
import time
from collections.abc import Mapping
from typing import Any

from resilience_tests.control.profile import Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultNotLanded, register
from resilience_tests.execution.injectors.process import (
    REVERT_TOTAL_BUDGET_S,
    SSH_TIMEOUT_S,
    OsSshProcessDriver,
    unit_stop_mode,
)
from resilience_tests.execution.remote import RemoteHost, as_root, as_user

q = shlex.quote

# a relation's main fork lives at base/<database oid>/<filenode> under the data directory
_RELATION_PATH_RE = re.compile(r"^base/\d+/(\d+)$")
SEGMENT_BYTES = 1024 ** 3          # files are split into 1 GB segments; the target is far smaller
_CLUSTER_STATE_RE = re.compile(r"Database cluster state:\s*(.+)")
# pg_checksums: 'checksum verification failed in file "<path>", block 17: calculated ...'
_CHECKSUM_FAILURE_RE = re.compile(r'checksum verification failed in file "([^"]+)", block (\d+)')
# printed only when pg_checksums actually scanned: "Bad checksums:  1"
_CHECKSUM_SUMMARY_RE = re.compile(r"Bad checksums:\s*\d+")


def parse_checksum_failures(output: str) -> list[tuple[str, int]]:
    return [(m.group(1), int(m.group(2))) for m in _CHECKSUM_FAILURE_RE.finditer(output)]


def parse_cluster_state(output: str) -> str | None:
    m = _CLUSTER_STATE_RE.search(output)
    return m.group(1).strip() if m else None


class OsSshCorruptionDriver(OsSshProcessDriver):
    """`data_corruption` -- flip one byte in one page of the harness's own relation."""

    fault_types = frozenset({"data_corruption"})
    driver_name = "os_ssh"

    def _tool(self, node: Node, name: str) -> str:
        return q(f"{node.pg_bin}/{name}")

    async def preflight(self, node: Node) -> dict[str, Any]:
        detail: dict[str, Any] = {"service": node.service}
        async with RemoteHost(node.ssh) as host:
            active = (await host.run(f"systemctl is-active {q(node.service)}",
                                     timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
            if active != "active":
                raise DriverNotAvailable(f"{node.name}: service {node.service} is {active!r}")
            # A smart shutdown waits for the workload's connections and is then killed by the
            # service manager -- an unclean stop, which step 2 would refuse anyway.
            exec_stop = await self._unit(host, node, "ExecStop")
            kill_signal = await self._unit(host, node, "KillSignal")
            mode = unit_stop_mode(exec_stop, kill_signal)
            if mode != "fast":
                raise DriverNotAvailable(
                    f"{node.name}: {node.service} stops PostgreSQL with a {mode or 'undeterminable'} shutdown; "
                    "data_corruption needs a FAST, clean shutdown so recovery cannot rewrite the damaged page")
            for tool in ("pg_controldata", "pg_checksums"):
                found = await host.run(as_user(node.os_user, f"test -x {self._tool(node, tool)}"),
                                       timeout_s=SSH_TIMEOUT_S, check=False)
                if found.exit_status != 0:
                    raise DriverNotAvailable(f"{node.name}: {node.pg_bin}/{tool} is not available")
        target = await self._database_adapter(node).prepare_corruption_target()
        self._check_target(node, target)
        self._target = target
        detail.update(stop_mode=mode, target=target)
        return detail

    def _check_target(self, node: Node, target: Mapping[str, Any]) -> None:
        """Refuse anything but a byte inside a page of the target's own main file."""
        m = _RELATION_PATH_RE.match(str(target.get("relation_path", "")))
        if m is None or int(m.group(1)) != int(target["filenode"]):
            raise DriverNotAvailable(f"corruption target path {target.get('relation_path')!r} is not the "
                                     f"main file of filenode {target.get('filenode')}")
        if not str(target["relation"]).startswith("resilience."):
            raise DriverNotAvailable(f"refusing to corrupt {target['relation']}: not a harness-owned relation")
        if not 0 < int(target["block"]) < int(target["pages"]):
            raise DriverNotAvailable(f"block {target['block']} is not a populated page of {target['relation']}")
        if not 24 <= int(target["byte_in_page"]) < int(target["block_size"]):  # never the page header
            raise DriverNotAvailable(f"byte {target['byte_in_page']} is outside the page's tuple area")
        if self._offset(target) >= SEGMENT_BYTES:
            raise DriverNotAvailable("the chosen page is beyond the relation's first segment file")

    @staticmethod
    def _offset(target: Mapping[str, Any]) -> int:
        return int(target["block"]) * int(target["block_size"]) + int(target["byte_in_page"])

    async def inject(self, node: Node) -> dict[str, Any]:
        target = getattr(self, "_target", None)
        if target is None:
            raise DriverNotAvailable("data_corruption needs preflight to have located its target")
        path = f"{node.pgdata.rstrip('/')}/{target['relation_path']}"
        offset = self._offset(target)
        read_byte = as_user(node.os_user, f"od -An -tx1 -j {offset} -N1 {q(path)}")
        detail: dict[str, Any] = {"action": "flip one byte in one heap page", "relation": target["relation"],
                                  "relation_path": target["relation_path"], "file": path,
                                  "block": target["block"], "byte_offset": offset,
                                  "integrity_exclusions": [target["relation"]]}
        async with RemoteHost(node.ssh) as host:
            size = (await host.run(as_user(node.os_user, f"stat -c %s {q(path)}"), timeout_s=SSH_TIMEOUT_S)).stdout
            if not size.strip().isdigit() or int(size) <= offset:
                raise FaultNotLanded(f"{path} is {size.strip()!r} bytes; offset {offset} is not inside it", detail)

            await host.run(as_root(f"systemctl stop {q(node.service)}"), timeout_s=REVERT_TOTAL_BUDGET_S)
            controldata = (await host.run(
                as_user(node.os_user, f"{self._tool(node, 'pg_controldata')} -D {q(node.pgdata)}"),
                timeout_s=SSH_TIMEOUT_S, check=False)).stdout
            detail["cluster_state"] = parse_cluster_state(controldata)
            if detail["cluster_state"] != "shut down":
                raise FaultNotLanded(f"the database was not shut down cleanly ({detail['cluster_state']!r}); "
                                     "crash recovery could restore the page, so no byte was written", detail)

            original = (await host.run(read_byte, timeout_s=SSH_TIMEOUT_S)).stdout.strip()
            if not re.fullmatch(r"[0-9a-f]{2}", original):
                raise FaultNotLanded(f"could not read the byte at {offset} of {path}: {original!r}", detail)
            flipped = int(original, 16) ^ 0xFF
            detail.update(original_byte=original, written_byte=f"{flipped:02x}")
            await host.run(as_user(node.os_user, f"printf '\\{flipped:03o}' | dd of={q(path)} bs=1 "
                                                  f"seek={offset} count=1 conv=notrunc,fsync status=none"),
                           timeout_s=SSH_TIMEOUT_S)
            after = (await host.run(read_byte, timeout_s=SSH_TIMEOUT_S)).stdout.strip()
            if after != detail["written_byte"]:
                raise FaultNotLanded(f"byte at {offset} reads {after!r} after the write, "
                                     f"expected {detail['written_byte']}", detail)

            # --filenode (PostgreSQL 13+; the PostgreSQL 12 spelling -r is rejected by current builds)
            command = (f"{self._tool(node, 'pg_checksums')} --check -D {q(node.pgdata)} "
                       f"--filenode={int(target['filenode'])}")
            checked = await host.run(as_user(node.os_user, command), timeout_s=REVERT_TOTAL_BUDGET_S, check=False)
            output = (checked.stdout + checked.stderr).strip()
            failures = parse_checksum_failures(output)
            # kept in the evidence: an abort must say what the tool itself said
            detail["pg_checksums"] = {"command": command, "exit_status": checked.exit_status,
                                      "failures": [{"file": f, "block": b} for f, b in failures],
                                      "output": output[-2000:]}
            if not failures and not _CHECKSUM_SUMMARY_RE.search(output):
                # no failure line AND no summary: the tool never got as far as checking
                raise FaultNotLanded(f"pg_checksums could not verify the page (exit {checked.exit_status}): "
                                     f"{output[-300:] or 'no output'}", detail)
            expected = [(f, b) for f, b in failures
                        if b == int(target["block"]) and f.endswith(str(target["relation_path"]))]
            if len(failures) != 1 or len(expected) != 1:
                raise FaultNotLanded(f"pg_checksums did not report exactly one checksum failure at block "
                                     f"{target['block']} of {target['relation_path']}: {failures}", detail)

            await host.run(as_root(f"systemctl start {q(node.service)}"), timeout_s=REVERT_TOTAL_BUDGET_S)
            state = await self._settled_state(host, node, time.monotonic() + REVERT_TOTAL_BUDGET_S)
            if state != "active":
                raise RuntimeError(f"{node.name}: {node.service} is {state!r} after restart with the damaged page")
            detail["t0_mono_ns"] = time.monotonic_ns()
        return detail

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Idempotent: bring the service back if the run left it stopped, then drop the target."""
        out: dict[str, Any] = {}
        async with RemoteHost(node.ssh) as host:
            state = await self._settled_state(host, node, time.monotonic() + REVERT_TOTAL_BUDGET_S)
            if state != "active":
                await host.run(as_root(f"systemctl reset-failed {q(node.service)}"), timeout_s=SSH_TIMEOUT_S, check=False)
                await host.run(as_root(f"systemctl start {q(node.service)}"), timeout_s=REVERT_TOTAL_BUDGET_S, check=False)
                state = await self._settled_state(host, node, time.monotonic() + REVERT_TOTAL_BUDGET_S)
                out["service_started"] = True
            if state != "active":
                raise RuntimeError(f"{node.name}: {node.service} is {state!r}; the corruption target was not dropped")
            relation = ((detail or {}).get("inject") or {}).get("relation") or "resilience.corruption_target"
            if not str(relation).startswith("resilience."):
                raise RuntimeError(f"refusing to drop {relation}: not a harness-owned relation")
            await host.run(self._psql(node, f"DROP TABLE IF EXISTS {relation}"), timeout_s=SSH_TIMEOUT_S)
        out.update(action=f"DROP TABLE IF EXISTS {relation}", state=state)
        return out


register("os_ssh", OsSshCorruptionDriver)

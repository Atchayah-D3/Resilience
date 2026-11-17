# infra/monitoring -- the framework's metrics and dashboards (Arch §9, L4 Observability)

One setup for the whole framework, never per scenario. **Context only:** the harness decides
pass/fail from its own measurements; nothing here enters a verdict (Arch §7.1).

## How it works

```
 target host (e.g. VM2)                         harness host (e.g. VM1)
 ┌────────────────────────┐   pull, every 1 s   ┌──────────────────────────────────────────┐
 │ node_exporter    :9100 │◄────────────────────│ Prometheus :9090  (stores 15 days)       │
 │ postgres_exporter:9187 │◄────────────────────│      ▲                    │ PromQL        │
 └────────────────────────┘                     │      │ metrics.json       ▼               │
                                                │ harness run ──markers──► Grafana :3000 ───┼──► browser
                                                └──────────────────────────────────────────┘
```

| Piece | Runs on | Role |
|---|---|---|
| `node_exporter` | target | publishes the machine's numbers (CPU, memory, disk, network) |
| `postgres_exporter` | target | publishes the database's numbers (sessions, locks, WAL, checkpoints, cache) via a read-only `pg_monitor` login |
| Prometheus | harness host | fetches every exporter every second (its **targets**) and keeps the history |
| Grafana | harness host | draws the dashboard `Resilience run` from Prometheus |
| the harness (`resilience_tests/observability/`) | harness host | marks each run's events on the graphs (run start, phases, warm-up, **T0** = fault injected, hold, **T1** = recovery measured from here); copies the run window's metrics into the run folder (`metrics.json`); puts the dashboard link in `report.html` |

Where things live in the repository:

| File | What |
|---|---|
| env profile `observability:` section | Prometheus and Grafana addresses, the token *file* (never the token), exporter ports |
| `resilience_tests/observability/observer.py` | the run's observer (markers, snapshot, link); `NoObserver` when a profile has no section |
| `resilience_tests/observability/metrics.py` | the series every run keeps -- the dashboard is built from the same list |
| `resilience_tests/observability/dashboard.py` | generates `grafana/dashboards/resilience-run.json` (never edit the JSON by hand) |
| `resilience_tests/observability/scrape_config.py` | generates Prometheus's targets from the env profiles |
| `grafana/provisioning/` | Grafana's data source and dashboard loading |
| `setup-harness-host.sh`, `setup-target-exporter.sh` | the install, repeatable |

If Prometheus or Grafana does not answer, the run still runs and is judged as before; the report
discloses what was not recorded.

## Setup

1. **Each target** (with sudo; `node_exporter` must already run there):
   ```bash
   sudo bash infra/monitoring/setup-target-exporter.sh --pg-bin /usr/lib/postgresql/17.11.1.0/bin --port 5433 --db resilience
   ```
   Expect `pg_up 1`. With `pg_up 0`, `pg_hba.conf` must allow `resilience_monitor` from `127.0.0.1`.
2. **The env profile** gets an `observability:` section (see `envs/e2-dedicated-vm-u22-2.yaml`).
3. **The harness host** (no sudo; from the checkout, with its `.venv`):
   ```bash
   infra/monitoring/setup-harness-host.sh --env e2-dedicated-vm-u22-2
   ```
   Expect `Prometheus Server is Ready.`, `Grafana is up`, and every target `"health":"up"`.
4. **Grafana token for the harness:** log in at `http://<harness host>:3000` (`admin`/`admin`, then
   change it). *Administration → Users and access → Service accounts → Add service account*
   `resilience-harness`, role **Editor** → *Add service account token* → copy it (shown once), then:
   ```bash
   mkdir -p ~/.config/resilience && chmod 700 ~/.config/resilience
   umask 077 && cat > ~/.config/resilience/grafana-token     # paste, Enter, Ctrl-D
   ```
5. **Check:** run any scenario; open its `report.html` → *open this run in Grafana*. The markers
   must sit where the report's own timeline puts them (Arch: check a panel against the harness's
   own number before trusting it).

Adding a target later: add it to an env profile, run step 1 on it, re-run step 3.
Changing the dashboard: edit `metrics.py` / `dashboard.py`, then
`python -m resilience_tests.observability.dashboard --out infra/monitoring/grafana/dashboards/resilience-run.json`.

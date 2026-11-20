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

### A target whose sudo runs no shell (e2-u22-3, 10.11.21.14)

`setup-target-exporter.sh` needs `sudo bash`. On an account whose sudo forbids shells and
`useradd` (env profile `sudo_shell: false`), the same result comes from one program per sudo, run
on the target as the SSH user (done for 10.11.21.14 on 20 Nov 2026). The exporter runs as
`nobody` and connects through the local socket, so no password is created or stored -- this needs
`local all all trust` in pg_hba.conf, as that target's env profile discloses; elsewhere use the
script and its password.

```bash
# 1. the exporter binary (downloaded as the SSH user; only `install` runs with sudo)
V=0.20.1; mkdir -p ~/pe && cd ~/pe
curl -fsSL -o pe.tar.gz "https://github.com/prometheus-community/postgres_exporter/releases/download/v$V/postgres_exporter-$V.linux-amd64.tar.gz"
tar xzf pe.tar.gz --strip-components=1
sudo install -m 755 ~/pe/postgres_exporter /usr/local/bin/postgres_exporter && rm -rf ~/pe

# 2. the read-only login (pg_monitor) and its settings -- SQL in a file psql reads as postgres
cat > /tmp/resilience-monitor.sql <<'SQL'
DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'resilience_monitor') THEN
    CREATE ROLE resilience_monitor LOGIN IN ROLE pg_monitor;
  END IF;
END $$;
ALTER ROLE resilience_monitor SET lock_timeout = '200ms';      -- never queue behind a lock
ALTER ROLE resilience_monitor SET statement_timeout = '800ms';
ALTER ROLE resilience_monitor SET log_min_messages = 'fatal';  -- keep its skipped queries out of the log
SQL
chmod 644 /tmp/resilience-monitor.sql
cd /tmp && sudo -u postgres -- /usr/pgsql-17.11.1.0/bin/psql -X -p 5434 -d resilience -v ON_ERROR_STOP=1 -f /tmp/resilience-monitor.sql
rm -f /tmp/resilience-monitor.sql

# 3. how it connects: the local socket (unix_socket_directories = /tmp), no secret
printf '%s\n' 'DATA_SOURCE_NAME=postgresql:///resilience?host=/tmp&port=5434&user=resilience_monitor&sslmode=disable' \
  | sudo tee /etc/default/postgres_exporter > /dev/null

# 4. the service, as user nobody
cat <<'UNIT' | sudo tee /etc/systemd/system/postgres_exporter.service > /dev/null
[Unit]
Description=Prometheus PostgreSQL exporter for the resilience harness (port 5434)
After=network-online.target
[Service]
User=nobody
EnvironmentFile=/etc/default/postgres_exporter
ExecStart=/usr/local/bin/postgres_exporter --web.listen-address=:9187 --collector.stat_checkpointer --collector.long_running_transactions --collector.postmaster
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
sudo systemctl daemon-reload && sudo systemctl enable postgres_exporter && sudo systemctl restart postgres_exporter
curl -s localhost:9187/metrics | grep '^pg_up '       # expect: pg_up 1
```

Then on the harness host: add the `observability:` section to the env profile, and regenerate
Prometheus's targets for every lab it watches, without restarting anything:

```bash
cd ~/resilience-attach
cp ~/monitoring/prometheus.yml ~/monitoring/prometheus.yml.bak
.venv/bin/python -m resilience_tests.observability.scrape_config --env e2-dedicated-vm-u22-2 --env e2-u22-3 \
    --out ~/monitoring/prometheus.yml
~/monitoring/prometheus/promtool check config ~/monitoring/prometheus.yml
curl -X POST localhost:9090/-/reload
curl -s localhost:9090/api/v1/targets | grep -o '"scrapeUrl":"[^"]*","globalUrl":"[^"]*","lastError":"[^"]*","lastScrape":"[^"]*","lastScrapeDuration":[^,]*,"health":"[a-z]*"' | grep -o 'scrapeUrl":"[^"]*\|health":"[a-z]*'
```

On 10.11.21.14 the "I/O pressure" graph stays empty: its kernel has no /proc/pressure (PSI is
off unless the machine is booted with `psi=1`).
Changing the dashboard: edit `metrics.py` / `dashboard.py`, then
`python -m resilience_tests.observability.dashboard --out infra/monitoring/grafana/dashboards/resilience-run.json`.

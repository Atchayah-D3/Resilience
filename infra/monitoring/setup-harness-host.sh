#!/usr/bin/env bash
# Prometheus + Grafana on the harness (driver) host, as the harness's own user -- no sudo.
# Idempotent: safe to run again (re-generates the scrape targets and restarts both).
#
#   infra/monitoring/setup-harness-host.sh --env e2-dedicated-vm-u22-2 [--env other-profile]
#
# Run from the repository checkout, with its .venv. See infra/monitoring/README.md.
set -euo pipefail

PROMETHEUS_VERSION=3.16.0
GRAFANA_VERSION=13.3.1
MON_DIR="${MON_DIR:-$HOME/monitoring}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
PY="$REPO/.venv/bin/python"

envs=()
while [ $# -gt 0 ]; do
  case "$1" in
    --env) envs+=(--env "$2"); shift 2 ;;
    *) echo "usage: $0 --env PROFILE [--env PROFILE ...]" >&2; exit 2 ;;
  esac
done
[ ${#envs[@]} -gt 0 ] || { echo "usage: $0 --env PROFILE [--env PROFILE ...]" >&2; exit 2; }

mkdir -p "$MON_DIR" && cd "$MON_DIR"

# --- 1. install (only when the pinned version is not there yet)
if ! ./prometheus/prometheus --version 2>/dev/null | grep -q "version $PROMETHEUS_VERSION "; then
  curl -fL -o prometheus.tar.gz \
    "https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS_VERSION/prometheus-$PROMETHEUS_VERSION.linux-amd64.tar.gz"
  rm -rf prometheus && mkdir prometheus && tar xzf prometheus.tar.gz -C prometheus --strip-components=1 && rm prometheus.tar.gz
fi
if ! ./grafana/bin/grafana --version 2>/dev/null | grep -q "version $GRAFANA_VERSION"; then
  curl -fL -o grafana.tar.gz "https://dl.grafana.com/oss/release/grafana-$GRAFANA_VERSION.linux-amd64.tar.gz"
  rm -rf grafana && mkdir grafana && tar xzf grafana.tar.gz -C grafana --strip-components=1 && rm grafana.tar.gz
fi

# --- 2. configure: scrape targets from the env profiles; Grafana's provisioning from the repo
"$PY" -m resilience_tests.observability.scrape_config "${envs[@]}" --out "$MON_DIR/prometheus.yml"
./prometheus/promtool check config "$MON_DIR/prometheus.yml"

mkdir -p grafana-data grafana-provisioning/{datasources,dashboards,plugins,alerting}
cp "$REPO/infra/monitoring/grafana/provisioning/datasources/prometheus.yaml" grafana-provisioning/datasources/
sed "s#@DASHBOARDS_DIR@#$REPO/infra/monitoring/grafana/dashboards#" \
  "$REPO/infra/monitoring/grafana/provisioning/dashboards/resilience.yaml" > grafana-provisioning/dashboards/resilience.yaml
cat > grafana.ini <<EOF
[server]
http_port = 3000
[paths]
data = $MON_DIR/grafana-data
logs = $MON_DIR/grafana-data/log
provisioning = $MON_DIR/grafana-provisioning
[analytics]
reporting_enabled = false
check_for_updates = false
EOF

# --- 3. (re)start both, and restart them after a reboot (the user's crontab; no sudo)
PROM_CMD="cd $MON_DIR && ./prometheus/prometheus --config.file=prometheus.yml --storage.tsdb.path=prometheus-data --storage.tsdb.retention.time=15d --web.listen-address=:9090 --web.enable-lifecycle >> prometheus.log 2>&1"
GRAF_CMD="cd $MON_DIR/grafana && ./bin/grafana server --homepath . --config ../grafana.ini >> ../grafana.log 2>&1"
pkill -u "$USER" -f "prometheus/prometheus --config.file=prometheus.yml" 2>/dev/null || true
pkill -u "$USER" -f "bin/grafana server --homepath . --config ../grafana.ini" 2>/dev/null || true
sleep 2
nohup bash -c "$PROM_CMD" >/dev/null 2>&1 &
nohup bash -c "$GRAF_CMD" >/dev/null 2>&1 &
{ crontab -l 2>/dev/null | grep -vF "$MON_DIR" || true
  echo "@reboot $PROM_CMD"
  echo "@reboot $GRAF_CMD"; } | crontab -

# --- 4. check
for i in $(seq 1 30); do curl -sf localhost:9090/-/ready >/dev/null && break; sleep 1; done
curl -sf localhost:9090/-/ready && echo
for i in $(seq 1 60); do curl -sf localhost:3000/api/health >/dev/null && break; sleep 1; done
curl -sf localhost:3000/api/health | grep -q '"database": *"ok"' && echo "Grafana is up"
curl -s localhost:9090/api/v1/targets | grep -o '"scrapeUrl":"[^"]*"\|"health":"[a-z]*"' || true
echo "Next: the Grafana service-account token (README.md, step 4)."

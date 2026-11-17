#!/usr/bin/env bash
# postgres_exporter on a target (database) host, for the harness's Prometheus. Needs sudo.
# Idempotent. node_exporter is assumed present (the lab installs it); see README.md.
#
#   sudo bash setup-target-exporter.sh --pg-bin /usr/lib/postgresql/17.11.1.0/bin --port 5433 --db resilience
#
# Creates a read-only monitoring login (built-in role pg_monitor) once; its password is generated
# here and only ever stored in /etc/default/postgres_exporter (root-only).
set -euo pipefail

EXPORTER_VERSION=0.20.1
LISTEN=":9187"
PG_BIN="" PORT="" DB="" OS_USER="postgres" ROLE="resilience_monitor"
while [ $# -gt 0 ]; do
  case "$1" in
    --pg-bin) PG_BIN="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --db) DB="$2"; shift 2 ;;
    --os-user) OS_USER="$2"; shift 2 ;;
    *) echo "usage: $0 --pg-bin DIR --port PORT --db NAME [--os-user postgres]" >&2; exit 2 ;;
  esac
done
[ -n "$PG_BIN" ] && [ -n "$PORT" ] && [ -n "$DB" ] || { echo "usage: $0 --pg-bin DIR --port PORT --db NAME" >&2; exit 2; }
[ "$(id -u)" -eq 0 ] || { echo "run with sudo" >&2; exit 2; }

# --- 1. the exporter binary and its system user
if ! /usr/local/bin/postgres_exporter --version 2>&1 | grep -q "version $EXPORTER_VERSION"; then
  tmp=$(mktemp -d)
  curl -fL -o "$tmp/pe.tar.gz" \
    "https://github.com/prometheus-community/postgres_exporter/releases/download/v$EXPORTER_VERSION/postgres_exporter-$EXPORTER_VERSION.linux-amd64.tar.gz"
  tar xzf "$tmp/pe.tar.gz" -C "$tmp" --strip-components=1
  install -m 755 "$tmp/postgres_exporter" /usr/local/bin/postgres_exporter
  rm -rf "$tmp"
fi
id postgres_exporter >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin postgres_exporter

# --- 2. the read-only login, created once; its password only in the root-only file
if [ ! -s /etc/default/postgres_exporter ]; then
  PW=$(openssl rand -base64 24 | tr -d '/+=')
  sudo -u "$OS_USER" sh -c "cd /tmp && '$PG_BIN/psql' -X -p '$PORT' -d '$DB' -v ON_ERROR_STOP=1 -c \
    \"DO \\\$\\\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$ROLE') THEN
        CREATE ROLE $ROLE LOGIN IN ROLE pg_monitor; END IF; END \\\$\\\$;\" -c \"ALTER ROLE $ROLE PASSWORD '$PW';\""
  umask 077
  echo "DATA_SOURCE_NAME=postgresql://$ROLE:$PW@127.0.0.1:$PORT/$DB?sslmode=disable" > /etc/default/postgres_exporter
  unset PW
fi
chmod 600 /etc/default/postgres_exporter

# The monitor never waits for a lock: some queries (table sizes) lock every table briefly, so
# behind a waiting schema change they would queue like any user -- the graphs would go blank at
# the very moment a lock pile-up is under test, and the exporter would join the queue. With a
# short lock timeout that query is skipped for that second instead; every other number keeps coming.
cat > /tmp/resilience-monitor-role.sql <<SQL
ALTER ROLE $ROLE SET lock_timeout = '200ms';
ALTER ROLE $ROLE SET statement_timeout = '800ms';
-- its own skipped queries are not the database's events: kept out of the server log the harness reads
ALTER ROLE $ROLE SET log_min_messages = 'fatal';
SQL
chmod 644 /tmp/resilience-monitor-role.sql
sudo -u "$OS_USER" sh -c "cd /tmp && '$PG_BIN/psql' -X -q -p '$PORT' -d '$DB' -v ON_ERROR_STOP=1 -f /tmp/resilience-monitor-role.sql"
rm -f /tmp/resilience-monitor-role.sql

# --- 3. the service: checkpoints, long transactions and the server start time on top of the defaults
cat > /etc/systemd/system/postgres_exporter.service <<EOF
[Unit]
Description=Prometheus PostgreSQL exporter for the resilience harness (port $PORT)
After=network-online.target
[Service]
User=postgres_exporter
EnvironmentFile=/etc/default/postgres_exporter
ExecStart=/usr/local/bin/postgres_exporter --web.listen-address=$LISTEN --collector.stat_checkpointer --collector.long_running_transactions --collector.postmaster
Restart=on-failure
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable postgres_exporter >/dev/null
systemctl restart postgres_exporter

# --- 4. check
for i in $(seq 1 15); do curl -sf "localhost$LISTEN/metrics" >/dev/null && break; sleep 1; done
curl -s "localhost$LISTEN/metrics" | grep -E '^pg_up ' || { echo "exporter not answering" >&2; exit 1; }
echo "pg_up 1 means the exporter logs in; 0: check pg_hba.conf allows $ROLE from 127.0.0.1 (journalctl -u postgres_exporter)"
systemctl is-active node_exporter >/dev/null && echo "node_exporter: active" || echo "node_exporter: NOT active -- install it (README.md)"

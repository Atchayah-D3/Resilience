#!/usr/bin/env bash
# Local quick-win feature-cost runs on the isolated test cluster (WSL), following the per-run
# checklist in features/README.md section 4:
#
#   local/quickwin_local.sh off      # R-OFF: dataset rrb_off (heap), sdbAudit off
#   local/quickwin_local.sh on       # R-ON : dataset rrb_on (TDE + immutable result), sdbAudit on
#   local/quickwin_local.sh compare  # compare the latest R-ON and R-OFF runs
#
# Test cluster (separate from releasetesting/main): primary :5440, replica :5441, both with
# pg_stat_statements, logging_collector and track_io_timing on; app instance on :8090 / :8091.
# Env: STEPS (20,40,80) STEP_HOLD (40s) STEP_RAMP (10s) SETTLE (10s) TARGET_RATE (top step)
#      AUDIT_LOG ('write, ddl')  KEYS (load/keys-50k.csv)
set -euo pipefail
KIT=$(cd "$(dirname "$0")/.." && pwd)
QW=/var/lib/postgresql/releasetesting/qw
PRIMARY_PORT=5440 REPLICA_PORT=5441 APP=8090 MGMT=8091
MODE=${1:?usage: quickwin_local.sh off|on|compare}
STEPS=${STEPS:-20,40,80}
export PGHOST=/tmp PGUSER=postgres

if [[ "$MODE" == compare ]]; then
  on=$(ls -td "$KIT"/results/R-ON-* 2>/dev/null | head -1); off=$(ls -td "$KIT"/results/R-OFF-* 2>/dev/null | head -1)
  [[ -n "$on" && -n "$off" ]] || { echo "need one R-ON-* and one R-OFF-* run in results/" >&2; exit 1; }
  exec python3 "$KIT/analysis/compare_runs.py" "$on" "$off"
fi
case "$MODE" in
  off) DB=rrb_off; AUDIT=off; LABEL=R-OFF ;;
  on)  DB=rrb_on;  AUDIT=on;  LABEL=R-ON ;;
  *)   echo "usage: quickwin_local.sh off|on|compare" >&2; exit 1 ;;
esac

echo "== 1. nodes up"
psql -p $PRIMARY_PORT -Atc "select 'primary', not pg_is_in_recovery()" -d postgres
psql -p $REPLICA_PORT -Atc "select 'replica', pg_is_in_recovery()" -d postgres

echo "== 2. sdbAudit $AUDIT"
PGPORT=$PRIMARY_PORT AUDIT_LOG="${AUDIT_LOG:-write, ddl}" "$KIT/features/set_audit.sh" "$AUDIT" | grep -E "sdbaudit.log ="

echo "== 3. partitions for the next 48 h"
psql -p $PRIMARY_PORT -d $DB -qAtc "SELECT rrb.ensure_partitions(now(), 48)" >/dev/null

echo "== 4. feature state"
psql -p $PRIMARY_PORT -d $DB -XAt -f "$KIT/features/feature_status.sql" | python3 -c '
import json,sys; f=json.load(sys.stdin)
print("  access methods:", f.get("table_access_methods"), "| immutable:", f.get("immutable_tables"),
      "| sdbaudit.log:", (f.get("sdbaudit") or {}).get("log", f.get("sdbaudit_log")))'

echo "== 5. reset pg_stat_statements (primary + replica)"
for p in $PRIMARY_PORT $REPLICA_PORT; do psql -p $p -d $DB -qAtc "SELECT pg_stat_statements_reset()" >/dev/null; done

echo "== 6. restart the app on :$APP for $DB, warm up 30 s"
pkill -f '^java .*-Drrb.instance=qw ' || true; sleep 2
( cd "$KIT/app"
  export APP_PORT=$APP MGMT_PORT=$MGMT DB_NAME=$DB DB_USER=rrb_app DB_PASSWORD="$(cat $QW/app.pw)" \
         DB_PRIMARY_HOST='[::1]' DB_PRIMARY_PORT=$PRIMARY_PORT \
         DB_REPLICA_ENABLED=true DB_REPLICA_HOST='[::1]' DB_REPLICA_PORT=$REPLICA_PORT
  setsid nohup java -Drrb.instance=qw -Xms1g -Xmx1g -jar target/rrb-api.jar > /tmp/rrb-api-qw.log 2>&1 < /dev/null & )
for i in $(seq 90); do curl -sf -o /dev/null localhost:$MGMT/actuator/prometheus && break; sleep 1; done
KEYS=${KEYS:-$KIT/load/keys-50k.csv}
k6 run -q -e PROFILE=smoke -e SMOKE_RATE=20 -e SMOKE_DURATION=30s -e STEP_STATS=0 \
  -e BASE_URL=http://localhost:$APP -e KEYS="$KEYS" --no-thresholds "$KIT/load/k6/rrb_session.js" >/dev/null 2>&1 \
  && echo "  warm-up done" || echo "  warm-up had errors (continuing)"

echo "== 7. run"
top=${STEPS##*,}
cd "$KIT"
RUN="$LABEL-$(date -u +%Y%m%dT%H%M%SZ)" PGPORT=$PRIMARY_PORT PGDATABASE=$DB KEYS="$KEYS" \
SAMPLERS="primary=port=$PRIMARY_PORT dbname=$DB;replica=port=$REPLICA_PORT dbname=$DB" \
APPS=http://localhost:$MGMT BASE_URL=http://localhost:$APP \
PRE_VUS=${PRE_VUS:-200} MAX_VUS=${MAX_VUS:-1000} STEPS=$STEPS STEP_HOLD=${STEP_HOLD:-40s} \
STEP_RAMP=${STEP_RAMP:-10s} SETTLE=${SETTLE:-10s} TARGET_RATE=${TARGET_RATE:-$top} \
./run_test.sh ramp

echo "== 8. partitions after the run (DEFAULT partitions must be empty)"
psql -p $PRIMARY_PORT -d $DB -Atc "SELECT * FROM rrb.check_partitions"

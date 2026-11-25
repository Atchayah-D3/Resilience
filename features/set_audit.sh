#!/usr/bin/env bash
# Switches sdbAudit on or off cluster-wide without a restart (ALTER SYSTEM + reload).
# Existing connections pick the change up at the reload, so the app does not need a restart.
#
#   PGHOST=<primary> PGPORT=5432 PGUSER=postgres ./set_audit.sh on     # production policy
#   PGHOST=<primary> PGPORT=5432 PGUSER=postgres ./set_audit.sh off    # sdbaudit.log = none
#   ./set_audit.sh status
#
# Env:
#   AUDIT_LOG            statement classes for "on"   (default: write, ddl)  AGREE WITH SENIOR
#   AUDIT_LOG_PARAMETER  log bind parameters (on|off) (default: off)
#   AUDIT_LOG_RELATION   one line per relation        (default: off)
#
# "off" keeps the sdbaudit library loaded and only sets sdbaudit.log = none, so its hooks
# still run. If "off" must mean "not loaded", remove sdbaudit from shared_preload_libraries
# in postgresql.conf (not with ALTER SYSTEM) and restart; see README.md.
#
# Audit records go to the server log, so on a run with audit on the log directory grows
# with the write rate. Put log_directory on its own disk if production does.
set -euo pipefail

MODE=${1:-status}
PSQL=(psql -X -q -v ON_ERROR_STOP=1 -d "${PGDATABASE:-postgres}")

show() {
  "${PSQL[@]}" -At -F' = ' -c "SELECT name, setting FROM pg_settings
    WHERE name IN ('sdbaudit.log','sdbaudit.log_parameter','sdbaudit.log_relation','shared_preload_libraries')
    ORDER BY name"
}

loaded=$("${PSQL[@]}" -Atc "SELECT current_setting('shared_preload_libraries') ~* '\msdbaudit\M'")
if [[ "$loaded" != "t" && "$MODE" == "on" ]]; then
  echo "sdbaudit is not in shared_preload_libraries; add it to postgresql.conf and restart first." >&2
  exit 1
fi

case "$MODE" in
  on)
    "${PSQL[@]}" -c "ALTER SYSTEM SET sdbaudit.log = '${AUDIT_LOG:-write, ddl}'" \
                 -c "ALTER SYSTEM SET sdbaudit.log_parameter = '${AUDIT_LOG_PARAMETER:-off}'" \
                 -c "ALTER SYSTEM SET sdbaudit.log_relation = '${AUDIT_LOG_RELATION:-off}'" \
                 -c "SELECT pg_reload_conf()" >/dev/null ;;
  off)
    "${PSQL[@]}" -c "ALTER SYSTEM SET sdbaudit.log = 'none'" \
                 -c "SELECT pg_reload_conf()" >/dev/null ;;
  status) ;;
  *) echo "usage: $0 on|off|status" >&2; exit 2 ;;
esac

sleep 1   # the reload is asynchronous
show

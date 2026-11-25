#!/usr/bin/env bash
# Builds the RRB quick-win dataset on the PRIMARY (the replica receives it via
# streaming replication).
#
# Usage:
#   PGHOST=db1 PGPORT=5432 PGUSER=rrb PGDATABASE=rrb ./load_data.sh [N] [CEN] [JOBS] [CHUNK]
#     N      candidates to load            (default 15000000 = 1.5 crore)
#     CEN    exam/notification id          (default 202501)
#     JOBS   parallel psql sessions        (default 8; ~1 per 4-8 DB cores)
#     CHUNK  candidates per transaction    (default 500000)
#   RESET=1 drops schema rrb first (DESTROYS existing benchmark data).
#   TABLE_AM=<access method> creates every table with that access method, e.g. the
#     encrypted one for the TDE dataset (pg_tde calls it tde_heap; confirm the sdbTDE name).
#     It is also set as the database default so partitions created later by
#     rrb.ensure_partitions() are encrypted too. See features/README.md.
#
# Connect DIRECTLY to the primary (not through sdbPool) for the load.
set -euo pipefail
cd "$(dirname "$0")"

N=${1:-15000000}
CEN=${2:-202501}
JOBS=${3:-8}
CHUNK=${4:-500000}
PSQL=(psql -X -q -v ON_ERROR_STOP=1)

echo "Target: ${PGHOST:-<socket>}:${PGPORT:-5432}/${PGDATABASE:-$USER}  N=$N CEN=$CEN JOBS=$JOBS CHUNK=$CHUNK TABLE_AM=${TABLE_AM:-<default>}"

if [[ -n "${TABLE_AM:-}" ]]; then
  if [[ -z "$("${PSQL[@]}" -Atc "SELECT 1 FROM pg_am WHERE amname = '${TABLE_AM}' AND amtype = 't'")" ]]; then
    echo "table access method '${TABLE_AM}' does not exist (is the TDE extension created in this database?)" >&2
    exit 1
  fi
  "${PSQL[@]}" -c "ALTER DATABASE \"$("${PSQL[@]}" -Atc 'SELECT current_database()')\" SET default_table_access_method = '${TABLE_AM}'"
  export PGOPTIONS="${PGOPTIONS:-} -c default_table_access_method=${TABLE_AM}"   # this session and the xargs workers
fi

if [[ "${RESET:-0}" == "1" ]] && [[ -n "$("${PSQL[@]}" -Atc "SELECT 1 FROM pg_attribute
      WHERE attrelid = to_regclass('rrb.result') AND attname = 'imtcrdt' AND NOT attisdropped" 2>/dev/null || true)" ]]; then
  echo "rrb.result is immutable (sdbImmutable), so schema rrb cannot be dropped." >&2
  echo "Rebuild by dropping and re-creating the database, or restore a snapshot." >&2
  exit 1
fi

if [[ "${RESET:-0}" == "1" ]]; then
  echo "RESET=1: dropping schema rrb"
  "${PSQL[@]}" -c "DROP SCHEMA IF EXISTS rrb CASCADE"
fi

if [[ -n "$("${PSQL[@]}" -Atc "SELECT 1 FROM rrb.candidate LIMIT 1" 2>/dev/null || true)" ]]; then
  echo "rrb.candidate already has rows. Re-run with RESET=1 to rebuild." >&2
  exit 1
fi

t0=$(date +%s)
echo "== schema";      "${PSQL[@]}" -f 01_schema.sql
echo "== partitions";  "${PSQL[@]}" -f 02_partitions.sql
"${PSQL[@]}" -c "CREATE TABLE IF NOT EXISTS rrb.application_c${CEN} PARTITION OF rrb.application FOR VALUES IN (${CEN})" \
             -c "CREATE TABLE IF NOT EXISTS rrb.result_c${CEN}      PARTITION OF rrb.result      FOR VALUES IN (${CEN})"

echo "== loading $N candidates in chunks of $CHUNK with $JOBS parallel sessions"
seq 1 "$CHUNK" "$N" | xargs -P "$JOBS" -I{} bash -c '
  lo={}; hi=$(( lo + '"$CHUNK"' - 1 )); (( hi > '"$N"' )) && hi='"$N"'
  psql -X -q -v ON_ERROR_STOP=1 -v lo=$lo -v hi=$hi -v cen='"$CEN"' -f load_chunk.sql \
    && echo "  loaded $lo..$hi"'

t1=$(date +%s); echo "== load took $((t1 - t0)) s; finalizing (indexes, vacuum)"
"${PSQL[@]}" -v cen="$CEN" -f 03_finalize.sql
t2=$(date +%s); echo "== finalize took $((t2 - t1)) s; total $((t2 - t0)) s"

echo "== smoke check"
"${PSQL[@]}" -v cen="$CEN" -f smoke_check.sql

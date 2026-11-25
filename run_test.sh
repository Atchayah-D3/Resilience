#!/usr/bin/env bash
# One command per benchmark run: creates the run folder, starts the DB sampler(s), runs the
# load, stops the samplers when the load ends, then writes the report and refreshes the index.
#
#   ./run_test.sh smoke|ramp|spike
#
# Everything for the run lands in results/<run>/ :
#   run.json                         run id, profile, tool, start/end, status, settings
#   os-vmstat.txt os-iostat.txt os-pidstat.txt os-sar.data   1-second OS records of this machine
#   k6-* | jmeter.*                  load results (per-step summary + step plan)
#   sampler-<name>.jsonl             DB / host / app samples;  feature_status-<name>.json
#   report.html  report.md  steps.csv  summary.json
#   metrics/     per-second CSV files (app, db-<n>, waits-<n>, os-<n>) + events.jsonl + README.md
# and results/index.html lists every run.
#
# Ctrl+C at any time: the load stops, the samplers stop, and the report is still written,
# marked INTERRUPTED with the unfinished steps shown as "not measured".
#
# Env (defaults in brackets):
#   TOOL [k6]              k6 | jmeter
#   RUN  [<profile>-<UTC time>-<6 hex>]   run id = folder name
#   RESULTS [<kit>/results]
#   SAMPLERS ["primary="]  ";"-separated name=dsn list, one sampler per database server.
#                          An empty dsn uses the PG* variables. "none" = no sampler.
#                          e.g. "primary=host=db1 port=5432 dbname=rrb user=rrb_monitor;replica=host=db2 ..."
#                          A dsn only overrides what differs from PG*: local replica on 5434 = "primary=;replica=port=5434"
#                          A sampler measures the host it RUNS on (CPU, disk). For DB-host OS metrics on
#                          real infra, start sampler.py on each DB server with --run <same id> instead.
#   APPS                   ","-separated app management URLs polled by the first sampler,
#                          e.g. "http://app1:8081,http://app2:8081"
#   GEN_SAMPLER [1]        also sample THIS machine's CPU as a load generator (proposal 9.2: a run is void
#                          above 70 % generator CPU). On other generators run: sampler.py --run <id> --name genN --no-db
#   OSTOOLS [1]            also record vmstat / iostat / sar / pidstat at 1 s into the run folder when installed
#                          (proposal 10.3 "OS-level confirmation at 1 s"); they measure THIS machine
#   SAMPLE_INTERVAL [1]    sampler interval in seconds
#   SAMPLER_LEAD [6] SAMPLER_TAIL [8]   seconds sampled before / after the load
#   TARGET_RATE [25000]    sessions/s at which the acceptance criteria (proposal 11.2) are evaluated
#   ANALYZE_ARGS           extra analyze_run.py arguments, e.g. "--p99 login=500,result=300,cutoff=300"
#   KEYS [<kit>/load/keys.csv]  key file (relative paths are resolved from where you run this)
#   Load settings pass through unchanged: k6 -> BASE_URL STEPS STEP_HOLD STEP_RAMP SETTLE PRE_VUS ...
#                                          jmeter -> HOST PORT PROTOCOL KEYS STEPS STEP_HOLD ...
#   Extra arguments after the profile go to run_k6.sh / run_jmeter.sh.
set -uo pipefail
KIT=$(cd "$(dirname "$0")" && pwd)

PROFILE=${1:-}; shift || true
case "$PROFILE" in smoke|ramp|spike) ;; *) echo "usage: $0 smoke|ramp|spike [extra load-tool args]" >&2; exit 2 ;; esac
TOOL=${TOOL:-k6}
[[ "$TOOL" == k6 || "$TOOL" == jmeter ]] || { echo "TOOL must be k6 or jmeter" >&2; exit 2; }
RUN=${RUN:-${PROFILE}-$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')}
RESULTS=$(mkdir -p "${RESULTS:-$KIT/results}" && cd "${RESULTS:-$KIT/results}" && pwd)
DIR="$RESULTS/$RUN"
SAMPLERS=${SAMPLERS-primary=}
# the load tools run from their own folders, so pass the key file as an absolute path
KEYS=${KEYS:-$KIT/load/keys.csv}
KEYS=$(realpath -m "$KEYS"); export KEYS
[[ -f "$KEYS" ]] || { echo "key file $KEYS not found (load/gen_zipf_csv.py creates it)" >&2; exit 2; }
APPS=${APPS:-}
if [[ -e "$DIR" ]]; then echo "$DIR already exists; choose another RUN" >&2; exit 2; fi
mkdir -p "$DIR"

json_str() { python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"; }
write_meta() {   # status [exit code]
  cat > "$DIR/run.json" <<EOF
{"run": $(json_str "$RUN"), "profile": "$PROFILE", "tool": "$TOOL", "status": "$1", "load_exit_code": ${2:-null}, "interrupted_epoch": ${INTERRUPTED_EPOCH:-null},
 "started_utc": "$STARTED_UTC", "started_epoch": $STARTED_EPOCH, "ended_utc": $( [[ -n "${ENDED_UTC:-}" ]] && json_str "$ENDED_UTC" || echo null),
 "host": $(json_str "$(hostname)"), "target": $(json_str "${BASE_URL:-${PROTOCOL:-http}://${HOST:-localhost}:${PORT:-8080}}"),
 "samplers": $(json_str "$SAMPLERS"), "apps": $(json_str "$APPS"),
 "settings": {$(for v in KEYS STEPS STEP_HOLD STEP_RAMP SETTLE SPIKE_RATE SPIKE_RISE SPIKE_HOLD IDLE_RATE SMOKE_RATE SMOKE_DURATION PRE_VUS MAX_VUS NEW_CONN THINK_MS CEN_ID REMOTE; do
     [[ -n "${!v:-}" ]] && printf '%s: %s, ' "$(json_str "$v")" "$(json_str "${!v}")"; done | sed 's/, $//')}}
EOF
}
STARTED_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ); STARTED_EPOCH=$(date +%s)
write_meta running
echo "== run $RUN  ($PROFILE, $TOOL)  ->  $DIR"

# ---------------------------------------------------------------- samplers
PIDS=()
if [[ "$SAMPLERS" != "none" && -n "$SAMPLERS" ]]; then
  IFS=';' read -ra entries <<< "$SAMPLERS"
  first=1
  for e in "${entries[@]}"; do
    [[ -z "$e" ]] && continue
    name=${e%%=*}; dsn=${e#*=}
    args=(--run "$RUN" --results "$RESULTS" --name "$name" --interval "${SAMPLE_INTERVAL:-1}")
    [[ -n "$dsn" ]] && args+=(--dsn "$dsn")
    if (( first )) && [[ -n "$APPS" ]]; then
      IFS=',' read -ra apps <<< "$APPS"; for u in "${apps[@]}"; do args+=(--app "$u"); done
    fi
    first=0
    python3 "$KIT/analysis/sampler.py" "${args[@]}" > "$DIR/sampler-$name.log" 2>&1 &
    PIDS+=($!)
    echo "== sampler $name started (pid $!, log sampler-$name.log)"
  done
fi
if [[ "${GEN_SAMPLER:-1}" == 1 ]]; then
  gname="gen-$(hostname -s)"
  python3 "$KIT/analysis/sampler.py" --run "$RUN" --results "$RESULTS" --name "$gname" --no-db \
    --interval "${SAMPLE_INTERVAL:-1}" > "$DIR/sampler-$gname.log" 2>&1 &
  PIDS+=($!)
  echo "== generator sampler $gname started (pid $!)"
fi
OSPIDS=()
if [[ "${OSTOOLS:-1}" == 1 ]]; then
  started=()
  command -v vmstat  >/dev/null && { vmstat -t -w 1 > "$DIR/os-vmstat.txt" 2>&1 & OSPIDS+=($!); started+=(vmstat); }
  command -v iostat  >/dev/null && { iostat -dxmt 1 > "$DIR/os-iostat.txt" 2>&1 & OSPIDS+=($!); started+=(iostat); }
  command -v sar     >/dev/null && { sar -o "$DIR/os-sar.data" 1 > /dev/null 2>&1 & OSPIDS+=($!); started+=(sar); }
  command -v pidstat >/dev/null && { pidstat -u -d -h 1 > "$DIR/os-pidstat.txt" 2>&1 & OSPIDS+=($!); started+=(pidstat); }
  (( ${#started[@]} )) && echo "== OS tools at 1 s: ${started[*]} (os-*.txt; sar binary: sar -f os-sar.data)" \
                       || echo "== OS tools: none installed (apt install sysstat procps)"
fi
if (( ${#PIDS[@]} )); then
  sleep "${SAMPLER_LEAD:-6}"
  for p in "${PIDS[@]}"; do
    kill -0 "$p" 2>/dev/null || { echo "!! a sampler exited early:"; tail -3 "$DIR"/sampler-*.log; }
  done
fi

stop_samplers() {
  for p in "${OSPIDS[@]}"; do kill -TERM "$p" 2>/dev/null; done
  OSPIDS=()
  (( ${#PIDS[@]} )) || return 0
  for p in "${PIDS[@]}"; do kill -TERM "$p" 2>/dev/null; done
  for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null; done
  PIDS=()
  echo "== samplers stopped"
}

# ---------------------------------------------------------------- load
INTERRUPTED=0; INTERRUPTED_EPOCH=
trap '(( INTERRUPTED )) || INTERRUPTED_EPOCH=$(date +%s.%N); INTERRUPTED=1; echo; echo "== interrupted: stopping the load, then writing the report"' INT TERM
echo "== load: $TOOL $PROFILE"
if [[ "$TOOL" == k6 ]]; then
  (cd "$KIT/load/k6" && OUT="$RESULTS" RUN="$RUN" ./run_k6.sh "$PROFILE" "$@")
else
  (cd "$KIT/load/jmeter" && OUT="$RESULTS" RUN="$RUN" ./run_jmeter.sh "$PROFILE" "$@")
fi
rc=$?
(( INTERRUPTED )) || sleep "${SAMPLER_TAIL:-8}"     # catch the last samples after the load
stop_samplers
trap - INT TERM

ENDED_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if (( INTERRUPTED )); then status=interrupted
elif [[ $rc -eq 0 || $rc -eq 99 ]]; then status=completed      # 99 = a k6 threshold failed: still a full run
else status=load_failed; fi
write_meta "$status" "$rc"
echo "== load finished: status=$status (exit code $rc)"

# ---------------------------------------------------------------- report
# shellcheck disable=SC2086
if python3 "$KIT/analysis/analyze_run.py" "$DIR" ${TARGET_RATE:+--target-rate $TARGET_RATE} ${ANALYZE_ARGS:-}; then
  echo
  echo "== report: $DIR/report.html"
  echo "== index:  $RESULTS/index.html"
  command -v wslpath >/dev/null && echo "   open on Windows: explorer.exe \"\$(wslpath -w $DIR/report.html)\""
else
  python3 "$KIT/analysis/build_index.py" "$RESULTS" >/dev/null
  echo "== no report: the load produced no usable results (see $DIR/*.log). Run folder kept and listed in the index."
  exit 1
fi

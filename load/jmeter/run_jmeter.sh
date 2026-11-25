#!/usr/bin/env bash
# Builds the Open Model arrival schedule and runs rrb_session.jmx in non-GUI mode.
#
#   ./run_jmeter.sh smoke|ramp|spike
#
# Rates below are TOTAL sessions/s across all generators. With REMOTE set, each JMeter
# server runs the whole schedule, so the script divides the rate by the server count.
#
# Env (defaults in brackets):
#   HOST [localhost] PORT [8080] PROTOCOL [http]   load balancer
#   KEYS [../keys.csv]       key file; must exist at the SAME path on every remote server
#   CEN_ID [202501]
#   REMOTE                   comma-separated JMeter servers (jmeter-server running on each)
#   SMOKE_RATE [20] SMOKE_DURATION [1 min]
#   STEPS [5000,...,35000] STEP_HOLD [10 min] STEP_RAMP [30 s]
#   SPIKE_RATE [25000] SPIKE_RISE [30 s] SPIKE_HOLD [10 min] IDLE_RATE [10]
#   OUT [../../results = <kit>/results]  RUN [<profile>-<UTC time>]
#                            output: $OUT/$RUN/ with jmeter.jtl, jmeter-run.log, report/ (HTML dashboard)
#                            and jmeter-plan.json (step windows for analysis/analyze_run.py)
#   SETTLE [30, 0 for smoke] seconds skipped at the start of each hold before a step is measured
#   HEAP ["-Xms4g -Xmx4g"]   generator JVM heap (use 8-16g on 32-core generator nodes)
set -euo pipefail
cd "$(dirname "$0")"

PROFILE=${1:-smoke}
HOST=${HOST:-localhost}; PORT=${PORT:-8080}; PROTOCOL=${PROTOCOL:-http}
KEYS=${KEYS:-$(realpath -m ../keys.csv)}
CEN_ID=${CEN_ID:-202501}
OUT=${OUT:-../../results}
export HEAP=${HEAP:-"-Xms4g -Xmx4g"}

NODES=1
if [[ -n "${REMOTE:-}" ]]; then
  IFS=, read -ra _r <<< "$REMOTE"; NODES=${#_r[@]}
fi
per() { awk -v t="$1" -v n="$NODES" 'BEGIN { r = t / n; if (r == int(r)) printf "%d", r; else printf "%.3f", r }'; }
secs() {   # "10 min" / "30 s" / "1 h" -> seconds
  awk -v d="$1" 'BEGIN { n = d + 0; u = d; sub(/^[0-9.]+ */, "", u);
    m = (u ~ /^h/) ? 3600 : (u ~ /^m/) ? 60 : 1; printf "%d", n * m }'
}
# Measurement windows (seconds from test start, TOTAL target rate) for analyze_run.py
WINDOWS=""
win() {   # label/target, start, duration
  local skip=$(( SETTLE < $3 / 2 ? SETTLE : $3 / 2 ))
  WINDOWS+="${WINDOWS:+,}{\"label\":\"$1\",\"target\":$1,\"from\":$(( $2 + skip )),\"to\":$(( $2 + $3 ))}"
}

SETTLE=${SETTLE:-$([[ "$PROFILE" == smoke ]] && echo 0 || echo 30)}
case "$PROFILE" in
  smoke)
    r=$(per "${SMOKE_RATE:-20}"); d=${SMOKE_DURATION:-1 min}
    SCHEDULE="rate(${r}/s) random_arrivals(${d}) rate(${r}/s)"
    win "${SMOKE_RATE:-20}" 0 "$(secs "$d")" ;;
  ramp)
    hold=${STEP_HOLD:-10 min}; up=${STEP_RAMP:-30 s}
    SCHEDULE="rate(0/s)"; t=0
    IFS=, read -ra steps <<< "${STEPS:-5000,10000,15000,20000,25000,30000,35000}"
    for s in "${steps[@]}"; do
      r=$(per "$s")
      SCHEDULE+=" random_arrivals(${up}) rate(${r}/s) random_arrivals(${hold}) rate(${r}/s)"
      t=$(( t + $(secs "$up") )); win "$s" "$t" "$(secs "$hold")"; t=$(( t + $(secs "$hold") ))
    done
    SCHEDULE+=" random_arrivals(30 s) rate(0/s)" ;;
  spike)
    i=$(per "${IDLE_RATE:-10}"); r=$(per "${SPIKE_RATE:-25000}")
    SCHEDULE="rate(${i}/s) random_arrivals(1 min) rate(${i}/s) random_arrivals(${SPIKE_RISE:-30 s}) rate(${r}/s) random_arrivals(${SPIKE_HOLD:-10 min}) rate(${r}/s) random_arrivals(30 s) rate(0/s)"
    win "${IDLE_RATE:-10}" 0 60
    t=$(( 60 + $(secs "${SPIKE_RISE:-30 s}") )); win "${SPIKE_RATE:-25000}" "$t" "$(secs "${SPIKE_HOLD:-10 min}")" ;;
  *) echo "usage: $0 smoke|ramp|spike" >&2; exit 2 ;;
esac

SCHEDULE+=" pause(10 s)"   # let in-flight sessions finish before JMeter stops threads
RUN=${RUN:-${PROFILE}-$(date -u +%Y%m%dT%H%M%SZ)}
DIR="$OUT/$RUN"
mkdir -p "$DIR"
echo "run=$RUN nodes=$NODES target=$PROTOCOL://$HOST:$PORT keys=$KEYS dir=$DIR"
echo "schedule (per node): $SCHEDULE"

if [[ -n "${REMOTE:-}" ]]; then
  P=(-R "$REMOTE" -X -Ghost="$HOST" -Gport="$PORT" -Gprotocol="$PROTOCOL" -Gkeys_csv="$KEYS" -Gcen_id="$CEN_ID" -Gschedule="$SCHEDULE")
else
  P=(-Jhost="$HOST" -Jport="$PORT" -Jprotocol="$PROTOCOL" -Jkeys_csv="$KEYS" -Jcen_id="$CEN_ID" -Jschedule="$SCHEDULE")
fi

launch_ms=$(date +%s%3N)
rc=0
jmeter -n -t rrb_session.jmx "${P[@]}" \
  -Jjmeter.save.saveservice.output_format=csv \
  -Jjmeter.save.saveservice.timestamp_format=ms \
  -Jsummariser.interval=10 \
  -l "$DIR/jmeter.jtl" -j "$DIR/jmeter-run.log" -e -o "$DIR/report" || rc=$?

# Test start = JMeter's "Running the test!" log line (after JVM start-up); launch time as fallback.
start_ms=$launch_ms
line=$(grep -m1 -E 'Running the test!|Starting distributed test|Start remote engines' "$DIR/jmeter-run.log" | cut -c1-23 | tr ',' '.')
if [[ -n "$line" ]] && t=$(date -d "$line" +%s%3N 2>/dev/null); then start_ms=$t; fi
printf '{"tool":"jmeter","profile":"%s","start_ms":%s,"nodes":%s,"windows":[%s]}\n' \
  "$PROFILE" "$start_ms" "$NODES" "$WINDOWS" > "$DIR/jmeter-plan.json"

echo "results: $DIR/jmeter.jtl   dashboard: $DIR/report/index.html   plan: $DIR/jmeter-plan.json"
exit $rc

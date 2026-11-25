#!/usr/bin/env bash
# Runs rrb_session.js and saves what analysis/analyze_run.py needs into one run directory.
#
#   RUN=R-ON ./run_k6.sh ramp [extra k6 args, e.g. --execution-segment "0:1/4" ...]
#
# Writes to $OUT/$RUN/ (default ../../results/<profile>-<UTC time>/):
#   k6-summary-<node>.json   end-of-test summary incl. per-step sub-metrics
#   k6-plan-<node>.json      step windows + wall-clock start (from the RRB_PLAN log line)
#   k6-<node>.log            console output
#   k6-dashboard-<node>.html k6's dashboard: client-side rate / latency / VUs over time
# All rrb_session.js env vars (BASE_URL, KEYS, STEPS, STEP_HOLD, SETTLE, ...) pass through.
# With several generators, use the same RUN on each and copy their files into one directory.
set -uo pipefail
cd "$(dirname "$0")"

PROFILE=${1:-smoke}; shift || true
RUN=${RUN:-${PROFILE}-$(date -u +%Y%m%dT%H%M%SZ)}
OUT=${OUT:-../../results}
NODE=${NODE:-$(hostname -s)}
DIR="$OUT/$RUN"
mkdir -p "$DIR"

echo "run=$RUN profile=$PROFILE node=$NODE dir=$DIR"
# Ctrl+C: k6 stops gracefully and still writes its summary; tee ignores the signal so the
# log is complete, and this script carries on to save the step plan.
trap '[[ -f "$DIR/k6-interrupted-$NODE" ]] || date +%s.%N > "$DIR/k6-interrupted-$NODE"
      echo "run_k6.sh: interrupted, saving what k6 recorded" >&2' INT TERM
# k6's own HTML dashboard: client-side request rate, latency percentiles, VUs and dropped
# iterations over time, aggregated per K6_DASHBOARD_PERIOD (no HTTP port is opened). DASHBOARD=0 disables.
if [[ "${DASHBOARD:-1}" == 1 ]]; then
  export K6_WEB_DASHBOARD=true K6_WEB_DASHBOARD_PORT=-1 K6_WEB_DASHBOARD_PERIOD="${K6_DASHBOARD_PERIOD:-5s}"
  export K6_WEB_DASHBOARD_EXPORT="$DIR/k6-dashboard-$NODE.html"
fi
k6 run -e PROFILE="$PROFILE" \
  --summary-trend-stats 'avg,med,p(95),p(99),p(99.9),max,count' \
  --summary-export "$DIR/k6-summary-$NODE.json" \
  "$@" rrb_session.js 2>&1 | (trap '' INT TERM; tee "$DIR/k6-$NODE.log")
rc=${PIPESTATUS[0]}
trap - INT TERM

plan=$(grep -o 'RRB_PLAN [A-Za-z0-9+/=]*' "$DIR/k6-$NODE.log" | head -1 | cut -d' ' -f2)
end_ms=$(grep -o 'RRB_END [0-9]*' "$DIR/k6-$NODE.log" | head -1 | cut -d' ' -f2)
if [[ -n "$plan" ]]; then
  echo "$plan" | base64 -d | python3 -c 'import json,sys; p=json.load(sys.stdin); e=sys.argv[1]
if e: p["end_ms"]=int(e)
print(json.dumps(p))' "${end_ms:-}" > "$DIR/k6-plan-$NODE.json"
else
  echo "WARNING: no RRB_PLAN line in the k6 output; per-step analysis is not possible" >&2
fi
echo "k6 exit code $rc (99 = a threshold failed). Files in $DIR"
exit $rc

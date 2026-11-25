// RRB result-release session: login -> result -> cut-off, one candidate per iteration.
//
// Open-loop: k6's ramping-arrival-rate starts SESSIONS at the target rate whatever the
// response time (no coordinated omission). The rate is SESSIONS/s; HTTP requests/s is ~3x.
//
//   k6 run -e PROFILE=smoke rrb_session.js
//   k6 run -e PROFILE=ramp  -e BASE_URL=http://lb:80 -e KEYS=keys.csv rrb_session.js
//   k6 run -e PROFILE=spike -e BASE_URL=http://lb:80 -e KEYS=keys.csv rrb_session.js
//
// Distributed (N generator nodes): run the SAME command on every node with its own
// execution segment, k6 splits the arrival rate and the key list between them:
//   node1: --execution-segment "0:1/4"   --execution-segment-sequence "0,1/4,2/4,3/4,1"
//   node2: --execution-segment "1/4:2/4" --execution-segment-sequence "0,1/4,2/4,3/4,1"  ...
//
// Env:
//   PROFILE     smoke | ramp | spike                                 (default smoke)
//   BASE_URL    load balancer URL                                    (default http://localhost:8080)
//   KEYS        CSV from gen_zipf_csv.py                             (default ../keys.csv)
//   CEN_ID      notification id                                      (default 202501)
//   STEPS       ramp steps, sessions/s                               (default 5000,...,35000)
//   STEP_HOLD   hold per step                                        (default 10m)
//   STEP_RAMP   transition between steps                             (default 30s)
//   SPIKE_RATE  spike target, sessions/s                             (default 25000)
//   SPIKE_RISE  idle -> target                                       (default 30s)
//   SPIKE_HOLD  hold at target                                       (default 10m)
//   IDLE_RATE   "idle" rate before the spike                         (default 10)
//   SMOKE_RATE / SMOKE_DURATION                                      (default 20/s, 1m)
//   PRE_VUS / MAX_VUS  VU pool PER RUN (split across segments)       (default by profile)
//   NEW_CONN=1  new TCP connection per session (closer to real browsers, costlier for k6)
//   THINK_MS    pause between the three calls, ms                    (default 0)
//   SETTLE      skipped at the start of every hold before the step is measured
//                                                (default 30s; 0s for smoke)
//   STEP_STATS=0  disable per-step tagging/summary (on by default)
//
// Per-step results: every iteration is tagged step=<target rate> while the arrival rate
// holds steady (after SETTLE), and step=transition while it ramps. Run through
// run_k6.sh, which exports the per-step summary and the step plan for analysis/analyze_run.py.
import http from 'k6/http';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const CEN = parseInt(__ENV.CEN_ID || '202501', 10);
const PROFILE = (__ENV.PROFILE || 'smoke').toLowerCase();
const THINK = parseInt(__ENV.THINK_MS || '0', 10) / 1000;

// Loaded once and shared read-only by all VUs (memory ~ file size, not x VUs).
const keys = new SharedArray('keys', () => {
  const lines = open(__ENV.KEYS || '../keys.csv').split('\n');
  const out = [];
  for (let i = 1; i < lines.length; i++) {          // skip header
    const l = lines[i].trim();
    if (!l) continue;
    const p = l.split(',');
    out.push([p[0], p[1]]);                           // registration_no, dob
  }
  if (out.length === 0) throw new Error('key file is empty');
  return out;
});

const loginFailed = new Counter('rrb_login_failed');
const sessionsOk = new Counter('rrb_sessions_ok');

function rampStages() {
  const steps = (__ENV.STEPS || '5000,10000,15000,20000,25000,30000,35000').split(',').map(Number);
  const hold = __ENV.STEP_HOLD || '10m';
  const ramp = __ENV.STEP_RAMP || '30s';
  const st = [];
  for (const s of steps) {
    st.push({ target: s, duration: ramp });
    st.push({ target: s, duration: hold });
  }
  st.push({ target: 0, duration: '30s' });
  return st;
}

function scenario() {
  switch (PROFILE) {
    case 'ramp':
      return {
        executor: 'ramping-arrival-rate', startRate: 0, timeUnit: '1s', stages: rampStages(),
        preAllocatedVUs: parseInt(__ENV.PRE_VUS || '8000', 10), maxVUs: parseInt(__ENV.MAX_VUS || '40000', 10),
      };
    case 'spike': {
      const rate = parseInt(__ENV.SPIKE_RATE || '25000', 10);
      return {
        executor: 'ramping-arrival-rate', startRate: parseInt(__ENV.IDLE_RATE || '10', 10), timeUnit: '1s',
        stages: [
          { target: parseInt(__ENV.IDLE_RATE || '10', 10), duration: '1m' },   // idle baseline
          { target: rate, duration: __ENV.SPIKE_RISE || '30s' },              // release
          { target: rate, duration: __ENV.SPIKE_HOLD || '10m' },              // storm
          { target: 0, duration: '30s' },
        ],
        // A spike needs the VUs up front; k6 allocating VUs mid-spike would itself be a bottleneck.
        preAllocatedVUs: parseInt(__ENV.PRE_VUS || '15000', 10), maxVUs: parseInt(__ENV.MAX_VUS || '40000', 10),
      };
    }
    case 'smoke':
      return {
        executor: 'constant-arrival-rate', rate: parseInt(__ENV.SMOKE_RATE || '20', 10), timeUnit: '1s',
        duration: __ENV.SMOKE_DURATION || '1m',
        preAllocatedVUs: parseInt(__ENV.PRE_VUS || String(Math.max(50, parseInt(__ENV.SMOKE_RATE || '20', 10))), 10),
        maxVUs: parseInt(__ENV.MAX_VUS || '2000', 10),
      };
    default:
      throw new Error(`unknown PROFILE ${PROFILE}`);
  }
}

function seconds(d) {                       // '30s', '10m', '1m30s', '2h' -> seconds
  let s = 0;
  for (const [, n, u] of String(d).matchAll(/(\d+(?:\.\d+)?)(ms|s|m|h)/g)) {
    s += parseFloat(n) * { ms: 0.001, s: 1, m: 60, h: 3600 }[u];
  }
  return s;
}

// Measurement windows (seconds from scenario start): every stage that holds a constant
// rate, minus SETTLE at its start. Labels are the target rate (sessions/s, all generators).
function stepWindows(sc) {
  const settle = seconds(__ENV.SETTLE || (PROFILE === 'smoke' ? '0s' : '30s'));
  const out = [];
  const add = (target, from, dur) => {
    if (target <= 0 || dur <= 0) return;
    let label = String(target);
    while (out.some((w) => w.label === label)) label += '+';
    const skip = Math.min(settle, dur / 2);
    out.push({ label, target, from: from + skip, to: from + dur });
  };
  if (sc.executor === 'constant-arrival-rate') {
    add(sc.rate, 0, seconds(sc.duration));
  } else {
    let t = 0, level = sc.startRate || 0;
    for (const st of sc.stages) {
      const dur = seconds(st.duration);
      if (st.target === level) add(st.target, t, dur);
      level = st.target;
      t += dur;
    }
  }
  return out;
}

const SCENARIO = scenario();
const STEP_STATS = __ENV.STEP_STATS !== '0';
const WINDOWS = STEP_STATS ? stepWindows(SCENARIO) : [];
// Scenario length on k6's own scheduling clock. Steps are placed with exec.scenario.progress
// (that clock), not Date.now(): the wall clock can drift from the scheduler (measured 1-3.5 %
// on WSL2), which would shift sessions between steps.
const SCENARIO_S = SCENARIO.executor === 'constant-arrival-rate'
  ? seconds(SCENARIO.duration) : SCENARIO.stages.reduce((t, st) => t + seconds(st.duration), 0);

// Thresholds that always pass: they only make k6 keep per-step sub-metrics in the summary export.
const stepThresholds = {};
for (const w of WINDOWS) {
  for (const n of ['login', 'result', 'cutoff']) stepThresholds[`http_req_duration{name:${n},step:${w.label}}`] = ['max>=0'];
  stepThresholds[`http_req_failed{step:${w.label}}`] = ['rate>=0'];
  stepThresholds[`iterations{step:${w.label}}`] = ['count>=0'];
  stepThresholds[`rrb_sessions_ok{step:${w.label}}`] = ['count>=0'];
  stepThresholds[`rrb_login_failed{step:${w.label}}`] = ['count>=0'];
}

export function setup() {
  // run_k6.sh reads this line to place the steps on the wall clock (to match the DB samplers).
  const plan = { start_ms: Date.now(), profile: PROFILE, windows: WINDOWS, base_url: BASE, scenario_s: SCENARIO_S };
  console.log(`RRB_PLAN ${encoding.b64encode(JSON.stringify(plan))}`);
}

export function teardown() {
  // wall-clock end of the scenario: lets analyze_run.py correct for wall-clock drift when it
  // places the steps on the samplers' (wall-clock) timeline
  console.log(`RRB_END ${Date.now()}`);
}

export const options = {
  scenarios: { [PROFILE]: SCENARIO },
  noVUConnectionReuse: __ENV.NEW_CONN === '1',
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'p(99.9)', 'max'],
  // Acceptance targets from the proposal (end-to-end, measured at the client).
  thresholds: {
    'http_req_duration{name:login}':  ['p(99)<500'],
    'http_req_duration{name:result}': ['p(99)<300'],
    'http_req_duration{name:cutoff}': ['p(99)<300'],
    'http_req_failed':                ['rate<0.001'],
    // Sessions k6 could not START on time = generator or VU-pool limit: the run is void.
    'dropped_iterations':             ['count<1'],
    ...stepThresholds,
  },
};

function tagStep() {
  if (!STEP_STATS) return;
  const el = exec.scenario.progress * SCENARIO_S;
  let step = 'transition';
  for (const w of WINDOWS) {
    if (el >= w.from && el < w.to) { step = w.label; break; }
  }
  exec.vu.tags.step = step;          // applies to every metric of this iteration
}

const JSON_HDR = { 'Content-Type': 'application/json' };

export default function () {
  tagStep();
  // iterationInTest is unique across all execution segments, so N generators walk
  // disjoint parts of the key file (wrapping at the end).
  const k = keys[exec.scenario.iterationInTest % keys.length];

  const login = http.post(`${BASE}/api/v1/auth/login`,
    JSON.stringify({ registrationNo: k[0], dob: k[1], cenId: CEN }),
    { headers: JSON_HDR, tags: { name: 'login' },
      // 401 is an expected answer when the CSV contains bad-DOB rows; not a transport failure.
      responseCallback: http.expectedStatuses(200, 401) });

  if (login.status !== 200) {
    if (login.status === 401) loginFailed.add(1);                 // expected for bad-DOB rows
    else check(login, { 'login 200': (r) => r.status === 200 });    // real failures only
    return;
  }
  const token = login.json('token');
  if (THINK) sleep(THINK);

  const res = http.get(`${BASE}/api/v1/results/${CEN}`,
    { headers: { Authorization: `Bearer ${token}` }, tags: { name: 'result' } });
  const ok = check(res, { 'result 200': (r) => r.status === 200 });
  if (!ok) return;
  if (THINK) sleep(THINK);

  const rrb = res.json('rrbCode');
  const cat = res.json('category');
  const cut = http.get(`${BASE}/api/v1/cutoffs/${CEN}?rrbCode=${rrb}&category=${cat}`,
    { tags: { name: 'cutoff' } });
  if (check(cut, { 'cutoff 200': (r) => r.status === 200 })) sessionsOk.add(1);
}

# Feature Specification: Back to normal before the next scenario's baseline

**Feature Branch**: `scenario1-attach-workload`
**Created**: 2026-11-27
**Status**: Implemented (unit tests); live run pending
**Touches**: the common workload's health check (specs/007, owner: gowri-shangarar) -- agree with the owner before merge

## Why

With one common workload for a whole session, scenarios run one after another. Each scenario
measures its own baseline and judges its fault against it. On e2-u22-3 (26 Nov 2026) NL-N-I01a's
baseline p99 was 11 ms; NL-N-I01b took its baseline 61 s after NL-N-I01a ended, at 40 ms, because
the database had not recovered. NL-N-I01b's p99 ratio came out 15.4x instead of about 56x, and its
back-to-normal bound was 60 ms instead of 16.5 ms. The health check between scenarios looked only at
the rate (75%) and errors (1%), so it let the slow baseline through.

A slow baseline makes every "how much slower" rule easier to pass: a possible false pass for any
scenario that judges ratios and runs after another (constitution: never a false pass).

## Decisions

- **The rule** is the harness's own "back to normal", Framework §6.2 (rto_decomposer): over the last
  60 s, p99 <= **1.5x** and TPS >= **80%** of the reference. Chosen over the 2x first proposed: it is
  the definition the Framework already uses for recovery, so "ready for the next baseline" and
  "recovered" mean the same thing. p99 over the minute from the summed latency histograms (every
  transaction), never a per-second value.
- **The reference** is the workload's own: the last 10 minutes of its measured 1-second samples
  before the first fault on it (first T0 mark), leaving out each scenario's start-up (run_start to
  its new `baseline` mark; init may restart the database). At least 5 minutes are needed.
  NEEDS SIGN-OFF: the 10 / 5 minute lengths (not the rule).
- **Before any fault** has run on the workload, the delay is not checked (recorded as not needed).
- **Between scenarios** the existing wait (cooldown 60 s, then up to 600 s) also waits for back to
  normal. Not back by then: the existing rule -- that scenario fails with the reason and the rest
  are skipped. Never a pass.
- **At the scenario's baseline** the check is run again and recorded (facts
  `back_to_normal_before_baseline`, a disclosure line, event `back_to_normal_check`). If it does not
  hold or cannot be judged, the run stops (PhaseAbort): never judged on a baseline not shown normal.
- **New mark** `baseline` in the workload's scenario marks (the baseline window opens).

## Requirements

- FR-001 The reference is computed only from measured samples before the first T0, outside every
  scenario's start-up; fewer than 300 s, or samples without latency histograms: not judgeable.
- FR-002 The check fails when the last 60 s has p99 > 1.5x or TPS < 80% of the reference, has less
  than 58.5 s of samples, or has a sample longer than 1.5 s; the message names the figures.
- FR-003 Not judgeable after a fault is never taken as healthy.
- FR-004 The health check between scenarios includes the check; the attach in init does not refuse on
  it (the baseline judges it, moments later); the baseline records it and stops the run if it fails.
- FR-005 The reference is read once per health-check object (fixed after the first fault) and the
  file is read only up to the first fault.

## Also in this change (NL-N-I01b review of run 1d7e0e)

- The memory guard is disarmed as soon as the reports are ended (it stayed armed about 15 minutes
  into recovery and validate).
- The "1b findings" helpers line names the server-wide `max_parallel_workers`, the operator limit
  that held the tenant to 6 helpers per report and 7 at once.

## Known consequence

The database drifts slower during a long session (dead rows grew from 1.1 M to 8.9 M over 1a and 1b).
With this check, a later scenario may wait the full 600 s and then be skipped. That is the honest
outcome; how to keep the database from drifting is a separate decision for the lead.

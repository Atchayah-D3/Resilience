# Research: NL-N-I03

## R1 Internal id
- Decision: `NL-N-I03` with lettered variants per run; excluded from checks 2 and 6.
- Rationale: Framework §10.6 has no such row; a numbered Framework id would break reconciliation.
- Alternatives: NL-N-09 (claims a Framework row that does not exist); three internal numbers (loses the one id).

## R2 Separate driver
- Decision: `OsSshLockPileupDriver` registered for the os_ssh section, like the corruption driver.
- Rationale: the process driver's fault-type set is pinned by tests; adding there changes it.

## R3 Hold ends on queue clear
- Decision: poll the adapter's state; end at the schema change's end or `fault.duration`.
- Rationale: the freeze is the measure; a fixed hold would mix the freeze with steady state.

## R4 Probe columns dropped at cleanup, not at release
- Decision: release commits the read only; cleanup's revert (after the workload stops) drops the column.
- Rationale: a drop needs the exclusive lock and would create a second, smaller pile-up during recovery.

## R5 Table users never time out by themselves
- Decision: `SET LOCAL lock_timeout = 0, statement_timeout = 0` in their transactions.
- Rationale: the wait must be measured as latency; the driver's own 10 s bound still applies and is disclosed.

## R6 Crash-restart evidence
- Decision: count the postmaster's crash-restart log lines after T0.
- Rationale: a backend crash restarts all sessions without changing the postmaster's start time.

# Contract: scenarios-junit.xml

```xml
<testsuites name="resilience-scenarios" tests="N" failures="F" errors="E" skipped="S" time="T">
  <testsuite name="NL" tests="..." failures="..." errors="..." skipped="..." time="...">
    <testcase classname="scenarios.NL-M" name="NL-M-03[E2:standalone:shaktidb-standalone]" time="598.1">
      <properties>
        <property name="scenario_id" value="NL-M-03"/>
        <property name="priority" value="P1"/>
        <property name="status" value="failed"/>
        <property name="run_id" value="NL-M-03-20261006T075615Z-5bd741"/>
        <property name="evidence_dir" value="/home/.../resilience-runs/NL-M-03-..."/>
        <property name="report" value="/home/.../report.html"/>
      </properties>
      <failure message="2 of 8 acceptance rules did not pass" type="verdict">
relations_left_unvacuumed == 0 -> fail (values: relations_left_unvacuumed=1)
rpo_txn == 0 -> not_measured (rpo_txn was not measured: ...)
      </failure>
    </testcase>
  </testsuite>
</testsuites>
```

Rules:
- One `<testsuite>` per tier prefix (NL / CL / DX); one `<testcase>` per scenario case.
- `passed` -> no child element. `failed` -> `<failure type="verdict">` listing every rule whose
  outcome is not `pass`, with values and reason. `aborted` / `error` / `no_result` ->
  `<error type="aborted|error|no_result">` with the reason and the phase that stopped.
  `stopped_before_fault` / `skipped` -> `<skipped message="reason">`.
- Properties always include `scenario_id`, `priority`, `status`; plus `run_id`,
  `evidence_dir`, `report` when a run happened, and `report_error` if the page failed.
- Counts in `<testsuite>` / `<testsuites>` match the children.

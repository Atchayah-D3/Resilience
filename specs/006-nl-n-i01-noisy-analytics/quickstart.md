# Quickstart: NL-N-I01

On the harness machine, in the harness folder.

1. Operator setup (done 2026-11-15): `analytics_role` with its limits and read access to
   `resilience_tpcc`, its `pg_hba.conf` line, its password in `~/.pgpass`; `pg_monitor` granted
   to `harness`; `shared_buffers` 1 GB.
2. First run: it loads the TPC-C data at the size calculated from the memory cache (about 280
   warehouses for 1 GB, about 30 minutes, once), then stops because no maximum is stored for the
   layout, and prints the command to measure it:
   ```bash
   .venv/bin/pytest resilience_tests/test_scenarios.py --env e2-u22-3 --scenario NL-N-I01 -q -rA
   ```
3. Measure the customers' maximum for the same layout (64 clerks on the active warehouses):
   ```bash
   .venv/bin/python -m resilience_tests.execution.workload.tpcc.capacity --env e2-u22-3 --clerks 64 --active-warehouses by_cache
   ```
4. Run again: the data is reused, the rate is 60% of the stored maximum, and the run ends with a verdict.
5. Expected: every check measured; the layout, the role's settings, the reports' outcomes and plans
   in `results.json`; the plan and the outcomes in `summary.txt` under "noisy analytics (NL-N-I01)".

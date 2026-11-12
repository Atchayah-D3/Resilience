# TPC-C speed test

Build step 0 of the TPC-C-like workload ([docs/tpcc-build-plan.md](../../docs/tpcc-build-plan.md)).
It answers one question before the engine is built: **can one Python process drive enough TPC-C
load, or must the engine be written in Go?**

## What it does

1. **Setup:** creates schema `resilience_speedtest` on the target, with a cut-down TPC-C data set
   and one database function that does the work of one New-Order plus the marker row
   ([schema.sql](schema.sql)).
2. **Run:** for 1, 2, 4 and 8 worker processes, each with 16 clerks, it sends New-Orders as fast
   as possible. A step that would need more connections than the target has free (keeping 10
   slots spare) is skipped, never forced. Per transaction each clerk does exactly what the real engine will do:
   - create the inputs (TPC-C Clause 2.4.1)
   - write the marker line and flush it to disk, using the harness's own journal code
   - send **one call** to the function
   - write the "committed" line (with the order number) or the "rolled back" line
3. **Checks** after every step (is the measurement true?):

   | Check | Passes when |
   |---|---|
   | counts agree | calls counted = "committed" lines = unique "committed" lines |
   | every committed transaction is in the database | no committed marker is missing from the database |
   | no unknown rows in the database | every marker row in the database has a marker line |
   | every committed line has a marker line before it | the journal order is right |
   | no rolled-back transaction in the database | rolled-back calls left nothing behind |
   | every committed New-Order is complete | its order, its new-order row and exactly its number of lines exist |
   | no errors and no unknown outcomes | nothing failed in a run without a fault |
   | journal flushes really happen | at least one flush, and never more flushes than lines |
   | business rollbacks are about 1% | 0.5%–1.5% of New-Orders (checked from 5,000 New-Orders) |

4. **Teardown:** drops the schema. The target is left as found. No server or role setting is
   changed.

## How to run it (on the harness machine, in `~/resilience`)

```bash
# everything in one go: setup, the 4 steps, teardown (teardown also runs after an error)
.venv/bin/python -m tools.tpcc_speedtest all --env e2-dedicated-vm --target-max-tps 5000

# or one part at a time
.venv/bin/python -m tools.tpcc_speedtest setup    --env e2-dedicated-vm
.venv/bin/python -m tools.tpcc_speedtest run      --env e2-dedicated-vm --processes 1,2,4,8
.venv/bin/python -m tools.tpcc_speedtest teardown --env e2-dedicated-vm
```

- `--target-max-tps`: the maximum TPS of the biggest target we plan to test (from the lead).
  Without it, the report shows the formula instead of the number of cores.
- `--out`: the output folder. Needed when you are not the user whose `run_dir` the environment file
  names, e.g. `--out ~/resilience-runs/speedtest-1`.
- `--clerks`, `--duration`, `--warmup`, `--warehouses`: see `--help`.
- Safety, as in the harness: the target's hostname (read over SSH) must match the environment's
  allowlist, and the sentinel table must have a row for it. Otherwise the test refuses to start.
- About 5 minutes for the default 4 steps, plus about 1 minute to load the data.

## How to read the result

The result is in `<driver_host.run_dir>/speedtest-<time>/summary.md` (or the folder given with `--out`; the
folder is created before any database work, so a folder you cannot write is refused at once) (all numbers in `results.json`;
the journals of each step in `processes-<n>/`).

| Column | Meaning |
|---|---|
| TPS per process | committed New-Orders per second, per Python process |
| Python CPU (lowest process) | how busy the least busy Python process was, as a share of one core |
| Python is the limit | **yes** when every process used at least 85% of a core: the TPS is then Python's real speed |
| Target CPU busy / iowait | how busy the database machine was |
| Checks | **pass** only when every check above passed; a step that fails is never used |

**The decision** (at the end of `summary.md`):

```
cores needed = (target maximum TPS × 1.2) ÷ (TPS of one Python process)
```

- **Python was the limit in some step:** the TPS per process is Python's real speed.
- **Python was never the limit:** the database, the harness disk or the network was the limit
  first. The TPS per process is then only a lower bound, so the number of cores is a maximum. The
  report names the likely limit.

If the cores needed fit the harness machines we have or can get: **Python**. If not: the engine
is written in **Go** (proposal 6.3).

## Tests

```bash
.venv/bin/python -m pytest tests/test_tpcc_speedtest.py -q
```

The unit tests cover the TPC-C input rules (each with its clause), the statistics, every check
(each must fail for its own reason) and the decision formula.

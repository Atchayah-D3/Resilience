# Quickstart: validate NL-I-02

On the harness VM, in `~/resilience` (back it up before replacing code, CLAUDE.md):

```bash
.venv/bin/pip install -e '.[test]'                 # main now needs Jinja2 (reporting)
.venv/bin/python -m pytest tests/test_index_corruption.py tests/test_heap_corruption.py -q
.venv/bin/python -m pytest tests -q                 # full suite (~7 min)
.venv/bin/python -m catalog.schema --partial

# live (stops/starts PostgreSQL on .112, damages only the harness's own index)
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
    --scenario NL-I-02 --target-is-disposable -q -rA
# NL-I-01 shares the driver and adapter code: re-run it too
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
    --scenario NL-I-01 --target-is-disposable -q -rA
```

Expected (NL-I-02): `passed`; in `summary.txt` the injected leaf block, the entry corrupted
(line pointer, key, heap row pointer before -> after) and the `pg_checksums` failure at that
block, index reads failing with `XX001` naming that block, `amcheck on the damaged relation:
detected=True`, REINDEX ok, read-after 20000 rows via the index, check-after clean. Measured:
`wrong_results_returned = 0`, `detection_identifies_block = True`, `reindex_restores = True`.
An abort "could not locate the byte to corrupt" means the page did not look as expected; the
reason is in the message and `located` is absent -- nothing was written.

After the run: `resilience.corruption_target` no longer exists; the ledger has no outstanding
entry.

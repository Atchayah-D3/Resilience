# Quickstart: NL-N-I03

On the harness VM, in the harness checkout:

```bash
.venv/bin/python -m catalog.schema --partial
.venv/bin/python -m pytest tests -q
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm-u22-2 --scenario NL-N-I03a --stop-before-fault -q -rA
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm-u22-2 --scenario NL-N-I03a -q -rA   # judged
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm-u22-2 --scenario NL-N-I03b -q -rA   # finding
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm-u22-2 --scenario NL-N-I03c -q -rA   # finding
.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm-u22-2
```

Expected: 3a passes with `ddl_lock_timeout_fired=true`, `queue_freeze_s <= 6`; 3b/3c pass on
`fault_confirmed` and the safety gates; afterwards no `resilience-lock-*` session and no
`resilience_probe_*` column remains.

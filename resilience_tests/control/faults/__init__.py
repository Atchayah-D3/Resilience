"""Per-fault behaviours of a run (see base.py). Importing the package registers them all."""

from resilience_tests.control.faults import (  # noqa: F401  (registers behaviours)
    connection_exhaustion,
    corruption,
    during,
    idle_transaction,
    service,
)
from resilience_tests.control.faults.base import (  # noqa: F401
    DuringBehaviour,
    FaultBehaviour,
    PhaseAbort,
    during_behaviour,
    fault_behaviour,
)

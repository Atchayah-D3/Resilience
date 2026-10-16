# Registration order matters: the kill switch falls back to the first driver registered for a
# profile section when a ledger entry names no fault type, and for os_ssh that is the process driver.
from resilience_tests.execution.injectors import process  # noqa: F401, I001  (registers drivers)
from resilience_tests.execution.injectors import corruption, power  # noqa: F401  (registers drivers)

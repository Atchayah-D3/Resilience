"""Workload profiles (see base.py). Importing the package registers them all."""

from resilience_tests.execution.workload.profiles.base import (  # noqa: F401
    Request,
    Result,
    Terminal,
    WorkloadProfile,
    profile_class,
    register_profile,
    registered_profiles,
)
from resilience_tests.execution.workload.tpcc import profile as _tpcc  # noqa: F401  (registers "tpcc")

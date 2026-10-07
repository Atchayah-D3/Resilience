"""pytest options for scenario execution (Arch §12: pytest parametrization + JUnit XML)."""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    g = parser.getgroup("resilience")
    g.addoption("--env", action="store", help="environment profile name (envs/<name>.yaml)")
    g.addoption("--scenario", action="append", default=[], help="scenario id to run (repeatable); default: all")
    g.addoption("--target-is-disposable", action="store_true",
                help="confirm the target is a scratch instance (required for NL-D, Arch §15)")
    g.addoption("--stop-before-fault", action="store_true",
                help="dry run: execute reset..pre_fault only, never inject, never produce a verdict")
    g.addoption("--reference-class", action="store", default="E2",
                help="reference environment class for env-insensitive scenarios (Arch §14: E2)")
    g.addoption("--workload", action="store", choices=["pgbench", "builtin"], default=None,
                help="override profile workload.generator (pgbench | builtin)")

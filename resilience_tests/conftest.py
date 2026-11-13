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
    g.addoption("--tpcc-warehouses", action="store", type=int, default=None,
                help="run the scenarios with the TPC-C workload (this many warehouses; the run loads them "
                     "if needed) in place of their own; disclosed in every report (TPC-C build steps 7, 10)")
    g.addoption("--tpcc-fresh-load", action="store_true",
                help="TPC-C runs load the data again before the baseline even when it is fine (default: only "
                     "when missing, of another size, grown or changed); disclosed in the report")

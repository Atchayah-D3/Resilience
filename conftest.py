"""Repository-wide pytest configuration: the report plugin (`--report-dir`, Arch §10.4) for
both the harness unit tests (tests/) and the scenario runs (resilience_tests/)."""

pytest_plugins = ["resilience_tests.reporting.plugin"]

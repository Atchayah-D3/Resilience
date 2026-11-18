"""The common long-running workload (specs/007-common-oltp-workload).

One TPC-C workload runs for hours, outside any scenario's 9 phases, started and stopped only by
its own commands (cli.py). No scenario starts, stops or changes it. Scenarios attach to it in a
later story; until then a scenario is refused while it runs (state.refuse_if_running).
"""

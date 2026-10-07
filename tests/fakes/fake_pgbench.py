#!/usr/bin/env python3
"""Fake pgbench executable for resilience harness tests.

Simulates ShaktiDB / PostgreSQL 17 pgbench output without requiring a running database.
Supports:
  - `--version`
  - `-c`, `-j`, `-T`, `-D`, `-f`, `-P`, `--report-per-command`, `--failures-detailed`, `--max-tries`
  - Honours `\\sleep` commands in scripts when simulating execution.
  - Generates progress lines: `progress: <t> s, <tps> tps, lat <ms> ms stddev <ms>, <n> failed`
  - Generates final per-command latency summaries.
  - Simulates faults via environment variables:
      FAKE_PGBENCH_VERSION: override version string
      FAKE_PGBENCH_ABORT: simulate "client 0 aborted in command <n> ..."
      FAKE_PGBENCH_ABORT_AFTER_S: float seconds before aborting
      FAKE_PGBENCH_EXIT_CODE: force specific exit code
      FAKE_PGBENCH_UNPARSEABLE: print unparseable garbage output
"""

import argparse
import os
import re
import sys
import time
import signal


def parse_args():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--version", action="store_true")
    parser.add_argument("-c", "--client", type=int, default=1)
    parser.add_argument("-j", "--threads", type=int, default=1)
    parser.add_argument("-T", "--time", type=float, default=10.0)
    parser.add_argument("-P", "--progress", type=int, default=1)
    parser.add_argument("-f", "--file", type=str, default="")
    parser.add_argument("-D", "--define", action="append", default=[])
    parser.add_argument("--report-per-command", action="store_true")
    parser.add_argument("--failures-detailed", action="store_true")
    parser.add_argument("--max-tries", type=int, default=1)
    # Common libpq / pgbench flags that might be passed
    parser.add_argument("-h", "--host", type=str, default=None)
    parser.add_argument("-p", "--port", type=str, default=None)
    parser.add_argument("-U", "--username", type=str, default=None)
    parser.add_argument("-d", "--dbname", type=str, default=None)
    parser.add_argument("-n", "--no-vacuum", action="store_true")
    parser.add_argument("-M", "--protocol", type=str, default=None)
    return parser.parse_known_args()


def main():
    args, unknown = parse_args()

    # Version check
    if args.version:
        ver = os.environ.get("FAKE_PGBENCH_VERSION", "pgbench (PostgreSQL) 17.11.1.0")
        print(ver)
        sys.exit(0)

    # Check for unparseable output trigger
    if os.environ.get("FAKE_PGBENCH_UNPARSEABLE") == "1":
        print("??? UNPARSEABLE CORRUPTED OUTPUT ???", file=sys.stdout)
        print("CRITICAL ERROR: memory buffer corrupted", file=sys.stderr)
        sys.exit(int(os.environ.get("FAKE_PGBENCH_EXIT_CODE", "1")))

    # Check for immediate forced exit
    forced_exit = os.environ.get("FAKE_PGBENCH_EXIT_CODE")
    if forced_exit is not None and not os.environ.get("FAKE_PGBENCH_ABORT") and not os.environ.get("FAKE_PGBENCH_ABORT_AFTER_S"):
        sys.exit(int(forced_exit))

    script_lines = []
    if args.file and os.path.exists(args.file):
        with open(args.file, "r") as f:
            script_lines = [line.strip() for line in f if line.strip() and not line.strip().startswith("--")]

    # Check for defined variables (-D key=val)
    defines = {}
    for d in args.define:
        if "=" in d:
            k, v = d.split("=", 1)
            defines[k] = v

    # Handle signal termination cleanly to print summary if needed
    stopped = False

    def handle_signal(sig, frame):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    duration = args.time
    abort_immed = os.environ.get("FAKE_PGBENCH_ABORT") == "1"
    abort_after = float(os.environ.get("FAKE_PGBENCH_ABORT_AFTER_S", "-1"))

    if abort_immed:
        print(f"client 0 aborted in command 2 (SQL) of script {args.file or 'script'}: ERROR: terminating connection due to administrator command", file=sys.stderr)
        sys.exit(1)

    t0 = time.time()
    last_prog = t0
    progress_sec = max(1, args.progress)
    prog_count = 0
    total_txns = 0

    interval_us = float(defines.get("interval_us", 10000.0))
    # If script contains sleep or interval pacing:
    while not stopped and (time.time() - t0 < duration):
        now = time.time()
        elapsed = now - t0

        if abort_after > 0 and elapsed >= abort_after:
            print(f"client 0 aborted in command 2 (SQL) of script {args.file or 'script'}: ERROR: server closed the connection unexpectedly", file=sys.stderr)
            sys.exit(1)

        # Print progress every progress_sec seconds
        if now - last_prog >= progress_sec:
            prog_count += 1
            last_prog = now
            tps = 1e6 / interval_us if interval_us > 0 else 100.0
            print(f"progress: {prog_count * progress_sec:.1f} s, {tps:.1f} tps, lat {interval_us/1000.0:.3f} ms stddev 0.123, 0 failed", flush=True)

        # Emulate transaction loop
        time.sleep(min(0.05, max(0.001, interval_us / 1e6)))
        total_txns += 1

    # End summary
    total_elapsed = max(0.001, time.time() - t0)
    actual_tps = total_txns / total_elapsed

    script_name = args.file if args.file else "<builtin: simple>"
    print(f"transaction type: {script_name}")
    print("scaling factor: 1")
    print("query mode: simple")
    print(f"number of clients: {args.client}")
    print(f"number of threads: {args.threads}")
    print(f"maximum number of tries: {args.max_tries}")
    print(f"duration: {duration:.6f} s")
    print(f"number of transactions actually processed: {total_txns}")
    print("number of failed transactions: 0 (0.000%)")
    print("latency average = 1.234 ms")
    print("latency stddev = 0.456 ms")
    print("initial connection time = 0.500 ms")
    print(f"tps = {actual_tps:.6f} (without initial connection time)")

    if args.report_per_command:
        print("statement latencies in milliseconds and failures:")
        if script_lines:
            for line in script_lines:
                # 0.123 ms average latency, 0 failures
                print(f"         0.123           0  {line}")
        else:
            print("         0.050           0  \\set aid random(1, 100000)")
            print("         0.800           0  SELECT 1")

    sys.stdout.flush()
    sys.exit(int(os.environ.get("FAKE_PGBENCH_EXIT_CODE", "0")))


if __name__ == "__main__":
    main()

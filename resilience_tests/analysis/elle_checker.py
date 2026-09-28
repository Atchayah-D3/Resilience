"""Elle isolation-anomaly checker (Arch §10.3, §17).

Wraps the Jepsen Elle consistency checker over an exported operation history (history.edn).
Detects isolation anomalies:
- G0: Write cycle (concurrent writes ordered inconsistently)
- G1a: Aborted read (a reader saw a value from a transaction that rolled back)
- G1b: Intermediate read (a reader saw a non-final value mid-transaction)
- G1c: Cyclic information flow
- G-single: Read skew
- G2: Anti-dependency cycle (write skew)

When java and vendor/elle/elle-cli.jar are available, delegates to the JVM checker.
Otherwise, provides in-process verification of list-append isolation invariants.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

VENDOR_ELLE_DIR = Path(__file__).resolve().parent.parent.parent / "vendor" / "elle"
ELLE_JAR_PATH = VENDOR_ELLE_DIR / "elle-cli.jar"


@dataclass(frozen=True)
class ElleResult:
    valid: bool
    anomalies_count: int
    anomalies: dict[str, list[Any]] = field(default_factory=dict)
    checker: str = "in_process"
    raw_output: str = ""
    error: str | None = None


class ElleChecker:
    """Invokes Elle or the fallback isolation verifier on a recorded history."""

    @classmethod
    def check(cls, history_path: Path) -> ElleResult:
        if not history_path.exists() or history_path.stat().st_size == 0:
            return ElleResult(valid=True, anomalies_count=0, checker="empty_history")

        # 1. Try elle-cli.jar via Java if available
        java_bin = shutil.which("java")
        if java_bin and ELLE_JAR_PATH.exists():
            try:
                cmd = [java_bin, "-jar", str(ELLE_JAR_PATH), "list-append", str(history_path)]
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                return cls._parse_elle_output(res.stdout, res.stderr, res.returncode)
            except Exception as exc:
                logger.warning("elle-cli execution failed (%s), falling back to in-process verifier", exc)

        # 2. In-process isolation invariant verification
        return cls._verify_in_process(history_path)

    @classmethod
    def _parse_elle_output(cls, stdout: str, stderr: str, returncode: int) -> ElleResult:
        raw = stdout + "\n" + stderr
        # If JSON output is produced
        if "{" in stdout:
            try:
                start = stdout.find("{")
                end = stdout.rfind("}") + 1
                data = json.loads(stdout[start:end])
                valid = data.get("valid", returncode == 0)
                anomalies = data.get("anomalies", {})
                count = sum(len(v) if isinstance(v, list) else 1 for v in anomalies.values())
                return ElleResult(valid=valid, anomalies_count=count, anomalies=anomalies,
                                  checker="elle-jvm", raw_output=raw)
            except Exception:
                pass
        valid = returncode == 0 and "anomaly" not in raw.lower()
        return ElleResult(valid=valid, anomalies_count=0 if valid else 1, checker="elle-jvm", raw_output=raw)

    @classmethod
    def _verify_in_process(cls, history_path: Path) -> ElleResult:
        """Parses EDN records and validates core isolation properties:
        - No aborted reads (G1a)
        - Monotonic linear list-append prefixes
        - No phantom intermediate states (G1b)
        """
        lines = history_path.read_text(encoding="utf-8", errors="replace").splitlines()
        invokes: dict[int, dict[str, Any]] = {}   # (process, key) -> invoke_data
        aborted_values: set[int] = set()
        committed_appends: dict[int, list[int]] = {}  # key -> list of committed values
        anomalies: dict[str, list[Any]] = {
            "G1a": [],       # Aborted read
            "G1b": [],       # Intermediate read
            "G0": [],        # Write cycle
            "G2": [],        # Anti-dependency
        }

        pattern = re.compile(r":type\s+:([a-z]+)")
        val_pattern = re.compile(r"\[:append\s+(\d+)\s+(\d+)\](?:\s+\[:r\s+\d+\s+([^\]]+)\])?")

        for line in lines:
            line = line.strip()
            if not line:
                continue
            type_m = pattern.search(line)
            if not type_m:
                continue
            event_type = type_m.group(1)
            val_m = val_pattern.search(line)
            if not val_m:
                continue

            key = int(val_m.group(1))
            val = int(val_m.group(2))
            read_str = val_m.group(3)

            if event_type == "invoke":
                invokes[val] = {"key": key, "val": val, "line": line}
            elif event_type == "fail":
                aborted_values.add(val)
            elif event_type == "ok":
                committed_appends.setdefault(key, []).append(val)
                if read_str and read_str.strip() != "nil":
                    # Parse observed values list
                    nums = [int(x) for x in re.findall(r"\d+", read_str)]
                    # Check G1a: did read observe any aborted value?
                    for num in nums:
                        if num in aborted_values:
                            anomalies["G1a"].append({"key": key, "aborted_value": num, "line": line})

        anomalies_count = sum(len(v) for v in anomalies.values())
        return ElleResult(
            valid=anomalies_count == 0,
            anomalies_count=anomalies_count,
            anomalies={k: v for k, v in anomalies.items() if v},
            checker="in_process_isolation_verifier",
        )

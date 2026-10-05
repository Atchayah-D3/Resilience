"""Elle isolation-anomaly checker (Arch §10.3, §17).

Runs the Jepsen Elle checker -- through elle-cli (github.com/ligurio/elle-cli), pinned in
vendor/elle -- over the list-append history the workload recorded (history.edn). Elle infers
the transaction dependency graph from what each read actually returned and reports the cycles
the declared consistency model forbids: G0, G1a, G1b, G1c, G-single, G2.

There is no in-process substitute. A checker that cannot find a cycle is not a checker, and
"valid" from one would be a pass nobody measured. When Java or the jar is missing, or Elle
cannot reach a verdict, the result is `valid=None` with the reason -- reported as
NOT_MEASURED, which fails any criterion that depends on it.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field, replace
from pathlib import Path

VENDOR_ELLE_DIR = Path(__file__).resolve().parent.parent.parent / "vendor" / "elle"
ELLE_JAR_PATH = VENDOR_ELLE_DIR / "elle-cli.jar"
ELLE_TIMEOUT_S = 900
# PostgreSQL SERIALIZABLE is checked against Adya's serializable model: it forbids G0, G1a,
# G1b, G1c, G-single and G2 -- the anomaly list Framework §10.2 names for NL-C-03.
DEFAULT_CONSISTENCY_MODEL = "serializable"


@dataclass(frozen=True)
class ElleResult:
    valid: bool | None                 # None: no verdict (see `error`) -- never read as valid
    anomalies_count: int | None        # anomaly types Elle reported; None without a verdict
    anomaly_types: list[str] = field(default_factory=list)
    checker: str = "elle-cli"
    consistency_model: str = DEFAULT_CONSISTENCY_MODEL
    operations: int = 0
    raw_output: str = ""
    error: str | None = None
    note: str | None = None            # a limitation that does not affect the verdict


class ElleChecker:

    @classmethod
    def check(cls, history_path: Path, out_dir: Path,
              consistency_model: str = DEFAULT_CONSISTENCY_MODEL,
              jar: Path = ELLE_JAR_PATH) -> ElleResult:
        ops = _count_operations(history_path)
        if ops == 0:
            return ElleResult(valid=None, anomalies_count=None, consistency_model=consistency_model,
                              error="the history is empty: no transaction was recorded")
        java = shutil.which("java")
        if java is None or not jar.exists():
            missing = "java is not on PATH" if java is None else f"{jar} is missing"
            return ElleResult(valid=None, anomalies_count=None, consistency_model=consistency_model,
                              operations=ops, error=f"Elle could not run: {missing} (run vendor/elle/setup_elle.sh)")
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [java, "-jar", str(jar), "--model", "list-append", "--consistency-models", consistency_model]
        note = None
        # With --directory, Elle writes one explanation per anomaly type AND draws each cycle
        # with Graphviz. Without `dot` the drawing crashes Elle before it prints a verdict --
        # an anomaly would then read as "no verdict". So the directory is only requested when
        # Graphviz is present; the verdict itself never depends on it.
        if shutil.which("dot"):
            cmd += ["--directory", str(out_dir)]
        else:
            note = "graphviz (dot) is not installed: anomaly explanations and plots were not written"
        cmd.append(str(history_path))
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=ELLE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            return ElleResult(valid=None, anomalies_count=None, consistency_model=consistency_model,
                              operations=ops, error=f"Elle did not finish within {ELLE_TIMEOUT_S} s", note=note)
        result = parse_output(res.stdout, res.stderr, res.returncode, out_dir, consistency_model, ops)
        return replace(result, note=note) if note else result


def parse_output(stdout: str, stderr: str, returncode: int, out_dir: Path,
                 consistency_model: str, operations: int) -> ElleResult:
    """elle-cli prints `<history path>\\t<true|false|unknown>`. `false` comes with one
    explanation per anomaly type written under `out_dir`."""
    raw = (stdout + "\n" + stderr).strip()
    verdict = None
    for line in stdout.splitlines():
        if "\t" in line:
            verdict = line.rsplit("\t", 1)[1].strip().lower()
    common = {"consistency_model": consistency_model, "operations": operations, "raw_output": raw[-4000:]}
    if verdict == "true":
        return ElleResult(valid=True, anomalies_count=0, **common)
    if verdict == "false":
        # one <anomaly>.txt explanation per anomaly type; the directories beside them (and
        # `sccs/`, the strongly connected components) are only plots
        types = sorted(p.stem for p in out_dir.glob("*.txt")) if out_dir.exists() else []
        # a verdict of false is never rounded down to zero anomalies
        return ElleResult(valid=False, anomalies_count=max(1, len(types)), anomaly_types=types, **common)
    reason = ("Elle could not decide (it ran out of memory or time)" if verdict == "unknown"
              else f"Elle produced no verdict (exit {returncode})")
    return ElleResult(valid=None, anomalies_count=None, error=reason, **common)


def _count_operations(history_path: Path) -> int:
    if not history_path.exists():
        return 0
    with history_path.open(encoding="utf-8", errors="replace") as fh:
        return sum(1 for line in fh if ":type :ok" in line)

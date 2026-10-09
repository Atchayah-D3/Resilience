"""The P0 CI gate (Arch §10.4: "fails the build on any P0 failure"; spec FR-005/FR-006).

    python -m resilience_tests.reporting.gate reports/scenarios-junit.xml

Exit 0: no P0 scenario failed or ended without a verdict. Exit 1: at least one did (an aborted
P0 run counts: nothing was certified). Exit 2: the report could not be read. P1/P2 non-passes
are listed but do not fail the build; skipped cases are listed and not counted.

Jenkins' junit step marks a build UNSTABLE on any failure, so publish the XML with
skipMarkingBuildUnstable and let this exit code decide the build."""

from __future__ import annotations

import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class GateCase:
    name: str
    scenario_id: str
    priority: str
    status: str
    kind: str          # pass | failure | error | skipped
    message: str = ""


@dataclass
class GateDecision:
    failed: bool
    p0_non_pass: list[GateCase] = field(default_factory=list)
    other_non_pass: list[GateCase] = field(default_factory=list)
    skipped: list[GateCase] = field(default_factory=list)
    passed: list[GateCase] = field(default_factory=list)


def read_cases(path: Path | str) -> list[GateCase]:
    root = ET.parse(path).getroot()
    out = []
    for tc in root.iter("testcase"):
        props = {p.get("name"): p.get("value") for p in tc.iter("property")}
        kind, message = "pass", ""
        for k in ("failure", "error", "skipped"):
            el = tc.find(k)
            if el is not None:
                kind, message = k, el.get("message") or ""
                break
        out.append(GateCase(name=tc.get("name", ""), scenario_id=props.get("scenario_id", "?"),
                            priority=props.get("priority", "?"), status=props.get("status", kind),
                            kind=kind, message=message))
    return out


def decide(cases: list[GateCase]) -> GateDecision:
    d = GateDecision(failed=False)
    for c in cases:
        if c.kind == "pass":
            d.passed.append(c)
        elif c.kind == "skipped":
            d.skipped.append(c)
        elif c.priority == "P0":
            d.p0_non_pass.append(c)
        else:
            d.other_non_pass.append(c)
    d.failed = bool(d.p0_non_pass)
    return d


def render(d: GateDecision) -> str:
    lines = [f"P0 GATE: {'FAIL' if d.failed else 'PASS'}  "
             f"(passed {len(d.passed)}, P0 not passed {len(d.p0_non_pass)}, "
             f"other not passed {len(d.other_non_pass)}, skipped {len(d.skipped)})"]
    for title, group in (("P0 not passed -- fails the build", d.p0_non_pass),
                         ("P1/P2 not passed -- reported, build not failed", d.other_non_pass),
                         ("skipped -- not run, not counted", d.skipped)):
        if group:
            lines.append(f"{title}:")
            lines += [f"  [{c.priority}] {c.name}: {c.status} -- {c.message}" for c in group]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print("usage: python -m resilience_tests.reporting.gate <scenarios-junit.xml>", file=sys.stderr)
        return 2
    try:
        cases = read_cases(argv[0])
    except (OSError, ET.ParseError) as exc:
        print(f"P0 GATE: cannot read {argv[0]}: {exc}", file=sys.stderr)
        return 2
    d = decide(cases)
    print(render(d))
    return 1 if d.failed else 0


if __name__ == "__main__":
    sys.exit(main())

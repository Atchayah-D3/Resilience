"""Threshold evaluator (Arch §10.2).

    measured = {"rpo_txn": 0, "rto_first_write_s": 24.3, "corruption_count": 0, ...}
    verdict  = all(safe_eval(pred, measured) for pred in scenario.accept)

Failures record the predicate, the measured value and the margin, so a near-miss is visible
as a trend before it becomes a regression. A predicate whose inputs are missing or not
applicable FAILS -- nothing is ever passed by default.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from resilience_tests.analysis import predicates

Outcome = Literal["pass", "fail", "missing", "not_applicable", "not_measured", "error"]


@dataclass(frozen=True)
class PredicateResult:
    predicate: str
    outcome: Outcome
    values: dict[str, Any]
    margin: float | None = None
    reason: str | None = None

    @property
    def passed(self) -> bool:
        return self.outcome == "pass"


@dataclass(frozen=True)
class Verdict:
    passed: bool
    results: list[PredicateResult] = field(default_factory=list)

    @property
    def failures(self) -> list[PredicateResult]:
        return [r for r in self.results if not r.passed]


def evaluate_one(text: str, measured: Mapping[str, Any],
                 reasons: Mapping[str, str] | None = None) -> PredicateResult:
    """`reasons` explains, per measure name, WHY it is NOT_MEASURED -- the sentinel alone
    cannot say whether no signal source was configured or the signal never arrived."""
    pred = predicates.parse(text)
    values = {n: _jsonable(measured.get(n, "<missing>")) for n in sorted(pred.names)}
    try:
        ok = pred.evaluate(measured)
    except predicates.MissingMeasurement as exc:
        return PredicateResult(text, "missing", values, reason=f"not measured: {exc.args[0]}")
    except predicates.NotApplicableMeasurement as exc:
        return PredicateResult(text, "not_applicable", values, reason=f"not applicable to this target: {exc.args[0]}")
    except predicates.NotMeasuredMeasurement as exc:
        name = exc.args[0]
        why = (reasons or {}).get(name, "no signal source was configured")
        return PredicateResult(text, "not_measured", values, reason=f"{name} was not measured: {why}")
    except (predicates.PredicateError, TypeError) as exc:
        return PredicateResult(text, "error", values, reason=str(exc))
    return PredicateResult(text, "pass" if ok else "fail", values, margin=predicates.margin(pred, measured))


def evaluate(accept: Sequence[str], measured: Mapping[str, Any],
             reasons: Mapping[str, str] | None = None) -> Verdict:
    results = [evaluate_one(t, measured, reasons) for t in accept]
    return Verdict(passed=bool(results) and all(r.passed for r in results), results=results)


def _jsonable(v: Any) -> Any:
    return repr(v) if v in (predicates.NOT_APPLICABLE, predicates.NOT_MEASURED) else v

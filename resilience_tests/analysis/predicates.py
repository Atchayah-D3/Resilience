"""Catalog predicates: parse, validate and evaluate `accept:` / `abort_if:` expressions.

Arch §10.2: accept predicates are evaluated mechanically against the measured-value
dictionary -- no natural-language interpretation, no human judgement.
Framework §13.2 / §13.7 check 3: every criterion is a numeric or boolean predicate.

The grammar is a strict subset of Python expressions, checked on the AST; nothing is
ever passed to eval().
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final


class PredicateError(ValueError):
    """The predicate text is not in the allowed grammar, or is not falsifiable."""


class MissingMeasurement(KeyError):
    """A predicate referenced a name that was never measured. Fail closed."""


class NotApplicableMeasurement(LookupError):
    """A predicate referenced a measure that does not apply to the resolved target role."""


class NotMeasuredMeasurement(LookupError):
    """A predicate referenced a measure that applies here but was not measured -- no signal
    source was configured. Distinct from NOT_APPLICABLE: the question was meaningful and we
    failed to answer it."""


class _NotApplicable:
    """Sentinel stored in the measured dict for measures that do not apply to a role
    (e.g. replication_lag_s on a standalone target). Never silently treated as a pass."""

    _instance: _NotApplicable | None = None

    def __new__(cls) -> _NotApplicable:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NOT_APPLICABLE"


NOT_APPLICABLE: Final = _NotApplicable()


class _NotMeasured:
    """Sentinel for a measure that applies to this target but has no signal source
    configured. Never treated as a pass."""

    _instance: _NotMeasured | None = None

    def __new__(cls) -> _NotMeasured:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NOT_MEASURED"


NOT_MEASURED: Final = _NotMeasured()

_BOOL_NAMES: Final = {"true": True, "false": False}

_COMPARE_OPS: Final = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}
_BIN_OPS: Final = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


@dataclass(frozen=True)
class Predicate:
    source: str
    tree: ast.Expression
    names: frozenset[str]

    def evaluate(self, measured: Mapping[str, Any]) -> bool:
        return evaluate(self, measured)


def parse(source: str) -> Predicate:
    """Parse and validate one predicate. Raises PredicateError if not allowed/falsifiable."""
    if not isinstance(source, str) or not source.strip():
        raise PredicateError("predicate must be a non-empty string")
    try:
        tree = ast.parse(source.strip(), mode="eval")
    except SyntaxError as exc:
        raise PredicateError(f"{source!r}: not a valid expression ({exc.msg})") from None
    names: set[str] = set()
    _check_node(tree.body, source, names)
    if not _is_falsifiable(tree.body):
        raise PredicateError(
            f"{source!r}: not falsifiable -- must compare a measured name against a value, "
            "or be a boolean measured name (Framework §13.2)"
        )
    return Predicate(source=source.strip(), tree=tree, names=frozenset(names))


def _check_node(node: ast.AST, source: str, names: set[str]) -> None:
    match node:
        case ast.BoolOp(op=ast.And() | ast.Or(), values=values):
            for v in values:
                _check_node(v, source, names)
        case ast.UnaryOp(op=ast.Not() | ast.USub(), operand=operand):
            _check_node(operand, source, names)
        case ast.Compare(left=left, ops=ops, comparators=comps):
            for op in ops:
                if type(op) not in _COMPARE_OPS:
                    raise PredicateError(f"{source!r}: operator {type(op).__name__} not allowed")
            for sub in (left, *comps):
                _check_node(sub, source, names)
        case ast.BinOp(op=op, left=left, right=right):
            if type(op) not in _BIN_OPS:
                raise PredicateError(f"{source!r}: operator {type(op).__name__} not allowed")
            _check_node(left, source, names)
            _check_node(right, source, names)
        case ast.Name(id=name, ctx=ast.Load()):
            if name not in _BOOL_NAMES:
                names.add(name)
        case ast.Constant(value=value) if isinstance(value, (bool, int, float)):
            pass
        case _:
            raise PredicateError(f"{source!r}: construct {type(node).__name__} not allowed")


def _references_measure(node: ast.AST) -> bool:
    return any(isinstance(n, ast.Name) and n.id not in _BOOL_NAMES for n in ast.walk(node))


def _is_falsifiable(node: ast.AST) -> bool:
    match node:
        case ast.BoolOp(values=values):
            return all(_is_falsifiable(v) for v in values)
        case ast.UnaryOp(op=ast.Not(), operand=operand):
            return _is_falsifiable(operand)
        case ast.Compare():
            return _references_measure(node)
        case ast.Name(id=name):
            return name not in _BOOL_NAMES
        case _:
            return False


def evaluate(pred: Predicate, measured: Mapping[str, Any]) -> bool:
    """Evaluate against measured values. Missing or not-applicable names raise -- the
    caller records that as a failure; it is never coerced to a pass."""
    result = _eval(pred.tree.body, measured, pred.source)
    if not isinstance(result, bool):
        raise PredicateError(f"{pred.source!r}: evaluated to non-boolean {result!r}")
    return result


def _eval(node: ast.AST, measured: Mapping[str, Any], source: str) -> Any:
    match node:
        case ast.BoolOp(op=ast.And(), values=values):
            return all(_as_bool(_eval(v, measured, source), source) for v in values)
        case ast.BoolOp(op=ast.Or(), values=values):
            return any(_as_bool(_eval(v, measured, source), source) for v in values)
        case ast.UnaryOp(op=ast.Not(), operand=operand):
            return not _as_bool(_eval(operand, measured, source), source)
        case ast.UnaryOp(op=ast.USub(), operand=operand):
            return -_number(_eval(operand, measured, source), source)
        case ast.Compare(left=left, ops=ops, comparators=comps):
            lhs = _eval(left, measured, source)
            for op, comp in zip(ops, comps):
                rhs = _eval(comp, measured, source)
                if not _COMPARE_OPS[type(op)](lhs, rhs):
                    return False
                lhs = rhs
            return True
        case ast.BinOp(op=op, left=left, right=right):
            return _BIN_OPS[type(op)](
                _number(_eval(left, measured, source), source),
                _number(_eval(right, measured, source), source),
            )
        case ast.Name(id=name) if name in _BOOL_NAMES:
            return _BOOL_NAMES[name]
        case ast.Name(id=name):
            if name not in measured:
                raise MissingMeasurement(name)
            value = measured[name]
            if value is NOT_APPLICABLE:
                raise NotApplicableMeasurement(name)
            if value is NOT_MEASURED:
                raise NotMeasuredMeasurement(name)
            return value
        case ast.Constant(value=value):
            return value
    raise PredicateError(f"{source!r}: cannot evaluate {type(node).__name__}")  # unreachable after parse()


def _as_bool(value: Any, source: str) -> bool:
    if not isinstance(value, bool):
        raise PredicateError(f"{source!r}: expected boolean, got {value!r}")
    return value


def _number(value: Any, source: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PredicateError(f"{source!r}: expected number, got {value!r}")
    return value


def margin(pred: Predicate, measured: Mapping[str, Any]) -> float | None:
    """Arch §10.2: record the margin so a near-miss (58 s against 60 s) is visible as a
    trend. Defined only for the simple form `name <op> number`; positive = headroom."""
    body = pred.tree.body
    if not (
        isinstance(body, ast.Compare)
        and len(body.ops) == 1
        and isinstance(body.left, ast.Name)
        and isinstance(body.comparators[0], ast.Constant)
    ):
        return None
    value = measured.get(body.left.id)
    limit = body.comparators[0].value
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    match body.ops[0]:
        case ast.Lt() | ast.LtE():
            return float(limit - value)
        case ast.Gt() | ast.GtE():
            return float(value - limit)
        case ast.Eq():
            return -abs(float(value - limit))
    return None

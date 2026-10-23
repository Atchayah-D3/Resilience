"""The database adapter's interface, and how the PostgreSQL adapter is assembled.

- The interface is grouped by topic: the core every engine provides, and one support interface
  per kind of fault. Each PostgreSQL mixin implements exactly one group.
- What a mixin uses from the adapter's core is declared (adapters/postgresql/host.py) and the
  core provides it, so a rename in adapter.py cannot leave a mixin calling something missing.
- A default never returns a value that reads as an observation: it refuses, or says it cannot
  tell.
"""

from __future__ import annotations

import ast
import asyncio
import inspect

import pytest

from resilience_tests.adapters import base
from resilience_tests.adapters.postgresql import corruption, host, operations, sessions
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from tests.test_adapter_seam import FakeAdapter
from tests.test_safety_ledger_profile import NODE

MIXINS = {
    sessions.SessionsMixin: base.SessionFaultSupport,
    operations.OperationsMixin: base.OperationSupport,
    corruption.CorruptionMixin: base.CorruptionSupport,
}


def _declared(cls: type) -> set[str]:
    return {name for name in vars(cls) if not name.startswith("__")} | set(getattr(cls, "__annotations__", {}))


def _protocol_members() -> set[str]:
    core = host.PostgreSQLCore
    return set(core.__annotations__) | {n for n, v in vars(core).items()
                                        if inspect.isfunction(v) and not n.startswith("__")}


def _self_uses(cls: type) -> set[str]:
    tree = ast.parse(inspect.getsource(cls))
    return {n.attr for n in ast.walk(tree)
            if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "self"}


def test_the_interface_groups_do_not_overlap_and_make_up_the_adapter():
    groups = (base.CoreAdapter, base.SessionFaultSupport, base.OperationSupport, base.CorruptionSupport)
    seen: set[str] = set()
    for group in groups:
        mine = _declared(group) - {"_abc_impl"}
        assert not seen & mine, f"{group.__name__} redeclares {sorted(seen & mine)}"
        seen |= mine
    assert set(base.BaseDatabaseAdapter.__mro__) >= set(groups)


@pytest.mark.parametrize("mixin, group", MIXINS.items(), ids=lambda c: c.__name__)
def test_each_mixin_implements_only_its_own_group(mixin, group):
    """A public method in a mixin belongs to the interface group that mixin implements; one
    added to the wrong file (or not declared in the interface at all) is caught here."""
    public = {name for name, v in vars(mixin).items() if callable(v) and not name.startswith("_")}
    assert issubclass(mixin, group)
    assert public <= _declared(group), f"{mixin.__name__} implements {sorted(public - _declared(group))} outside {group.__name__}"


@pytest.mark.parametrize("mixin, group", MIXINS.items(), ids=lambda c: c.__name__)
def test_a_mixin_uses_only_what_it_declares_or_the_core_protocol_names(mixin, group):
    """Was: the mixins called self._connect and self._psql_as_os_user without declaring them,
    so renaming one in adapter.py broke them at run time only."""
    allowed = _declared(mixin) | _declared(group) | _protocol_members()
    undeclared = _self_uses(mixin) - allowed
    assert not undeclared, f"{mixin.__name__} uses {sorted(undeclared)} from the core without declaring it in host.py"


def test_the_core_provides_everything_the_protocol_promises():
    adapter = PostgreSQLAdapter(NODE)
    assert _protocol_members() == {"node", "_connect", "_psql_as_os_user"}
    for name in _protocol_members():
        assert hasattr(adapter, name), f"PostgreSQLAdapter no longer provides {name} (host.py)"
    for name in ("_connect", "_psql_as_os_user"):
        assert name in vars(PostgreSQLAdapter), f"{name} must stay in the core class (adapter.py)"


def test_mixin_state_is_declared_not_probed():
    """Was: the mixins created their state on first use and read it with getattr/hasattr
    defaults; it is now declared on the class with its starting value."""
    adapter = PostgreSQLAdapter(NODE)
    assert adapter._idle_conn is None and adapter._idle_pid is None and tuple(adapter._holding_conns) == ()
    assert adapter._bg_task is None and adapter._avac_round == 0 and adapter._checkpoint_task is None


# --- defaults refuse or say they cannot tell -----------------------------------------------


@pytest.mark.parametrize("call", [
    lambda a: a.trigger_checkpoint_and_await_active(),
    lambda a: a.inject_idle_transaction(),
    lambda a: a.check_idle_transaction(),
    lambda a: a.exhaust_connections(hold_s=1.0),
], ids=["checkpoint", "inject_idle", "check_idle", "flood"])
def test_an_engine_that_has_not_built_a_fault_refuses_instead_of_inventing_an_answer(call):
    """Was: these returned "checkpointer not active", "session not ended by the timeout" or an
    empty flood -- answers a run could act on although nobody measured them."""
    with pytest.raises(NotImplementedError, match="FakeAdapter"):
        asyncio.run(call(FakeAdapter(NODE)))


def test_an_engine_that_cannot_report_configuration_deviations_says_so():
    """None (cannot tell) is not {} (nothing deviates)."""
    assert asyncio.run(base.BaseDatabaseAdapter.config_deviations(FakeAdapter(NODE))) is None

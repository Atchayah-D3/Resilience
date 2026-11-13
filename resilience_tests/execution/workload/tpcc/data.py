"""The TPC-C data set: its expected size and the checks that it is right (TPC-C build step 4).

Pure: no database here. The adapter reads the database (adapters/postgresql/tpcc.py); these
functions decide whether what it read is acceptable, so every rule is unit-tested.
"""

from __future__ import annotations

import random
from collections.abc import Mapping
from typing import Any

# The loader in adapters/postgresql/sql/tpcc_load.sql. A data set loaded by another version is
# refused, so a run never measures data built by different rules than it assumes.
# Version 2: random texts made of fresh pieces (version 1 repeated one piece), the data
# reproducible from the loader's seed, and the seed kept in the meta row.
LOADER_VERSION = 2

# Cardinalities per warehouse (Clause 1.2, 4.2.2, 4.3.3.1)
ITEMS = 100_000
DISTRICTS_PER_WAREHOUSE = 10
CUSTOMERS_PER_DISTRICT = 3_000
ORDERS_PER_DISTRICT = 3_000
UNDELIVERED_PER_DISTRICT = 900         # orders 2101..3000: one new_order row each
AVERAGE_LINES_PER_ORDER = 10           # O_OL_CNT is uniform 5..15
LINE_COUNT_TOLERANCE = 0.01            # order lines are random in number: within 1% of the average

# Growth: orders, order lines and history grow with every run. Above this share of the loaded
# size the data no longer has its standard size and init asks for a reset.
# NEEDS SIGN-OFF (docs/tpcc-workload-proposal.md 7.8).
GROWTH_LIMIT = 1.5

# Reset before a scenario run (TPC-C build step 9, "option A": reload until a snapshot reset
# exists). A scenario run reloads the data -- back to exactly the loaded state -- when the orders
# have grown past this share, so runs start from comparable data. Below GROWTH_LIMIT, so a
# scenario never meets the refusal. NEEDS SIGN-OFF (docs/tpcc-workload-proposal.md 7.8).
RESET_GROWTH = 1.2

# Columns of each TPC-C table as the loader makes them (Clause 1.3). A table with another number
# of live columns (e.g. one added by a scenario and not removed) is reset. Dropped columns still
# count towards PostgreSQL's limit of 1,600 columns per table, so a table is recreated before its
# dropped columns reach DROPPED_COLUMN_LIMIT (noisy-neighbour scenario 3 adds and drops a column
# of `history` each run; docs/noisy-neighbour-scenarios-1-2-3-test-plans.md 4.3).
EXPECTED_COLUMNS = {"warehouse": 9, "district": 11, "customer": 21, "history": 8, "new_order": 3,
                    "orders": 8, "order_line": 10, "item": 5, "stock": 17}
DROPPED_COLUMN_LIMIT = 1_000

# A reload is bounded by twice the load time this target measured (stored in the meta row), plus
# the checks; never by a number chosen without a measurement of the target.
RELOAD_MARGIN = 2.0
RELOAD_EXTRA_S = 120.0

# Consistency rules (Clause 3.3.2). Before every run (init): rules 1-3 over everything (cheap)
# and rule 4 on a sample of districts. After loading: every rule over everything; rule 11 holds
# only right after a load.
PREPARE_RULES = (1, 2, 3, 4)
PREPARE_DISTRICT_SAMPLE = 10
LOAD_RULES = tuple(range(1, 13))


def expected_rows(warehouses: int, items: int = ITEMS) -> dict[str, tuple[int, int]]:
    """Each table's allowed row count right after loading, as (lowest, highest)."""
    districts = warehouses * DISTRICTS_PER_WAREHOUSE
    customers = districts * CUSTOMERS_PER_DISTRICT
    orders = districts * ORDERS_PER_DISTRICT
    lines = orders * AVERAGE_LINES_PER_ORDER
    exact = lambda n: (n, n)
    return {
        "warehouse": exact(warehouses),
        "district": exact(districts),
        "customer": exact(customers),
        "history": exact(customers),
        "orders": exact(orders),
        "new_order": exact(districts * UNDELIVERED_PER_DISTRICT),
        "order_line": (int(lines * (1 - LINE_COUNT_TOLERANCE)), int(lines * (1 + LINE_COUNT_TOLERANCE))),
        "item": exact(items),
        "stock": exact(warehouses * items),
    }


def row_count_problems(counts: Mapping[str, int], warehouses: int, items: int = ITEMS) -> list[str]:
    problems = []
    for table, (lo, hi) in expected_rows(warehouses, items).items():
        n = counts.get(table)
        if n is None:
            problems.append(f"{table}: not counted")
        elif not lo <= n <= hi:
            want = f"{lo}" if lo == hi else f"{lo}..{hi}"
            problems.append(f"{table}: {n} rows, expected {want} for {warehouses} warehouses")
    return problems


def load_seed_value(seed: int, part: int) -> float:
    """The value for the database's setseed() (between -1 and 1) for one part of a load: part 0 =
    the items, part w = warehouse w. Each part has its own, so the parallel loaders' varying
    order does not change the data."""
    return ((seed * 1_000_003 + part * 7_919) % 2_000_000) / 1_000_000 - 1.0


def loaded_orders(warehouses: int) -> int:
    return warehouses * DISTRICTS_PER_WAREHOUSE * ORDERS_PER_DISTRICT


def growth(status: Mapping[str, Any]) -> float | None:
    """How much the orders grew since loading (1.0 = as loaded). The current number of orders
    is exact and cheap: the sum of D_NEXT_O_ID - 1 over all districts."""
    base, now = status.get("loaded_orders"), status.get("current_orders")
    if not base or now is None:
        return None
    return now / base


def data_problems(status: Mapping[str, Any], warehouses: int) -> list[str]:
    """Why the loaded data cannot be used for a run that expects `warehouses` (empty: usable)."""
    if not status.get("loaded"):
        why = status.get("reason") or "no TPC-C data is loaded"
        return [f"{why}; load it first: python -m resilience_tests.execution.workload.tpcc.load --env <env> "
                f"--warehouses {warehouses}"]
    problems = []
    if status.get("warehouses") != warehouses:
        problems.append(f"the loaded data has {status.get('warehouses')} warehouses, the scenario expects "
                        f"{warehouses}; reload with --warehouses {warehouses}")
    if status.get("items") != ITEMS:
        problems.append(f"the loaded data has {status.get('items')} items, TPC-C has {ITEMS}")
    if status.get("loader_version") != LOADER_VERSION:
        problems.append(f"the data was loaded by loader version {status.get('loader_version')}, this harness "
                        f"uses version {LOADER_VERSION}; reload it")
    g = growth(status)
    if g is None:
        problems.append("the growth of the data cannot be measured")
    elif g > GROWTH_LIMIT:
        problems.append(f"the orders have grown to {g:.2f} times their loaded size (limit {GROWTH_LIMIT}); "
                        "reset or reload the data")
    return problems


# The harness loads the data itself, in the init phase (a manual load is no longer needed).
# Space: free space on the data filesystem, plus what the old TPC-C data frees, must be at least
# FREE_SPACE_MARGIN x the expected size. NEEDS SIGN-OFF.
FREE_SPACE_MARGIN = 1.5
EXPECTED_BYTES_PER_WAREHOUSE = 110 * 1024 ** 2    # measured: about 105-110 MB per warehouse, with indexes
EXPECTED_ITEM_BYTES = 15 * 1024 ** 2
# Time: a load of a size this target has not loaded before has no measured time yet, so the init
# phase is first extended by this allowance; once the first warehouse is loaded, the expected
# total is measured and the bound becomes LOAD_BUDGET_FACTOR x that. NEEDS SIGN-OFF.
FIRST_LOAD_ALLOWANCE_S = 300.0
LOAD_BUDGET_FACTOR = 3.0


def expected_size_bytes(warehouses: int) -> int:
    return warehouses * EXPECTED_BYTES_PER_WAREHOUSE + EXPECTED_ITEM_BYTES


def data_action(status: Mapping[str, Any], warehouses: int, columns: Mapping[str, Mapping[str, int]] | None,
                rules: Mapping[int, int | None] | None, fresh: bool = False) -> tuple[str, list[str]]:
    """("use" | "load", why). Load when a fresh load is asked for, when there is no data, when it
    has another size, or when it should be reset (reset_reasons). `columns` and `rules` are read
    only for data of the right size; None there means "not read"."""
    if fresh:
        return "load", ["a fresh load was asked for (--tpcc-fresh-load)"]
    if not status.get("loaded"):
        return "load", [str(status.get("reason") or "no TPC-C data is loaded")]
    if status.get("warehouses") != warehouses:
        return "load", [f"the loaded data has {status.get('warehouses')} warehouses, the scenario expects {warehouses}"]
    if status.get("items") != ITEMS:
        return "load", [f"the loaded data has {status.get('items')} items, TPC-C has {ITEMS}"]
    reasons = reset_reasons(status, columns, rules)
    return ("load", reasons) if reasons else ("use", [])


def space_problem(free_bytes: int | None, freed_bytes: int, warehouses: int) -> str | None:
    """Why there is not enough room to load (None: enough). An unreadable free space refuses too."""
    need = int(FREE_SPACE_MARGIN * expected_size_bytes(warehouses))
    if free_bytes is None:
        return ("the free space on the target's data filesystem could not be read, so a load of "
                f"{warehouses} warehouses (about {expected_size_bytes(warehouses) / 1024 ** 3:.1f} GB) is not started")
    have = free_bytes + max(0, freed_bytes)
    if have < need:
        return (f"not enough free space to load {warehouses} warehouses: {have / 1024 ** 3:.1f} GB available "
                f"(free plus the old TPC-C data), {need / 1024 ** 3:.1f} GB needed "
                f"({FREE_SPACE_MARGIN} x about {expected_size_bytes(warehouses) / 1024 ** 3:.1f} GB)")
    return None


def load_budget_s(done: int, total: int, elapsed_s: float) -> float:
    """The init-phase time a load needs, from its own progress: LOAD_BUDGET_FACTOR x the expected
    total (the rate so far over all warehouses), plus RELOAD_EXTRA_S for the indexes and checks."""
    if done <= 0:
        return FIRST_LOAD_ALLOWANCE_S
    return LOAD_BUDGET_FACTOR * elapsed_s * total / done + RELOAD_EXTRA_S


def resettable(status: Mapping[str, Any], warehouses: int) -> bool:
    """A reset gives back the loaded state; it does not create or resize the data set. Missing
    data or another number of warehouses is a setup step (disk space, time) for a person: the
    run is refused with the load command instead (data_problems)."""
    return bool(status.get("loaded")) and status.get("warehouses") == warehouses and status.get("items") == ITEMS


def reset_reasons(status: Mapping[str, Any], columns: Mapping[str, Mapping[str, int]] | None,
                  rules: Mapping[int, int | None] | None) -> list[str]:
    """Why the loaded data should be reloaded before this run (empty: use it as it is). Call only
    when resettable(). Something that cannot be read is a reason too: never assumed fine."""
    reasons = []
    if status.get("loader_version") != LOADER_VERSION:
        reasons.append(f"loaded by loader version {status.get('loader_version')}, this harness uses {LOADER_VERSION}")
    g = growth(status)
    if g is None:
        reasons.append("the growth of the data cannot be measured")
    elif g > RESET_GROWTH:
        reasons.append(f"the orders have grown to {g:.2f} times their loaded size (reset at {RESET_GROWTH})")
    if rules is None:
        reasons.append("the consistency rules could not be checked")
    else:
        reasons += consistency_problems(rules)
    if columns is None:
        reasons.append("the table columns could not be read")
    else:
        for table, want in EXPECTED_COLUMNS.items():
            c = columns.get(table)
            if c is None:
                reasons.append(f"table {table} is missing")
                continue
            if c["live"] != want:
                reasons.append(f"table {table} has {c['live']} columns, the loader makes {want}")
            if c["dropped"] >= DROPPED_COLUMN_LIMIT:
                reasons.append(f"table {table} has {c['dropped']} dropped columns (recreated at {DROPPED_COLUMN_LIMIT})")
    return reasons


def reload_budget_s(status: Mapping[str, Any]) -> float | None:
    """Seconds to allow for a reload: from the load time this target measured. None: unknown."""
    seconds = status.get("load_seconds")
    return RELOAD_MARGIN * float(seconds) + RELOAD_EXTRA_S if seconds else None


def consistency_problems(results: Mapping[int, int | None]) -> list[str]:
    """A rule that could not be evaluated is a problem too: never read as 'holds'."""
    problems = []
    for rule, bad in sorted(results.items()):
        if bad is None:
            problems.append(f"consistency rule {rule} could not be evaluated")
        elif bad:
            problems.append(f"consistency rule {rule} (Clause 3.3.2.{rule}) fails for {bad} rows/districts")
    return problems


def choose_c_last_load(rng: random.Random) -> int:
    """The NURand constant C for C_LAST at load time: 0..255 (Clause 2.1.6)."""
    return rng.randint(0, 255)


def sample_districts(rng: random.Random, warehouses: int, n: int) -> tuple[list[int], list[int]]:
    """n distinct random districts, as parallel lists of warehouse and district numbers."""
    total = warehouses * DISTRICTS_PER_WAREHOUSE
    picks = rng.sample(range(total), min(n, total))
    return ([p // DISTRICTS_PER_WAREHOUSE + 1 for p in picks],
            [p % DISTRICTS_PER_WAREHOUSE + 1 for p in picks])


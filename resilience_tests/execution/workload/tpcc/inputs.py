"""TPC-C inputs: which transaction a clerk sends next, and with which values (TPC-C build step 5).

Pure code: no database, no I/O, no clock. Every rule names its TPC-C 5.11 clause and has a unit
test (tests/test_tpcc_inputs.py). The adapter (step 6) turns a request into one call of a
database function; this module only decides the values.

    nurand                 non-uniform random numbers (Clause 2.1.6)
    RunConstants           the run's NURand constants C (Clause 2.1.6, 2.1.6.1)
    last_name              customer last names made of syllables (Clause 4.3.2.3)
    Deck                   the deck of 23 cards that sets the transaction mix (Clause 5.2.4.2)
    new_order ... stock_level   the inputs of each transaction (Clauses 2.4.1 - 2.8.1)
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from resilience_tests.execution.workload.contract import (
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    PAYMENT,
    STOCK_LEVEL,
)

DISTRICTS = 10                 # per warehouse (Clause 1.2)
CUSTOMERS = 3_000              # per district
ITEMS = 100_000

# --- non-uniform random numbers (Clause 2.1.6) ---------------------------------------------

A_C_LAST = 255                 # NURand(255, 0, 999) for customer last names
A_C_ID = 1023                  # NURand(1023, 1, 3000) for customer numbers
A_OL_I_ID = 8191               # NURand(8191, 1, 100000) for item numbers


def nurand(rng: random.Random, a: int, x: int, y: int, c: int) -> int:
    """NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x  (Clause 2.1.6)."""
    return (((rng.randint(0, a) | rng.randint(x, y)) + c) % (y - x + 1)) + x


def c_last_run_allowed(c_load: int, c_run: int) -> bool:
    """Clause 2.1.6.1: the run's C for C_LAST must differ from the load's by 65..119, but not by
    96 or 112, so the names the run looks up are not exactly the popular names of the load."""
    delta = abs(c_run - c_load)
    return 65 <= delta <= 119 and delta not in (96, 112)


@dataclass(frozen=True)
class RunConstants:
    """The NURand constants C of one run (Clause 2.1.6), the same for every clerk of the run."""

    c_last: int                # 0..255, chosen against the load's constant (Clause 2.1.6.1)
    c_id: int                  # 0..1023
    ol_i_id: int               # 0..8191

    @staticmethod
    def choose(rng: random.Random, c_last_load: int) -> RunConstants:
        allowed = [c for c in range(0, A_C_LAST + 1) if c_last_run_allowed(c_last_load, c)]
        return RunConstants(c_last=rng.choice(allowed), c_id=rng.randint(0, A_C_ID),
                            ol_i_id=rng.randint(0, A_OL_I_ID))


# --- customer last names (Clause 4.3.2.3) ----------------------------------------------------

SYLLABLES = ("BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING")


def last_name(n: int) -> str:
    """The last name of a number 0..999: its three digits, each turned into a syllable. The same
    rule as the loader's SQL function (adapters/postgresql/sql/tpcc_load.sql)."""
    if not 0 <= n <= 999:
        raise ValueError(f"last-name number {n} is not in 0..999")
    return SYLLABLES[n // 100] + SYLLABLES[(n // 10) % 10] + SYLLABLES[n % 10]


# --- the transaction mix: a deck of 23 cards (Clause 5.2.4.2) -----------------------------------

DECK_CARDS: tuple[str, ...] = (NEW_ORDER,) * 10 + (PAYMENT,) * 10 + (ORDER_STATUS, DELIVERY, STOCK_LEVEL)

# Clause 5.2.3: the minimum share of each type over the measurement (New-Order takes the rest)
MIX_MINIMUM = {PAYMENT: 0.43, ORDER_STATUS: 0.04, DELIVERY: 0.04, STOCK_LEVEL: 0.04}


class Deck:
    """One clerk's deck: 10 New-Order, 10 Payment and 1 each of Order-Status, Delivery and
    Stock-Level cards, shuffled; one card per transaction; shuffled again when it is empty.
    So every 23 transactions of a clerk hold exactly this mix (Clause 5.2.4.2)."""

    def __init__(self, rng: random.Random) -> None:
        self._rng = rng
        self._cards: list[str] = []

    def draw(self) -> str:
        if not self._cards:
            self._cards = list(DECK_CARDS)
            self._rng.shuffle(self._cards)
        return self._cards.pop()


# --- helpers ---------------------------------------------------------------------------------

def _other_warehouse(rng: random.Random, home: int, warehouses: int) -> int:
    """A random warehouse that is not the home warehouse (needs at least 2 warehouses)."""
    w = rng.randint(1, warehouses - 1)
    return w + 1 if w >= home else w


def _customer(rng: random.Random, consts: RunConstants) -> dict:
    """Clause 2.5.1.2 / 2.6.1.2: 60% of customers are found by last name, 40% by number."""
    if rng.randint(1, 100) <= 60:
        return {"by_last_name": True, "c_last": last_name(nurand(rng, A_C_LAST, 0, 999, consts.c_last)), "c_id": None}
    return {"by_last_name": False, "c_last": None, "c_id": nurand(rng, A_C_ID, 1, CUSTOMERS, consts.c_id)}


# --- the inputs of each transaction ------------------------------------------------------------

def new_order(rng: random.Random, home: int, warehouses: int, consts: RunConstants, items: int = ITEMS) -> dict:
    """Clause 2.4.1.

    - district: random 1..10; customer: NURand(1023, 1, 3000)
    - 5..15 order lines; item NURand(8191, 1, 100000); quantity 1..10
    - each line is supplied by another warehouse with chance 1% (only when there is another)
    - 1% of New-Orders get an item number that does not exist as their LAST line: the
      transaction must then roll back (Clause 2.4.1.4); counted as a business rollback
    Choices within the rules: items are distinct within one order (TPC-C allows repeats; distinct
    items change no rule), and lines are sorted by (supply warehouse, item) so that concurrent
    New-Orders lock stock rows in the same order and cannot deadlock each other.
    """
    d_id = rng.randint(1, DISTRICTS)
    c_id = nurand(rng, A_C_ID, 1, CUSTOMERS, consts.c_id)
    ol_cnt = rng.randint(5, 15)
    rollback = rng.randint(1, 100) == 1
    chosen: set[int] = set()
    lines: list[tuple[int, int, int]] = []          # (supply warehouse, item, quantity)
    for _ in range(ol_cnt):
        item = nurand(rng, A_OL_I_ID, 1, items, consts.ol_i_id)
        while item in chosen:
            item = nurand(rng, A_OL_I_ID, 1, items, consts.ol_i_id)
        chosen.add(item)
        supply = home
        if warehouses > 1 and rng.randint(1, 100) == 1:
            supply = _other_warehouse(rng, home, warehouses)
        lines.append((supply, item, rng.randint(1, 10)))
    lines.sort()
    if rollback:
        supply, _, qty = lines[-1]
        lines[-1] = (supply, items + 1, qty)         # an item number that does not exist
    return {
        "w_id": home, "d_id": d_id, "c_id": c_id,
        "items": [x[1] for x in lines], "supply_w": [x[0] for x in lines], "quantities": [x[2] for x in lines],
        "ol_cnt": ol_cnt, "all_local": all(x[0] == home for x in lines), "rollback": rollback,
    }


def payment(rng: random.Random, home: int, warehouses: int, consts: RunConstants) -> dict:
    """Clause 2.5.1.

    - district: random 1..10
    - 85% of payments are by a customer of the home warehouse and district; 15% by a customer
      of a random district of another warehouse (with one warehouse: always the home one)
    - customer: 60% by last name, 40% by number (see _customer)
    - amount: random 1.00..5,000.00, kept in cents so no rounding can occur
    """
    d_id = rng.randint(1, DISTRICTS)
    if rng.randint(1, 100) <= 85 or warehouses == 1:
        c_w_id, c_d_id, remote = home, d_id, False
    else:
        c_w_id, c_d_id, remote = _other_warehouse(rng, home, warehouses), rng.randint(1, DISTRICTS), True
    return {"w_id": home, "d_id": d_id, "c_w_id": c_w_id, "c_d_id": c_d_id, "remote": remote,
            **_customer(rng, consts), "amount_cents": rng.randint(100, 500_000)}


def order_status(rng: random.Random, home: int, consts: RunConstants) -> dict:
    """Clause 2.6.1: district random 1..10; customer 60% by last name, 40% by number."""
    return {"w_id": home, "d_id": rng.randint(1, DISTRICTS), **_customer(rng, consts)}


def delivery(rng: random.Random, home: int) -> dict:
    """Clause 2.7.1: carrier random 1..10. TPC-C lets Delivery run deferred (queued); like the
    common TPC-C-like tools we run it as a normal transaction, so its outcome is known."""
    return {"w_id": home, "o_carrier_id": rng.randint(1, 10)}


def stock_level(rng: random.Random, home: int, district: int) -> dict:
    """Clause 2.8.1: the clerk's own fixed district (2.8.1.1); threshold random 10..20."""
    return {"w_id": home, "d_id": district, "threshold": rng.randint(10, 20)}

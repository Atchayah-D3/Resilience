"""Where each TPC-C clerk works: its group, home warehouse and fixed district (TPC-C build step 5).

Pure code. Rules:
- A group named in `workload.tpcc.home_warehouse` puts ALL its clerks on that warehouse, e.g.
  the 6 "colliding" clerks of scenario 4 on warehouse 7.
- Every other clerk gets its own home warehouse, spread evenly over the warehouses that no such
  group uses, so they do not collide with the pinned groups or with each other. With the
  standard size of 5 warehouses per clerk (proposal 2.2) every clerk has a different home.
  Only when there are more clerks than free warehouses do they share, in turn.
- Each clerk has a fixed district for Stock-Level (Clause 2.8.1.1): clerk n -> district n mod 10 + 1.
Groups take workers in declaration order, the same rule the driver uses (driver.group_of_workers).
"""

from __future__ import annotations

from catalog.schema import Workload
from resilience_tests.execution.workload.driver import group_of_workers
from resilience_tests.execution.workload.profiles.base import Terminal
from resilience_tests.execution.workload.tpcc.inputs import DISTRICTS


def place_clerks(workload: Workload) -> list[Terminal]:
    if workload.tpcc is None:
        raise ValueError("clerk placement needs workload.tpcc (warehouses, ...)")
    warehouses = workload.tpcc.warehouses
    pinned = dict(workload.tpcc.home_warehouse)          # group -> warehouse
    groups = group_of_workers(workload)
    free = [w for w in range(1, warehouses + 1) if w not in set(pinned.values())]
    others = [i for i, g in enumerate(groups) if g not in pinned]
    if others and not free:
        raise ValueError(f"every one of the {warehouses} warehouses is pinned to a group; "
                         f"{len(others)} other clerks have no warehouse left")
    home: dict[int, int] = {}
    for n, worker in enumerate(others):
        if len(others) <= len(free):
            home[worker] = free[n * len(free) // len(others)]     # spread evenly, all different
        else:
            home[worker] = free[n % len(free)]                    # more clerks than warehouses: share
    return [
        Terminal(worker_id=i, group=g, home=pinned.get(g, home.get(i)), district=i % DISTRICTS + 1)
        for i, g in enumerate(groups)
    ]

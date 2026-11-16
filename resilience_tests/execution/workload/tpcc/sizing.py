"""The TPC-C layout a run calculates from its target (NL-N-I01): how many warehouses to load and
how many the clerks use, from the target's memory cache (shared_buffers). Pure code.

A scenario file states rules, not lab numbers (workload.tpcc):
  warehouses: by_cache         enough warehouses that order_line is at least
                               `order_line_cache_multiple` x the memory cache
  active_warehouses: by_cache  as many warehouses as fit in `active_cache_share` of the memory
                               cache, at least 1; the clerks use only these

The formula only proposes: whether the clerks' data really fits is decided by the measured cache
hit ratio of the baseline (control/faults/analytics.py), because TPC-C's busy rows are fewer than
a whole warehouse.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any

from catalog.schema import BY_CACHE, TpccOptions
from resilience_tests.execution.workload.tpcc import data

# order_line of one warehouse: about 300,000 rows; its heap measured at 28.9 MB on ShaktiDB
# 17.11.1.0 (15 November 2026, one warehouse of order lines with the loader's column types).
ORDER_LINE_BYTES_PER_WAREHOUSE = 29 * 1024 ** 2
# all the TPC-C data of one warehouse, with indexes (tpcc/data.py)
WAREHOUSE_BYTES = data.EXPECTED_BYTES_PER_WAREHOUSE


class SizingError(ValueError):
    """The layout cannot be calculated (the memory cache is unknown, or the rules contradict)."""


@dataclass(frozen=True)
class Layout:
    warehouses: int                 # loaded
    active_warehouses: int | None   # used by the clerks (1 .. active); None: every warehouse
    cache_bytes: int | None         # the memory cache it was calculated from (None: not needed)
    by_cache: tuple[str, ...]       # which sizes were calculated ("warehouses", "active_warehouses")

    @property
    def extra_warehouses(self) -> tuple[int, int] | None:
        """(first, last) of the warehouses the clerks do not use, or None when there are none."""
        if self.active_warehouses is None or self.active_warehouses >= self.warehouses:
            return None
        return self.active_warehouses + 1, self.warehouses

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "by_cache": list(self.by_cache), "extra_warehouses": self.extra_warehouses}


def warehouses_by_cache(cache_bytes: int, multiple: float) -> int:
    """Enough warehouses that order_line is at least `multiple` x the memory cache."""
    return max(1, math.ceil(multiple * cache_bytes / ORDER_LINE_BYTES_PER_WAREHOUSE))


def active_by_cache(cache_bytes: int, share: float) -> int:
    """As many warehouses as fit in `share` of the memory cache, at least 1."""
    return max(1, math.floor(share * cache_bytes / WAREHOUSE_BYTES))


def needs_cache(tpcc: TpccOptions) -> bool:
    return BY_CACHE in (tpcc.warehouses, tpcc.active_warehouses)


def resolve_layout(tpcc: TpccOptions, cache_bytes: int | None) -> Layout:
    """The layout for this target. `cache_bytes` is the memory cache read from the target; it is
    needed only when a size is `by_cache`."""
    calculated: list[str] = []
    if needs_cache(tpcc) and not cache_bytes:
        raise SizingError("the TPC-C sizes are calculated from the memory cache (by_cache), "
                          "but the target's memory cache could not be read")
    if tpcc.warehouses == BY_CACHE:
        assert cache_bytes and tpcc.order_line_cache_multiple
        warehouses = warehouses_by_cache(cache_bytes, tpcc.order_line_cache_multiple)
        calculated.append("warehouses")
    else:
        warehouses = int(tpcc.warehouses)
    active: int | None
    if tpcc.active_warehouses == BY_CACHE:
        assert cache_bytes and tpcc.active_cache_share
        active = min(active_by_cache(cache_bytes, tpcc.active_cache_share), warehouses)
        calculated.append("active_warehouses")
    else:
        active = tpcc.active_warehouses
    if active is not None and active > warehouses:
        raise SizingError(f"active_warehouses ({active}) is more than warehouses ({warehouses})")
    return Layout(warehouses=warehouses, active_warehouses=active,
                  cache_bytes=cache_bytes if calculated else None, by_cache=tuple(calculated))


def resolved_options(tpcc: TpccOptions, layout: Layout) -> TpccOptions:
    """The scenario's TPC-C options with every size a number."""
    return tpcc.model_copy(update={"warehouses": layout.warehouses, "active_warehouses": layout.active_warehouses,
                                   "order_line_cache_multiple": None, "active_cache_share": None})

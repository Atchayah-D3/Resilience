"""The TPC-C layout calculated from the target's memory cache (NL-N-I01; tpcc/sizing.py)."""

from __future__ import annotations

import pytest

from catalog.schema import TpccOptions
from resilience_tests.execution.workload.tpcc import sizing

GB = 1024 ** 3


def by_cache(**extra) -> TpccOptions:
    return TpccOptions(warehouses="by_cache", order_line_cache_multiple=8,
                       active_warehouses="by_cache", active_cache_share=0.5, **extra)


def test_the_lab_layout_with_a_1_gb_cache():
    layout = sizing.resolve_layout(by_cache(), 1 * GB)
    # order_line at 8 x 1 GB = 8 GB, about 29 MB per warehouse -> 283 warehouses
    assert layout.warehouses == 283
    assert layout.warehouses * sizing.ORDER_LINE_BYTES_PER_WAREHOUSE >= 8 * GB
    assert (layout.warehouses - 1) * sizing.ORDER_LINE_BYTES_PER_WAREHOUSE < 8 * GB
    # half of 1 GB holds 4 warehouses of about 110 MB
    assert layout.active_warehouses == 4
    assert layout.extra_warehouses == (5, 283)
    assert layout.by_cache == ("warehouses", "active_warehouses") and layout.cache_bytes == GB


def test_the_layout_follows_the_cache():
    small, large = sizing.resolve_layout(by_cache(), GB // 8), sizing.resolve_layout(by_cache(), 2 * GB)
    assert small.warehouses < 283 < large.warehouses
    assert small.active_warehouses == 1          # 64 MB holds no whole warehouse: at least 1
    assert large.active_warehouses == 9


def test_fixed_numbers_need_no_cache_and_are_kept():
    layout = sizing.resolve_layout(TpccOptions(warehouses=32, active_warehouses=3), None)
    assert (layout.warehouses, layout.active_warehouses, layout.by_cache, layout.cache_bytes) == (32, 3, (), None)
    assert layout.extra_warehouses == (4, 32)


def test_without_active_warehouses_every_warehouse_is_used_and_none_is_extra():
    layout = sizing.resolve_layout(TpccOptions(warehouses=16), None)
    assert layout.active_warehouses is None and layout.extra_warehouses is None


def test_an_unreadable_cache_refuses_rather_than_guessing():
    with pytest.raises(sizing.SizingError, match="memory cache could not be read"):
        sizing.resolve_layout(by_cache(), None)


def test_calculated_active_warehouses_never_exceed_the_loaded_ones():
    layout = sizing.resolve_layout(TpccOptions(warehouses=2, active_warehouses="by_cache", active_cache_share=1), 8 * GB)
    assert layout.active_warehouses == 2 and layout.extra_warehouses is None


def test_resolved_options_carry_numbers_only():
    tpcc = sizing.resolved_options(by_cache(), sizing.resolve_layout(by_cache(), GB))
    assert (tpcc.warehouses, tpcc.active_warehouses) == (283, 4) and tpcc.resolved
    assert tpcc.order_line_cache_multiple is None and tpcc.active_cache_share is None


@pytest.mark.parametrize("fields, message", [
    ({"warehouses": "by_cache"}, "order_line_cache_multiple is required"),
    ({"warehouses": 8, "order_line_cache_multiple": 8}, "order_line_cache_multiple is required"),
    ({"warehouses": 8, "active_warehouses": "by_cache"}, "active_cache_share is required"),
    ({"warehouses": 8, "active_warehouses": 9}, "more than warehouses"),
    ({"warehouses": 8, "active_warehouses": 2, "home_warehouse": {"g": 1}}, "not combined"),
])
def test_the_catalog_refuses_contradicting_sizes(fields, message):
    with pytest.raises(ValueError, match=message):
        TpccOptions(**fields)

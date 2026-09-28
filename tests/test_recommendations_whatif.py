"""Tests for the recommendation engine and the what-if simulator (Step 9)."""
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from python_pipeline import recommendation_engine as re_  # noqa: E402
from python_pipeline import segment_rules as sr  # noqa: E402
from python_pipeline import what_if_simulator as wi  # noqa: E402

CFG = sr.load_config()


@pytest.fixture(scope="module")
def recs():
    return re_.build(re_.load_sources(), CFG)


def test_every_recommendation_has_numeric_evidence(recs):
    assert len(recs) > 0
    for r in recs.itertuples():
        assert len(r.evidence) >= 2, r.recommendation_id
        # each line carries a computed number, not a static sentence
        assert sum(bool(re.search(r"\d", e)) for e in r.evidence) >= 2, r.recommendation_id
    assert recs.type.nunique() == 9


def test_priority_buckets_follow_percentiles():
    impacts = list(range(1, 101))
    got = re_.assign_priority(impacts, CFG["recommendations"])
    by_value = dict(zip(impacts, got))
    assert [by_value[v] for v in (100, 91)] == ["Critical", "Critical"]
    assert by_value[90] == "High" and by_value[71] == "High"
    assert by_value[70] == "Medium" and by_value[31] == "Medium"
    assert by_value[30] == "Low" and by_value[1] == "Low"
    assert [got.count(p) for p in re_.PRIORITIES] == [10, 20, 40, 30]
    # the same bucket sizes on a different scale: buckets come from ranks, not fixed values
    scaled = re_.assign_priority([v * 1e6 for v in impacts], CFG["recommendations"])
    assert scaled == got


def test_priority_counts_on_real_output(recs):
    n = len(recs)
    critical = (recs.priority == "Critical").sum()
    assert critical == pytest.approx(0.10 * n, abs=1)


# baseline: price 100, cost 40, 1,000 units, 20% of portions wasted (250 units)
B = wi.Baseline(item_id="X", item_name="x", price=100.0, unit_cost=40.0, quantity=1000.0,
                wastage_pct=0.2, discount_pct=0.2, promotion_dependency=0.3, elasticity=-1.0,
                elasticity_source="test", promo_margin_effect=-0.1, period_days=100,
                forecast_daily_demand=12.0)


def check(r, revenue, margin, demand=None):
    assert r["note"] == "Simulated estimate, not an actual result"
    assert r["simulated"]["revenue"] == pytest.approx(revenue)
    assert r["simulated"]["contribution_margin"] == pytest.approx(margin)
    if demand is not None:
        assert r["simulated"]["demand"] == pytest.approx(demand)
    for k in ["revenue", "contribution_margin", "demand", "wastage_cost", "profitability"]:
        assert k in r["simulated"] and k in r["baseline"]


def test_baseline_metrics():
    m = wi.baseline_metrics(B)
    assert m["revenue"] == 100_000 and m["contribution_margin"] == 60_000
    assert m["wastage_units"] == pytest.approx(250) and m["wastage_cost"] == pytest.approx(10_000)
    assert m["net_margin_after_wastage"] == pytest.approx(50_000)


def test_price_change():
    # +10% price, elasticity -1: demand 900, price 110
    r = wi.price_change(B, 0.10)
    check(r, 110 * 900, 70 * 900, 900)
    assert r["simulated"]["wastage_units"] == pytest.approx(225)


def test_discount_change():
    # list price 125; discount 20% -> 10% gives effective price 112.5 (+12.5%), demand 875
    r = wi.discount_change(B, 0.10)
    check(r, 112.5 * 875, 72.5 * 875, 875)


def test_promotion_frequency():
    # promoted share 0.3, doubled: +30% revenue at margin rate 0.6 - 0.1 = 0.5
    r = wi.promotion_frequency(B, 2.0)
    check(r, 130_000, 60_000 + 30_000 * 0.5, 1300)
    same = wi.promotion_frequency(B, 1.0)
    check(same, 100_000, 60_000, 1000)


def test_remove_item():
    r = wi.remove_item(B)
    check(r, 0, 0, 0)
    assert any("not modelled" in s for s in r["limitations"])


def test_reduce_prep():
    # prepared 1,250; a 10% cut (125) comes out of 250 wasted units
    r = wi.reduce_prep(B, 0.10)
    check(r, 100_000, 60_000, 1000)
    assert r["simulated"]["wastage_units"] == pytest.approx(125)
    # daily prep 1,125 / 100 = 11.25 is below the forecast of 12 a day
    assert len(r["flags"]) == 1 and r["flags"][0].startswith("stockout risk")
    r = wi.reduce_prep(wi.Baseline(**{**B.__dict__, "forecast_daily_demand": 11.0}), 0.10)
    assert r["flags"] == []
    r = wi.reduce_prep(B, 0.30)  # cut 375 > waste 250: 125 units of sales lost
    check(r, 100 * 875, 60 * 875, 875)
    assert any(f.startswith("stockout") for f in r["flags"])


def test_demand_change():
    r = wi.demand_change(B, 0.20)
    check(r, 120_000, 72_000, 1200)
    assert r["simulated"]["wastage_cost"] == pytest.approx(12_000)


def test_wastage_assumption():
    # 10% wastage: 1000 * 0.1 / 0.9 = 111.1 wasted units
    r = wi.wastage_assumption(B, 0.10)
    check(r, 100_000, 60_000, 1000)
    waste = 1000 * 0.1 / 0.9 * 40
    assert r["simulated"]["wastage_cost"] == pytest.approx(waste)
    assert r["net_margin_improvement"] == pytest.approx(10_000 - waste)


def test_simulate_on_real_item_is_labelled():
    item = re_.load_sources()["classification"].item_id.iloc[0]
    for name, params in [("price_change", {"pct": 0.1}), ("remove_item", {}),
                         ("reduce_prep", {"pct": 0.05})]:
        r = wi.simulate(item, name, **params)
        assert r["note"] == wi.NOTE and r["baseline_inputs"]["item_id"] == item

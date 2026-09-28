"""Unit tests for spark_jobs/05_menu_classification.py on small synthetic item rows.

Each test ranks one target item against ten filler items with evenly spaced metrics, so
a target value above every filler lands at percentile 1.0 (High), below every filler at
0.0 (Low), and between the 5th and 6th filler at 0.5 (Medium). Boundary values are
derived from the loaded config so the tests still hold if a threshold is changed.
"""
import datetime as dt
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "menu_classification",
    Path(__file__).resolve().parent.parent / "spark_jobs" / "05_menu_classification.py",
)
mc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mc)

T = mc.load_thresholds()

ITEM_SCHEMA = (
    "item_id string, total_quantity_sold long, profit_percentage double, "
    "contribution_margin double, average_rating double, repeat_purchase_rate double, "
    "wastage_percentage double, promotion_dependency double, weekend_order_ratio double, "
    "days_since_launch int, is_seasonal boolean"
)

# (total_quantity_sold, profit_percentage, (average_rating, repeat_purchase_rate), wastage_percentage)
HIGH = dict(qty=500, profit=80.0, quality=(4.8, 0.5), waste=max(0.10, T["excessive_wastage_min"] + 0.05))
MID = dict(qty=145, profit=54.5, quality=(3.72, 0.145), waste=0.0145)
LOW = dict(qty=10, profit=20.0, quality=(3.0, 0.05), waste=0.001)


def filler(i):
    return dict(item_id=f"F{i}", total_quantity_sold=100 + 10 * i, profit_percentage=50.0 + i,
                contribution_margin=1000.0, average_rating=3.5 + 0.05 * i,
                repeat_purchase_rate=0.10 + 0.01 * i, wastage_percentage=0.010 + 0.001 * i,
                promotion_dependency=0.1, weekend_order_ratio=0.45, days_since_launch=1000,
                is_seasonal=False)


def item(qty=MID, profit=MID, quality=MID, waste=MID, **overrides):
    rating, repeat = quality["quality"]
    row = dict(item_id="X", total_quantity_sold=qty["qty"], profit_percentage=profit["profit"],
               contribution_margin=1000.0, average_rating=rating, repeat_purchase_rate=repeat,
               wastage_percentage=waste["waste"], promotion_dependency=0.1,
               weekend_order_ratio=0.45, days_since_launch=1000, is_seasonal=False)
    row.update(overrides)
    return row


def to_df(spark, rows):
    cols = [c.split()[0] for c in ITEM_SCHEMA.split(", ")]
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], ITEM_SCHEMA)


def run(spark, target, t=T):
    df = to_df(spark, [filler(i) for i in range(10)] + [target])
    out = mc.add_flags(mc.classify(df, t), t)
    return {r["item_id"]: r.asDict() for r in out.collect()}


def classify_one(spark, target, t=T):
    return run(spark, target, t)["X"]


def raised(row):
    return {f for f in mc.FLAGS if f != "location_inconsistent" and row[f]}


# --- main categories under normal conditions ---

def test_profit_driver(spark):
    r = classify_one(spark, item(qty=HIGH, profit=HIGH, waste=LOW))
    assert (r["demand_tier"], r["profitability_tier"], r["wastage_tier"]) == ("High", "High", "Low")
    assert r["category"] == mc.PROFIT_DRIVER
    assert raised(r) == set()


def test_volume_driver(spark):
    r = classify_one(spark, item(qty=HIGH, profit=LOW))
    assert r["category"] == mc.VOLUME_DRIVER
    assert raised(r) == set()


def test_hidden_opportunity_from_quality(spark):
    r = classify_one(spark, item(quality=HIGH))
    assert (r["demand_tier"], r["profitability_tier"], r["quality_tier"]) == ("Medium", "Medium", "High")
    assert r["category"] == mc.HIDDEN_OPPORTUNITY


def test_low_performer(spark):
    r = classify_one(spark, item(qty=LOW, profit=LOW, quality=LOW))
    assert r["category"] == mc.LOW_PERFORMER
    assert raised(r) == set()


def test_all_medium_is_low_performer(spark):
    r = classify_one(spark, item())
    assert {r["demand_tier"], r["profitability_tier"], r["quality_tier"]} == {"Medium"}
    assert r["demand_percentile"] == pytest.approx(0.5)
    assert r["category"] == mc.LOW_PERFORMER


# --- the ten tricky cases ---

def test_high_selling_loss_making_is_volume_driver(spark):
    r = classify_one(spark, item(qty=HIGH, profit_percentage=-15.0, contribution_margin=-2000.0))
    assert r["loss_making"]
    assert r["profitability_tier"] == "Low"
    assert r["category"] == mc.VOLUME_DRIVER


def test_highly_profitable_rarely_purchased(spark):
    r = classify_one(spark, item(qty=LOW, profit=HIGH))
    assert r["rarely_purchased_high_margin"]
    assert r["category"] == mc.HIDDEN_OPPORTUNITY


def test_popular_dish_with_excessive_wastage_loses_profit_driver(spark):
    r = classify_one(spark, item(qty=HIGH, profit=HIGH, waste=HIGH))
    assert r["excessive_wastage"]
    assert r["category"] == mc.VOLUME_DRIVER


def test_relatively_high_wastage_under_floor_is_not_excessive(spark):
    # top wastage tier, but the whole menu wastes little, so it is not excessive
    r = classify_one(spark, item(qty=HIGH, profit=HIGH, wastage_percentage=T["excessive_wastage_min"]))
    assert r["wastage_tier"] == "High"
    assert not r["excessive_wastage"]
    assert r["category"] == mc.PROFIT_DRIVER


def test_highly_rated_poor_profitability(spark):
    r = classify_one(spark, item(profit=LOW, average_rating=T["high_rating_min"], repeat_purchase_rate=0.3))
    assert r["high_rating_low_profit"]
    assert r["quality_tier"] == "High"
    assert r["category"] == mc.HIDDEN_OPPORTUNITY


def test_low_rated_high_sales(spark):
    r = classify_one(spark, item(qty=HIGH, average_rating=T["low_rating_max"]))
    assert r["low_rating_high_sales"]
    assert r["category"] == mc.VOLUME_DRIVER


@pytest.mark.parametrize("delta, expected", [(0.01, True), (0.0, False)])
def test_promotion_dependent(spark, delta, expected):
    r = classify_one(spark, item(promotion_dependency=T["promotion_dependent_above"] + delta))
    assert r["promotion_dependent"] is expected


@pytest.mark.parametrize("delta, expected", [(0.01, True), (0.0, False)])
def test_weekend_skewed(spark, delta, expected):
    r = classify_one(spark, item(weekend_order_ratio=T["weekend_skewed_above"] + delta))
    assert r["weekend_skewed"] is expected


def test_seasonal_item_flag_does_not_change_category(spark):
    plain = classify_one(spark, item(qty=HIGH, profit=HIGH, waste=LOW))
    seasonal = classify_one(spark, item(qty=HIGH, profit=HIGH, waste=LOW, is_seasonal=True))
    assert seasonal["seasonal_item"] and not plain["seasonal_item"]
    assert seasonal["category"] == plain["category"] == mc.PROFIT_DRIVER


def test_new_item_is_insufficient_history_regardless_of_numbers(spark):
    r = classify_one(spark, item(qty=HIGH, profit=HIGH, quality=HIGH, waste=HIGH, is_seasonal=True,
                                 promotion_dependency=0.9, contribution_margin=-10.0,
                                 days_since_launch=T["new_item_days"] - 1))
    assert r["category"] == mc.INSUFFICIENT_HISTORY
    assert r["insufficient_history"]
    # flags that don't need percentiles are still computed
    assert r["seasonal_item"] and r["promotion_dependent"] and r["loss_making"]
    assert r["demand_percentile"] is None and r["profitability_percentile"] is None
    assert not (r["rarely_purchased_high_margin"] or r["excessive_wastage"]
                or r["low_rating_high_sales"])


def test_item_at_new_item_cutoff_is_classified_normally(spark):
    r = classify_one(spark, item(qty=HIGH, profit=HIGH, waste=LOW, days_since_launch=T["new_item_days"]))
    assert r["category"] == mc.PROFIT_DRIVER
    assert not r["insufficient_history"]


def test_new_items_do_not_shift_established_percentiles(spark):
    rows = run(spark, item(qty=HIGH, days_since_launch=1))
    assert rows["F9"]["demand_percentile"] == pytest.approx(1.0)
    assert rows["F0"]["demand_percentile"] == pytest.approx(0.0)


def test_zero_sales_is_low_performer(spark):
    r = classify_one(spark, item(total_quantity_sold=0, profit_percentage=None, contribution_margin=0.0,
                                 average_rating=None, repeat_purchase_rate=None,
                                 wastage_percentage=1.0, promotion_dependency=None,
                                 weekend_order_ratio=None))
    assert r["category"] == mc.LOW_PERFORMER
    assert r["demand_percentile"] is None and r["wastage_percentile"] is None
    assert raised(r) == set()


def test_unrated_item_uses_repeat_rate_for_quality(spark):
    r = classify_one(spark, item(average_rating=None, repeat_purchase_rate=0.9))
    assert r["quality_score"] == pytest.approx(0.9)
    assert r["category"] == mc.HIDDEN_OPPORTUNITY


# --- location consistency ---

def loc_rows(target_id, locations, levels):
    rows = []
    for loc, lvl in zip(locations, levels):
        for i in range(10):
            f = filler(i)
            rows.append(dict(f, location_id=loc))
        rows.append(dict(item(qty=lvl, profit=lvl, quality=lvl, waste=LOW), item_id=target_id,
                         location_id=loc))
    return rows


def test_location_inconsistent(spark):
    t = T
    min_locs = t["location"]["min_locations"]
    n = max(4, min_locs)
    n_diff = -(-n * t["location"]["inconsistent_share"] // 1)  # smallest count at the share
    x_levels = [LOW] * int(n_diff) + [HIGH] * (n - int(n_diff))
    rows = (loc_rows("X", [f"LX{i}" for i in range(n)], x_levels)
            + loc_rows("Y", [f"LY{i}" for i in range(n)], [HIGH] * n)
            + loc_rows("Z", [f"LZ{i}" for i in range(min_locs - 1)], [LOW] * (min_locs - 1)))
    cols = ["location_id"] + [c.split()[0] for c in ITEM_SCHEMA.split(", ")]
    loc_metrics = spark.createDataFrame(
        [tuple(r[c] for c in cols) for r in rows],
        "location_id string, " + ITEM_SCHEMA,
    ).drop("promotion_dependency", "weekend_order_ratio", "is_seasonal", "days_since_launch")
    item_categories = spark.createDataFrame(
        [(i, mc.PROFIT_DRIVER, 1000) for i in ("X", "Y", "Z")]
        + [(f"F{i}", mc.LOW_PERFORMER, 1000) for i in range(10)],
        "item_id string, category string, days_since_launch int",
    )

    per_item, by_loc = mc.location_consistency(item_categories, loc_metrics, t)
    res = {r["item_id"]: r.asDict() for r in per_item.collect()}

    assert res["X"]["location_inconsistent"]
    assert res["X"]["location_mismatch_share"] == pytest.approx(n_diff / n)
    assert dict(res["X"]["location_categories"])["LX0"] == mc.LOW_PERFORMER
    assert not res["Y"]["location_inconsistent"]
    assert set(dict(res["Y"]["location_categories"]).values()) == {mc.PROFIT_DRIVER}
    # every location disagrees, but too few locations to judge
    assert not res["Z"]["location_inconsistent"]
    assert res["Z"]["location_mismatch_share"] is None
    assert by_loc.filter("item_id = 'X'").count() == n


# --- build() wiring ---

def test_build_filters_inactive_and_derives_launch_age(spark, monkeypatch):
    as_of = "2025-12-31"
    feats = [dict(filler(i), item_name=f"F{i}", category_name="C") for i in range(10)]
    feats.append(dict(item(qty=HIGH, profit=HIGH, waste=LOW, item_id="NEW"), item_name="New",
                      category_name="C"))
    feats.append(dict(item(item_id="OFF"), item_name="Off", category_name="C"))
    feat_cols = [c.split()[0] for c in ITEM_SCHEMA.split(", ")
                 if c.split()[0] not in ("days_since_launch", "is_seasonal")]
    item_feats = spark.createDataFrame(
        [tuple(r[c] for c in feat_cols) + (r["item_name"], r["category_name"], 1.0, 1.0, None, None, 0.0)
         for r in feats],
        ", ".join(c for c in ITEM_SCHEMA.split(", ")
                  if c.split()[0] not in ("days_since_launch", "is_seasonal"))
        + ", item_name string, category_name string, item_revenue double, item_cost double, "
          "sales_trend double, rating_trend double, price_change_percentage double",
    )
    menu = spark.createDataFrame(
        [(f"F{i}", True, dt.date(2020, 1, 1), False) for i in range(10)]
        + [("NEW", True, dt.date(2025, 11, 1), True), ("OFF", False, dt.date(2020, 1, 1), False)],
        "item_id string, is_active boolean, launch_date date, is_seasonal boolean",
    )
    empty_loc = spark.createDataFrame(
        [], "item_id string, location_id string, contribution_margin double, profit_percentage double, "
            "total_quantity_sold long, repeat_purchase_rate double, average_rating double, "
            "wastage_percentage double")
    monkeypatch.setattr(mc.fe, "item_location_features", lambda *a: empty_loc)

    classification, _ = mc.build(item_feats, menu, None, None, None, as_of, T)
    rows = {r["item_id"]: r.asDict() for r in classification.collect()}

    assert "OFF" not in rows
    assert len(rows) == 11
    new = rows["NEW"]
    assert new["days_since_launch"] == 60
    assert new["category"] == mc.INSUFFICIENT_HISTORY
    assert new["seasonal_item"]
    assert set(mc.FLAGS) <= set(new)
    assert new["location_inconsistent"] is False and new["n_locations_sold"] == 0


# --- config drives behavior ---

def edited_config(tmp_path, old, new):
    text = mc.CONFIG_PATH.read_text()
    assert old in text
    path = tmp_path / "thresholds.yaml"
    path.write_text(text.replace(old, new))
    return mc.load_thresholds(path)


def test_config_wastage_floor_changes_category(spark, tmp_path):
    floor = T["excessive_wastage_min"]
    boundary = item(qty=HIGH, profit=HIGH, wastage_percentage=floor - 0.01)
    assert classify_one(spark, boundary)["category"] == mc.PROFIT_DRIVER

    lowered = edited_config(tmp_path, f"excessive_wastage_min: {floor}",
                            f"excessive_wastage_min: {floor - 0.02}")
    r = classify_one(spark, boundary, lowered)
    assert r["excessive_wastage"]
    assert r["category"] == mc.VOLUME_DRIVER


def test_config_new_item_days_changes_category(spark, tmp_path):
    days = T["new_item_days"]
    boundary = item(qty=HIGH, profit=HIGH, waste=LOW, days_since_launch=days + 10)
    assert classify_one(spark, boundary)["category"] == mc.PROFIT_DRIVER

    longer = edited_config(tmp_path, f"new_item_days: {days}", f"new_item_days: {days + 30}")
    assert classify_one(spark, boundary, longer)["category"] == mc.INSUFFICIENT_HISTORY

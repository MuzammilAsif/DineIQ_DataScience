"""Tests for spark_jobs/15_demand_forecasting.py on a small synthetic daily series."""
import datetime as dt
import importlib.util
import math
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "demand_forecasting",
    Path(__file__).resolve().parent.parent / "spark_jobs" / "15_demand_forecasting.py")
fc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fc)

START = dt.date(2025, 1, 1)
GAP = START + dt.timedelta(days=40)


def qty(i):
    return float(10 + i % 7 + i // 10)


@pytest.fixture(scope="module")
def daily(spark):
    # 90 stocked days for one item-location, except one missing day (not stocked)
    rows = [("I1", "L1", START + dt.timedelta(days=i), qty(i)) for i in range(90)
            if START + dt.timedelta(days=i) != GAP]
    return spark.createDataFrame(rows, "item_id string, location_id string, date date, qty double")


def cfg(h=1):
    return {"horizon_days": h, "lags": [1, 7, 14], "rolling_windows": [7, 14], "test_days": 20}


def test_lag_7_equals_quantity_seven_days_earlier(daily):
    rows = {r.origin_date: r for r in fc.build_table(daily, cfg()).collect()}
    o = START + dt.timedelta(days=30)
    assert rows[o].lag_7 == qty(23) and rows[o].lag_1 == qty(29) and rows[o].lag_14 == qty(16)
    assert rows[o].rolling_mean_7 == pytest.approx(sum(qty(i) for i in range(23, 30)) / 7)
    assert rows[o].target == qty(30)
    # with horizon 1 the baseline is exactly lag_7
    assert all(r.same_weekday_last_week == r.lag_7 for r in rows.values())
    # no row may borrow a neighbour for the missing day
    for d in [GAP + dt.timedelta(days=k) for k in (1, 7, 14)]:
        assert d not in rows
    assert min(rows) == START + dt.timedelta(days=14)


def test_horizon_shifts_target_not_history(daily):
    rows = {r.origin_date: r for r in fc.build_table(daily, cfg(h=7)).collect()}
    o = START + dt.timedelta(days=20)
    assert rows[o].target_date == o + dt.timedelta(days=6)
    assert rows[o].target == qty(26)
    assert rows[o].lag_1 == qty(19)
    # baseline = same weekday as the target, latest one known at the origin
    assert rows[o].same_weekday_last_week == qty(19)


def test_split_is_chronological(daily):
    table = fc.build_table(daily, cfg())
    train, test, test_start = fc.chronological_split(table, 20)
    max_train = train.agg({"target_date": "max"}).first()[0]
    min_test = test.agg({"target_date": "min"}).first()[0]
    assert max_train < min_test == test_start
    assert train.count() + test.count() == table.count()


def test_metrics_hand_computed(spark):
    df = spark.createDataFrame([(10.0, 12.0), (0.0, 1.0), (5.0, 2.0), (20.0, 20.0)],
                               "actual double, pred double")
    m = fc.metrics(df, "pred")
    errors = [2, 1, -3, 0]
    assert m["mae"] == pytest.approx(sum(abs(e) for e in errors) / 4)
    assert m["rmse"] == pytest.approx(math.sqrt(sum(e * e for e in errors) / 4))
    # MAPE skips the zero actual
    assert m["mape"] == pytest.approx((2 / 10 + 3 / 5 + 0 / 20) / 3)
    mean = 35 / 4
    sst = sum((a - mean) ** 2 for a in [10, 0, 5, 20])
    assert m["r2"] == pytest.approx(1 - 14 / sst)
    assert m["zero_actuals"] == 1 and m["n"] == 4

"""Unit tests for spark_jobs/04_feature_engineering.py on a small synthetic dataset.

Orders (customer, location, time, channel, status, promo, total_amount):
  O1  A  L1  2025-01-03 12:30 Fri  Dine-in  Completed  P1  50   lines: I1 x2 (disc 5, total 15), I2 x1 (10)
  O2  A  L1  2025-01-06 16:00 Mon  App      Completed  -   30   lines: I1 x1 (10)
  O3  B  L2  2025-01-07 20:00 Tue  App      Completed  -   40   lines: I1 x3 (30), I2 x1 (10)
  O4  B  L2  2025-01-08 13:00 Wed  Dine-in  Cancelled  -   99   lines: I1 x5 (50)
  O5  A  L2  2025-03-01 19:30 Sat  App      Completed  -   20   lines: I1 x1 (10)
Unit cost: I1 = 4, I2 = 3. I3 is on the menu but never sold. L3 has no orders.
"""
import datetime as dt
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "feature_engineering",
    Path(__file__).resolve().parent.parent / "spark_jobs" / "04_feature_engineering.py",
)
fe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fe)

FULL = "2025-12-31"
JAN = "2025-01-31"


def ts(s):
    return dt.datetime.fromisoformat(s)


def d(s):
    return dt.date.fromisoformat(s)


ORDERS = [
    ("O1", "A", "L1", ts("2025-01-03 12:30:00"), "Dine-in", "Completed", "P1", 50.0),
    ("O2", "A", "L1", ts("2025-01-06 16:00:00"), "App", "Completed", None, 30.0),
    ("O3", "B", "L2", ts("2025-01-07 20:00:00"), "App", "Completed", None, 40.0),
    ("O4", "B", "L2", ts("2025-01-08 13:00:00"), "Dine-in", "Cancelled", None, 99.0),
    ("O5", "A", "L2", ts("2025-03-01 19:30:00"), "App", "Completed", None, 20.0),
]
# order_item_id, order_id, item_id, quantity, unit_cost, line_discount_amount, line_total
LINES = [
    ("L01", "O1", "I1", 2, 4.0, 5.0, 15.0),
    ("L02", "O1", "I2", 1, 3.0, 0.0, 10.0),
    ("L03", "O2", "I1", 1, 4.0, 0.0, 10.0),
    ("L04", "O3", "I1", 3, 4.0, 0.0, 30.0),
    ("L05", "O3", "I2", 1, 3.0, 0.0, 10.0),
    ("L06", "O4", "I1", 5, 4.0, 0.0, 50.0),
    ("L07", "O5", "I1", 1, 4.0, 0.0, 10.0),
]


@pytest.fixture(scope="module")
def data(spark):
    orders = spark.createDataFrame(
        ORDERS,
        "order_id string, customer_id string, location_id string, order_datetime timestamp, "
        "order_channel string, order_status string, promotion_id string, total_amount double",
    )
    lines = spark.createDataFrame(
        LINES,
        "order_item_id string, order_id string, item_id string, quantity int, unit_cost double, "
        "line_discount_amount double, line_total double",
    )
    fact = lines.join(orders.drop("total_amount"), "order_id")
    menu_items = spark.createDataFrame(
        [("I1", "Item One", "C1", 4.0, d("2020-01-01")),
         ("I2", "Item Two", "C1", 3.0, d("2020-01-01")),
         ("I3", "Item Three", "C2", 5.0, d("2020-01-01"))],
        "item_id string, item_name string, category_id string, base_cost double, launch_date date",
    )
    menu_categories = spark.createDataFrame(
        [("C1", "Cat One"), ("C2", "Cat Two")], "category_id string, category_name string"
    )
    ratings = spark.createDataFrame(
        [("I1", "L1", 4, d("2025-01-10")), ("I1", "L2", 2, d("2025-01-20")),
         ("I1", "L1", 4, d("2025-02-05")), ("I1", "L2", 5, d("2025-03-05")),
         ("I2", "L2", 3, d("2025-01-15"))],
        "item_id string, location_id string, rating_value int, rating_date date",
    )
    wastage = spark.createDataFrame(
        [("I1", "L1", d("2025-01-05"), 8.0), ("I1", "L2", d("2025-04-01"), 4.0),
         ("I3", "L1", d("2025-01-10"), 10.0)],
        "item_id string, location_id string, date date, cost_of_waste double",
    )
    pricing = spark.createDataFrame(
        [("I1", None, 10.0, d("2024-07-01")), ("I1", None, 12.0, d("2025-06-01")),
         ("I1", "L1", 15.0, d("2025-02-01")), ("I2", None, 10.0, d("2024-07-01"))],
        "item_id string, location_id string, price double, effective_start_date date",
    )
    restaurants = spark.createDataFrame(
        [("L1", "Loc One", "X", "R", "Urban"), ("L2", "Loc Two", "Y", "R", "Mall"),
         ("L3", "Loc Three", "Z", "R", "Highway")],
        "location_id string, name string, city string, region string, location_type string",
    )
    return dict(orders=orders, fact=fact, menu_items=menu_items, menu_categories=menu_categories,
                ratings=ratings, wastage=wastage, pricing=pricing, restaurants=restaurants)


def items_as_of(data, as_of):
    df = fe.item_features(data["fact"], data["menu_items"], data["menu_categories"],
                          data["ratings"], data["wastage"], data["pricing"], as_of)
    return {r["item_id"]: r.asDict() for r in df.collect()}


def customers_as_of(data, as_of):
    df = fe.customer_features(data["orders"], data["fact"], as_of)
    return {r["customer_id"]: r.asDict() for r in df.collect()}


def locations_as_of(data, as_of):
    df = fe.location_features(data["fact"], data["wastage"], data["restaurants"], as_of)
    return {r["location_id"]: r.asDict() for r in df.collect()}


def test_item_sales_features(data):
    i1 = items_as_of(data, FULL)["I1"]
    assert i1["item_revenue"] == pytest.approx(65.0)  # O4 is cancelled
    assert i1["item_cost"] == pytest.approx(28.0)
    assert i1["contribution_margin"] == pytest.approx(37.0)
    assert i1["profit_percentage"] == pytest.approx(37 / 65 * 100)
    assert i1["total_quantity_sold"] == 7
    assert i1["order_frequency"] == 4
    assert i1["repeat_purchase_rate"] == pytest.approx(0.5)  # A: 3 orders, B: 1
    assert i1["promotion_dependency"] == pytest.approx(15 / 65)
    assert i1["discount_percentage"] == pytest.approx(5 / 70)


def test_item_popularity_is_percentile_rank(data):
    items = items_as_of(data, FULL)
    assert items["I3"]["item_popularity"] == pytest.approx(0.0)
    assert items["I2"]["item_popularity"] == pytest.approx(0.5)
    assert items["I1"]["item_popularity"] == pytest.approx(1.0)


def test_item_rating_features(data):
    i1 = items_as_of(data, FULL)["I1"]
    assert i1["average_rating"] == pytest.approx(3.75)
    # monthly means Jan 3, Feb 4, Mar 5
    assert i1["rating_trend"] == pytest.approx(1.0)


def test_rating_trend_null_under_three_months(data):
    items = items_as_of(data, "2025-02-28")
    assert items["I1"]["rating_trend"] is None
    assert items["I1"]["average_rating"] == pytest.approx(10 / 3)
    assert items["I2"]["rating_trend"] is None
    assert items["I3"]["rating_trend"] is None


def test_item_wastage_percentage(data):
    items = items_as_of(data, FULL)
    # I1: waste cost 12 / avg unit cost 4 = 3 portions; 3 / (3 + 7)
    assert items["I1"]["total_quantity_wasted"] == pytest.approx(3.0)
    assert items["I1"]["wastage_percentage"] == pytest.approx(0.3)
    assert items["I2"]["wastage_percentage"] == pytest.approx(0.0)
    # I3 never sold: falls back to base_cost 5 -> 2 portions wasted, 0 sold
    assert items["I3"]["wastage_percentage"] == pytest.approx(1.0)


def test_item_price_change_uses_chain_wide_prices(data):
    items = items_as_of(data, FULL)
    assert items["I1"]["price_change_percentage"] == pytest.approx(20.0)
    assert items["I2"]["price_change_percentage"] == pytest.approx(0.0)
    assert items["I3"]["price_change_percentage"] is None


def test_item_with_no_sales_has_null_ratios(data):
    i3 = items_as_of(data, FULL)["I3"]
    assert i3["item_revenue"] == 0
    assert i3["total_quantity_sold"] == 0
    assert i3["profit_percentage"] is None
    assert i3["repeat_purchase_rate"] is None
    assert i3["promotion_dependency"] is None
    assert i3["discount_percentage"] is None


def test_item_as_of_date_excludes_later_records(data):
    full, jan = items_as_of(data, FULL)["I1"], items_as_of(data, JAN)["I1"]
    assert full["item_revenue"] == pytest.approx(65.0)
    assert jan["item_revenue"] == pytest.approx(55.0)
    assert jan["total_quantity_sold"] == 6
    assert jan["wastage_percentage"] == pytest.approx(2 / 8)  # April wastage excluded
    assert jan["price_change_percentage"] == pytest.approx(0.0)  # June price excluded


def test_items_launched_after_as_of_date_are_excluded(data, spark):
    menu = data["menu_items"].union(spark.createDataFrame(
        [("I9", "Later Item", "C1", 1.0, d("2025-06-01"))], data["menu_items"].schema))
    jan = fe.item_features(data["fact"], menu, data["menu_categories"], data["ratings"],
                           data["wastage"], data["pricing"], JAN)
    assert "I9" not in {r["item_id"] for r in jan.collect()}


def test_item_weekend_order_ratio(data):
    items = items_as_of(data, FULL)
    # I1: Fri x2 + Sat x1 of 7 (cancelled Wed order excluded); I2: Fri x1 of 2
    assert items["I1"]["weekend_order_ratio"] == pytest.approx(3 / 7)
    assert items["I2"]["weekend_order_ratio"] == pytest.approx(0.5)
    assert items["I3"]["weekend_order_ratio"] is None


def test_item_sales_trend(data, spark):
    # I1 sells in Jan and Mar only, so it has under 3 months of history
    assert items_as_of(data, FULL)["I1"]["sales_trend"] is None

    extra = spark.createDataFrame(
        [("L08", "O6", "I1", 2, 4.0, 0.0, 20.0, "A", "L1", ts("2025-02-10 12:00:00"), "App",
          "Completed", None)],
        "order_item_id string, order_id string, item_id string, quantity int, unit_cost double, "
        "line_discount_amount double, line_total double, customer_id string, location_id string, "
        "order_datetime timestamp, order_channel string, order_status string, promotion_id string",
    )
    fact = data["fact"].unionByName(extra)
    df = fe.item_features(fact, data["menu_items"], data["menu_categories"], data["ratings"],
                          data["wastage"], data["pricing"], FULL)
    i1 = {r["item_id"]: r.asDict() for r in df.collect()}["I1"]
    # monthly sums Jan 6, Feb 2, Mar 1
    assert i1["sales_trend"] == pytest.approx(-2.5)


def test_item_location_features(data):
    df = fe.item_location_features(data["fact"], data["ratings"], data["wastage"], FULL)
    rows = {(r["item_id"], r["location_id"]): r.asDict() for r in df.collect()}
    assert set(rows) == {("I1", "L1"), ("I1", "L2"), ("I2", "L1"), ("I2", "L2")}

    l1 = rows[("I1", "L1")]
    assert l1["total_quantity_sold"] == 3
    assert l1["contribution_margin"] == pytest.approx(13.0)
    assert l1["profit_percentage"] == pytest.approx(13 / 25 * 100)
    assert l1["repeat_purchase_rate"] == pytest.approx(1.0)
    assert l1["average_rating"] == pytest.approx(4.0)
    # waste cost 8 / unit cost 4 = 2 portions; 2 / (2 + 3)
    assert l1["wastage_percentage"] == pytest.approx(0.4)

    l2 = rows[("I1", "L2")]
    assert l2["total_quantity_sold"] == 4  # cancelled O4 excluded
    assert l2["repeat_purchase_rate"] == pytest.approx(0.0)
    assert l2["average_rating"] == pytest.approx(3.5)
    assert l2["wastage_percentage"] == pytest.approx(0.2)
    assert rows[("I2", "L1")]["average_rating"] is None


def test_customer_features(data):
    a = customers_as_of(data, FULL)["A"]
    assert a["customer_recency"] == 305  # 2025-03-01 -> 2025-12-31
    assert a["customer_frequency"] == 3
    assert a["customer_monetary_value"] == pytest.approx(100.0)
    assert a["average_order_value"] == pytest.approx(100 / 3)
    assert a["peak_hour_frequency"] == pytest.approx(2 / 3)  # 12:30, 19:30 peak; 16:00 not
    assert a["weekend_order_ratio"] == pytest.approx(2 / 3)  # Fri, Sat
    assert a["channel_preference"] == "App"
    assert a["basket_size_avg"] == pytest.approx(4 / 3)


def test_customer_excludes_cancelled_orders(data):
    b = customers_as_of(data, FULL)["B"]
    assert b["customer_frequency"] == 1
    assert b["customer_monetary_value"] == pytest.approx(40.0)
    assert b["customer_recency"] == 358
    assert b["weekend_order_ratio"] == pytest.approx(0.0)
    assert b["peak_hour_frequency"] == pytest.approx(1.0)


def test_customer_as_of_date_changes_recency(data):
    a = customers_as_of(data, JAN)["A"]
    assert a["customer_recency"] == 25  # last order 2025-01-06
    assert a["customer_frequency"] == 2
    assert a["customer_monetary_value"] == pytest.approx(80.0)
    # Dine-in and App tie 1-1, tie goes to the alphabetically first
    assert a["channel_preference"] == "App"


def test_as_of_date_includes_whole_day(data):
    before = customers_as_of(data, "2025-03-01")["A"]
    assert before["customer_frequency"] == 3
    assert before["customer_recency"] == 0
    assert customers_as_of(data, "2025-02-28")["A"]["customer_frequency"] == 2


def test_peak_hour_boundaries(spark):
    import pyspark.sql.functions as F

    rows = [(ts(f"2025-01-01 {h:02d}:30:00"),) for h in (11, 12, 13, 14, 18, 19, 21, 22)]
    df = spark.createDataFrame(rows, "t timestamp").select(fe.is_peak_hour(F.col("t")).alias("p"))
    assert [r["p"] for r in df.collect()] == [False, True, True, False, False, True, True, False]


def test_location_features(data):
    locs = locations_as_of(data, FULL)
    l1, l2 = locs["L1"], locs["L2"]
    assert l1["location_revenue"] == pytest.approx(35.0)
    assert l1["location_profit"] == pytest.approx(20.0)
    assert l1["location_avg_order_value"] == pytest.approx(17.5)
    assert l1["location_customer_count"] == 1
    assert l1["location_repeat_customer_rate"] == pytest.approx(1.0)
    assert l1["location_wastage_rate"] == pytest.approx(18 / 35)
    assert l2["location_revenue"] == pytest.approx(50.0)  # O4 cancelled
    assert l2["location_profit"] == pytest.approx(31.0)
    assert l2["location_avg_order_value"] == pytest.approx(25.0)
    assert l2["location_customer_count"] == 2
    assert l2["location_repeat_customer_rate"] == pytest.approx(0.0)
    assert l2["location_wastage_rate"] == pytest.approx(0.08)


def test_location_with_no_orders_has_null_ratios(data):
    l3 = locations_as_of(data, FULL)["L3"]
    assert l3["location_revenue"] == 0
    assert l3["location_avg_order_value"] is None
    assert l3["location_repeat_customer_rate"] is None
    assert l3["location_wastage_rate"] is None


def test_location_as_of_date_excludes_later_records(data):
    l2 = locations_as_of(data, JAN)["L2"]
    assert l2["location_revenue"] == pytest.approx(40.0)
    assert l2["location_wastage_rate"] == pytest.approx(0.0)


def test_basket_size(data):
    df = fe.add_basket_size(data["fact"])
    sizes = {r["order_id"]: r["basket_size"] for r in df.select("order_id", "basket_size").distinct().collect()}
    assert sizes == {"O1": 2, "O2": 1, "O3": 2, "O4": 1, "O5": 1}
    assert df.count() == data["fact"].count()

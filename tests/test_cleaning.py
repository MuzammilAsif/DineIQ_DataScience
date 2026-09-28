"""Checks that processed_data/ (written by spark_jobs/02_clean.py) violates none of the
rules in documentation/data_quality_rules.md. Requires that job to have been run first.
"""
import pyspark.sql.functions as F

from schemas import load_clean_table
from spark_utils import PROCESSED_DIR

UNIT_CANON = ["pcs", "kg", "liters"]


def test_no_negative_quantity(spark):
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    assert oi.filter(F.col("quantity") < 0).count() == 0


def test_no_invalid_rating(spark):
    r = load_clean_table(spark, "Ratings", PROCESSED_DIR)
    bad = r.filter(F.col("rating_value").isNull() | (F.col("rating_value") < 1) | (F.col("rating_value") > 5))
    assert bad.count() == 0


def test_no_invalid_price(spark):
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    assert oi.filter(F.col("unit_price").isNull() | (F.col("unit_price") <= 0)).count() == 0
    mi = load_clean_table(spark, "Menu_Items", PROCESSED_DIR)
    assert mi.filter(F.col("base_price") <= 0).count() == 0
    ph = load_clean_table(spark, "Pricing_History", PROCESSED_DIR)
    assert ph.filter(F.col("price") <= 0).count() == 0


def test_no_incorrect_discount(spark):
    o = load_clean_table(spark, "Orders", PROCESSED_DIR)
    assert o.filter(F.col("discount_amount") > F.col("subtotal")).count() == 0


def test_no_missing_required_ids(spark):
    o = load_clean_table(spark, "Orders", PROCESSED_DIR)
    assert o.filter(F.col("customer_id").isNull()).count() == 0
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    assert oi.filter(F.col("item_id").isNull()).count() == 0
    w = load_clean_table(spark, "Wastage", PROCESSED_DIR)
    assert w.filter(F.col("item_id").isNull()).count() == 0


def test_no_orphan_orders_customer(spark):
    orders = load_clean_table(spark, "Orders", PROCESSED_DIR)
    customers = load_clean_table(spark, "Customers", PROCESSED_DIR)
    orphans = orders.join(customers.select("customer_id"), "customer_id", "left_anti")
    assert orphans.count() == 0


def test_no_orphan_order_items(spark):
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    orders = load_clean_table(spark, "Orders", PROCESSED_DIR)
    menu_items = load_clean_table(spark, "Menu_Items", PROCESSED_DIR)
    assert oi.join(orders.select("order_id"), "order_id", "left_anti").count() == 0
    orphan_items = oi.filter(F.col("item_id").isNotNull()).join(
        menu_items.select("item_id"), "item_id", "left_anti"
    )
    assert orphan_items.count() == 0


def test_no_duplicate_orders(spark):
    o = load_clean_table(spark, "Orders", PROCESSED_DIR)
    assert o.count() == o.select("order_id").distinct().count()


def test_no_duplicate_order_items(spark):
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    assert oi.count() == oi.select("order_id", "item_id").distinct().count()


def test_units_normalized(spark):
    for table in ("Inventory", "Wastage"):
        df = load_clean_table(spark, table, PROCESSED_DIR)
        bad = df.filter(~F.col("unit").isin(UNIT_CANON))
        assert bad.count() == 0, f"{table} has un-normalized unit values"


def test_no_wastage_exceeding_prepared(spark):
    w = load_clean_table(spark, "Wastage", PROCESSED_DIR)
    inv = load_clean_table(spark, "Inventory", PROCESSED_DIR).select(
        "item_id", "location_id", "date", "prepared_quantity", "consumed_stock"
    )
    joined = w.join(inv, ["item_id", "location_id", "date"], "inner")
    violations = joined.filter(F.col("quantity_wasted") > (F.col("prepared_quantity") - F.col("consumed_stock")))
    assert violations.count() == 0

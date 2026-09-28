"""Checks the integrated fact table written by spark_jobs/03_integrate_and_store.py.
Requires that job to have been run first.
"""
from schemas import load_clean_table
from spark_utils import PARQUET_DIR, PROCESSED_DIR


def test_fact_table_row_count_matches_cleaned_order_items(spark):
    fact = spark.read.parquet(str(PARQUET_DIR / "fact_order_line"))
    oi = load_clean_table(spark, "Order_Items", PROCESSED_DIR)
    assert fact.count() == oi.count()


def test_fact_table_has_expected_grain_columns(spark):
    fact = spark.read.parquet(str(PARQUET_DIR / "fact_order_line"))
    expected = {
        "order_item_id", "order_id", "item_id", "quantity", "unit_price", "line_total",
        "customer_id", "location_id", "item_name", "category_name", "order_year", "order_month",
    }
    assert expected.issubset(set(fact.columns))


def test_fact_table_order_item_id_is_unique(spark):
    fact = spark.read.parquet(str(PARQUET_DIR / "fact_order_line"))
    assert fact.count() == fact.select("order_item_id").distinct().count()

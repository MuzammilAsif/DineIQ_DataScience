"""Explicit StructType schemas for the 13 raw tables, per documentation/data_dictionary.md.

Date/timestamp columns are typed as DateType/TimestampType so that a value which
doesn't match the declared format (dateFormat "yyyy-MM-dd", timestampFormat
"yyyy-MM-dd HH:mm:ss") is nulled out by the CSV reader (PERMISSIVE mode) instead of
silently kept as a string. That's the mechanism 01_ingest_and_validate.py uses to
demonstrate data-type validation.
"""
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DATE_FMT = "yyyy-MM-dd"
TIMESTAMP_FMT = "yyyy-MM-dd HH:mm:ss"
MONEY = DecimalType(12, 2)
QTY = DecimalType(12, 3)

SCHEMAS = {
    "Restaurants": StructType(
        [
            StructField("location_id", StringType(), False),
            StructField("name", StringType(), False),
            StructField("city", StringType(), False),
            StructField("region", StringType(), False),
            StructField("address", StringType(), False),
            StructField("location_type", StringType(), False),
            StructField("seating_capacity", IntegerType(), False),
            StructField("opening_date", DateType(), False),
            StructField("latitude", DecimalType(9, 6), False),
            StructField("longitude", DecimalType(9, 6), False),
        ]
    ),
    "Menu_Categories": StructType(
        [
            StructField("category_id", StringType(), False),
            StructField("category_name", StringType(), False),
            StructField("description", StringType(), False),
        ]
    ),
    "Menu_Items": StructType(
        [
            StructField("item_id", StringType(), False),
            StructField("item_name", StringType(), False),
            StructField("category_id", StringType(), False),
            StructField("base_price", MONEY, False),
            StructField("base_cost", MONEY, False),
            StructField("description", StringType(), False),
            StructField("is_active", BooleanType(), False),
            StructField("launch_date", DateType(), False),
            StructField("is_seasonal", BooleanType(), False),
            StructField("season", StringType(), False),
        ]
    ),
    "Pricing_History": StructType(
        [
            StructField("price_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("location_id", StringType(), True),
            StructField("price", MONEY, False),
            StructField("cost", MONEY, False),
            StructField("effective_start_date", DateType(), False),
            StructField("effective_end_date", DateType(), True),
            StructField("change_reason", StringType(), False),
        ]
    ),
    "Customers": StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("signup_date", DateType(), False),
            StructField("age_group", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("home_location_id", StringType(), True),
            StructField("preferred_channel", StringType(), False),
            StructField("loyalty_member", BooleanType(), False),
        ]
    ),
    "Promotions": StructType(
        [
            StructField("promotion_id", StringType(), False),
            StructField("promotion_name", StringType(), False),
            StructField("promotion_type", StringType(), False),
            StructField("discount_value", MONEY, False),
            StructField("start_date", DateType(), False),
            StructField("end_date", DateType(), False),
            StructField("min_order_value", MONEY, True),
        ]
    ),
    "Promotion_Items": StructType(
        [
            StructField("promotion_id", StringType(), False),
            StructField("item_id", StringType(), False),
        ]
    ),
    "Promotion_Locations": StructType(
        [
            StructField("promotion_id", StringType(), False),
            StructField("location_id", StringType(), False),
        ]
    ),
    "Orders": StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("location_id", StringType(), False),
            StructField("order_datetime", TimestampType(), False),
            StructField("order_channel", StringType(), False),
            StructField("order_status", StringType(), False),
            StructField("promotion_id", StringType(), True),
            StructField("subtotal", MONEY, False),
            StructField("discount_amount", MONEY, False),
            StructField("tax_amount", MONEY, False),
            StructField("total_amount", MONEY, False),
            StructField("payment_method", StringType(), False),
        ]
    ),
    "Order_Items": StructType(
        [
            StructField("order_item_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("item_id", StringType(), True),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", MONEY, False),
            StructField("unit_cost", MONEY, False),
            StructField("discount_amount", MONEY, False),
            StructField("line_total", MONEY, False),
        ]
    ),
    "Ratings": StructType(
        [
            StructField("rating_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("item_id", StringType(), True),
            StructField("location_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("rating_value", IntegerType(), True),
            StructField("rating_date", DateType(), False),
            StructField("review_text", StringType(), True),
        ]
    ),
    "Inventory": StructType(
        [
            StructField("inventory_id", StringType(), False),
            StructField("item_id", StringType(), False),
            StructField("location_id", StringType(), False),
            StructField("date", DateType(), False),
            StructField("opening_stock", QTY, False),
            StructField("received_stock", QTY, False),
            StructField("prepared_quantity", QTY, False),
            StructField("consumed_stock", QTY, False),
            StructField("closing_stock", QTY, False),
            StructField("unit", StringType(), False),
        ]
    ),
    "Wastage": StructType(
        [
            StructField("wastage_id", StringType(), False),
            StructField("item_id", StringType(), True),
            StructField("location_id", StringType(), False),
            StructField("date", DateType(), False),
            StructField("quantity_wasted", QTY, False),
            StructField("unit", StringType(), False),
            StructField("cost_of_waste", MONEY, False),
            StructField("reason", StringType(), False),
        ]
    ),
}

# Table -> raw CSV filename (Step 1 wrote these with the exact table names).
FILES = {name: f"{name}.csv" for name in SCHEMAS}


def string_schema(struct_type: StructType) -> StructType:
    """Same field names/nullability, every field as StringType. Used to read a raw,
    un-cast copy of a table for the type-validation diff."""
    return StructType(
        [StructField(f.name, StringType(), True) for f in struct_type.fields]
    )


def load_table(spark, name: str, raw_dir):
    path = str(raw_dir / FILES[name])
    return (
        spark.read.option("header", True)
        .option("dateFormat", DATE_FMT)
        .option("timestampFormat", TIMESTAMP_FMT)
        .schema(SCHEMAS[name])
        .csv(path)
    )


def load_table_raw_strings(spark, name: str, raw_dir):
    path = str(raw_dir / FILES[name])
    return (
        spark.read.option("header", True)
        .schema(string_schema(SCHEMAS[name]))
        .csv(path)
    )


def load_clean_table(spark, name: str, processed_dir):
    """Cleaned tables keep the same columns/types as the raw table (quarantine_reason
    and any join-only helper columns are dropped before 02_clean.py writes them)."""
    path = str(processed_dir / f"{name}.csv")
    return (
        spark.read.option("header", True)
        .option("dateFormat", DATE_FMT)
        .option("timestampFormat", TIMESTAMP_FMT)
        .schema(SCHEMAS[name])
        .csv(path)
    )

"""Step 3 — feature engineering (SRS Step 7).

Every feature function takes an as_of_date and only uses records dated on or before
it (orders, ratings, wastage, pricing). Recency and rating trends are measured
relative to as_of_date, not the dataset's max date. Later steps (forecasting, time-aware
validation) call these same functions with their own cutoffs.

Outputs:
  parquet_data/features/{item,customer,location}_features/as_of_date=<date>/
  parquet_data/fact_order_line/ rewritten with an order-level basket_size column

Run: .venv/bin/python spark_jobs/04_feature_engineering.py
"""
import datetime as dt
import shutil
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, get_spark  # noqa: E402

FEATURES_DIR = PARQUET_DIR / "features"

# Half-open [start, end) hour windows, matching the generator's HOUR_BASE peaks
# (12-13 and 19-21). Hours 14 and 22 are shoulder hours and are not counted as peak.
PEAK_HOURS = [(12, 14), (19, 22)]
# Spark dayofweek: 1=Sunday ... 7=Saturday. Weekend is Fri-Sat-Sun.
WEEKEND_DAYS = [6, 7, 1]
CANCELLED = "Cancelled"
MIN_TREND_MONTHS = 3


def _as_date(as_of_date):
    if isinstance(as_of_date, str):
        return dt.date.fromisoformat(as_of_date)
    if isinstance(as_of_date, dt.datetime):
        return as_of_date.date()
    return as_of_date


def filter_timestamp_as_of(df, as_of_date, col="order_datetime"):
    # as_of_date is a calendar day, so the whole day is included
    next_day = _as_date(as_of_date) + dt.timedelta(days=1)
    return df.filter(F.col(col) < F.lit(next_day).cast("timestamp"))


def filter_date_as_of(df, as_of_date, col):
    return df.filter(F.col(col) <= F.lit(_as_date(as_of_date)))


def safe_div(num, den):
    return F.when(den.isNotNull() & (den != 0), num / den)


def is_peak_hour(ts_col):
    h = F.hour(ts_col)
    cond = F.lit(False)
    for start, end in PEAK_HOURS:
        cond = cond | ((h >= start) & (h < end))
    return cond


def is_weekend(ts_col):
    return F.dayofweek(ts_col).isin(WEEKEND_DAYS)


def monthly_slope(df, key_cols, date_col, value_col, out):
    """Least-squares slope of value_col over calendar months, null under MIN_TREND_MONTHS."""
    return df.groupBy(*key_cols).agg(
        F.when(
            F.count("*") >= MIN_TREND_MONTHS,
            safe_div(F.covar_pop(F.year(date_col) * 12 + F.month(date_col), value_col),
                     F.var_pop(F.year(date_col) * 12 + F.month(date_col))),
        ).alias(out)
    )


def add_basket_size(fact):
    """Order-level basket_size (count of order lines), attached to every fact row."""
    fact = fact.drop("basket_size") if "basket_size" in fact.columns else fact
    return fact.withColumn("basket_size", F.count(F.lit(1)).over(Window.partitionBy("order_id")))


def completed_lines(fact, as_of_date):
    """Non-cancelled order lines up to as_of_date, money columns as double."""
    return (
        filter_timestamp_as_of(fact, as_of_date)
        .filter(F.col("order_status") != CANCELLED)
        .withColumn("line_total", F.col("line_total").cast("double"))
        .withColumn("line_discount_amount", F.col("line_discount_amount").cast("double"))
        .withColumn("line_cost", F.col("quantity") * F.col("unit_cost").cast("double"))
    )


def item_features(fact, menu_items, menu_categories, ratings, wastage, pricing_history, as_of_date):
    lines = completed_lines(fact, as_of_date).filter(F.col("item_id").isNotNull())

    sales = lines.groupBy("item_id").agg(
        F.sum("line_total").alias("item_revenue"),
        F.sum("line_cost").alias("item_cost"),
        F.sum("quantity").alias("total_quantity_sold"),
        F.sum(F.when(is_weekend(F.col("order_datetime")), F.col("quantity")).otherwise(0))
        .alias("weekend_quantity"),
        F.countDistinct("order_id").alias("order_frequency"),
        F.sum(F.when(F.col("promotion_id").isNotNull(), F.col("line_total")).otherwise(0.0))
        .alias("promo_revenue"),
        F.sum("line_discount_amount").alias("discount_total"),
    )

    repeat = (
        lines.filter(F.col("customer_id").isNotNull())
        .groupBy("item_id", "customer_id")
        .agg(F.countDistinct("order_id").alias("n_orders"))
        .groupBy("item_id")
        .agg((F.sum(F.when(F.col("n_orders") > 1, 1).otherwise(0)) / F.count("*"))
             .alias("repeat_purchase_rate"))
    )

    rated = filter_date_as_of(ratings, as_of_date, "rating_date").filter(
        F.col("item_id").isNotNull() & F.col("rating_value").isNotNull()
    )
    avg_rating = rated.groupBy("item_id").agg(F.avg("rating_value").alias("average_rating"))
    # slope on the monthly means, in rating points per month
    trend = monthly_slope(
        rated.groupBy("item_id", F.trunc("rating_date", "month").alias("month"))
        .agg(F.avg("rating_value").alias("v")),
        ["item_id"], "month", "v", "rating_trend")

    # slope on monthly quantity sums, in portions per month
    sales_trend = monthly_slope(
        lines.groupBy("item_id", F.trunc(F.to_date("order_datetime"), "month").alias("month"))
        .agg(F.sum("quantity").alias("v")),
        ["item_id"], "month", "v", "sales_trend")

    waste = (
        filter_date_as_of(wastage, as_of_date, "date")
        .filter(F.col("item_id").isNotNull())
        .groupBy("item_id")
        .agg(F.sum(F.col("cost_of_waste").cast("double")).alias("wastage_cost"))
    )

    chain_prices = filter_date_as_of(pricing_history, as_of_date, "effective_start_date").filter(
        F.col("location_id").isNull()
    )
    w_first = Window.partitionBy("item_id").orderBy(F.col("effective_start_date").asc())
    w_last = Window.partitionBy("item_id").orderBy(F.col("effective_start_date").desc())
    prices = (
        chain_prices.withColumn("price", F.col("price").cast("double"))
        .withColumn("rn_first", F.row_number().over(w_first))
        .withColumn("rn_last", F.row_number().over(w_last))
        .groupBy("item_id")
        .agg(
            F.max(F.when(F.col("rn_first") == 1, F.col("price"))).alias("first_price"),
            F.max(F.when(F.col("rn_last") == 1, F.col("price"))).alias("latest_price"),
        )
        .select(
            "item_id",
            (safe_div(F.col("latest_price") - F.col("first_price"), F.col("first_price")) * 100)
            .alias("price_change_percentage"),
        )
    )

    items = (
        filter_date_as_of(menu_items, as_of_date, "launch_date")
        .join(menu_categories.select("category_id", "category_name"), "category_id", "left")
        .select("item_id", "item_name", "category_id", "category_name",
                F.col("base_cost").cast("double").alias("base_cost"))
    )

    df = (
        items.join(sales, "item_id", "left")
        .join(repeat, "item_id", "left")
        .join(avg_rating, "item_id", "left")
        .join(trend, "item_id", "left")
        .join(sales_trend, "item_id", "left")
        .join(waste, "item_id", "left")
        .join(prices, "item_id", "left")
        .fillna(0, subset=["item_revenue", "item_cost", "total_quantity_sold", "order_frequency",
                           "weekend_quantity", "promo_revenue", "discount_total", "wastage_cost"])
    )

    # Wastage is logged in kg/liters/pcs while sales are in portions. cost_of_waste is
    # portions * unit cost, so dividing by the item's average unit cost converts it back
    # to portions. Items with no sales yet fall back to the menu base_cost.
    avg_unit_cost = F.coalesce(safe_div(F.col("item_cost"), F.col("total_quantity_sold")),
                               F.col("base_cost"))
    wasted = safe_div(F.col("wastage_cost"), avg_unit_cost)

    return df.select(
        "item_id", "item_name", "category_id", "category_name",
        "item_revenue",
        "item_cost",
        (F.col("item_revenue") - F.col("item_cost")).alias("contribution_margin"),
        (safe_div(F.col("item_revenue") - F.col("item_cost"), F.col("item_revenue")) * 100)
        .alias("profit_percentage"),
        F.col("total_quantity_sold").cast("long").alias("total_quantity_sold"),
        F.percent_rank().over(Window.orderBy("total_quantity_sold")).alias("item_popularity"),
        F.col("order_frequency").cast("long").alias("order_frequency"),
        "repeat_purchase_rate",
        "average_rating",
        "rating_trend",
        safe_div(F.col("weekend_quantity"), F.col("total_quantity_sold")).alias("weekend_order_ratio"),
        "sales_trend",
        "wastage_cost",
        wasted.alias("total_quantity_wasted"),
        safe_div(wasted, wasted + F.col("total_quantity_sold")).alias("wastage_percentage"),
        safe_div(F.col("promo_revenue"), F.col("item_revenue")).alias("promotion_dependency"),
        safe_div(F.col("discount_total"), F.col("item_revenue") + F.col("discount_total"))
        .alias("discount_percentage"),
        "price_change_percentage",
    )


def item_location_features(fact, ratings, wastage, as_of_date):
    """The item metrics menu classification needs, at item x location grain.

    Only (item, location) pairs with at least one completed sale are returned.
    """
    keys = ["item_id", "location_id"]
    lines = completed_lines(fact, as_of_date).filter(F.col("item_id").isNotNull())

    sales = lines.groupBy(*keys).agg(
        F.sum("line_total").alias("item_revenue"),
        F.sum("line_cost").alias("item_cost"),
        F.sum("quantity").alias("total_quantity_sold"),
    )
    repeat = (
        lines.filter(F.col("customer_id").isNotNull())
        .groupBy(*keys, "customer_id")
        .agg(F.countDistinct("order_id").alias("n_orders"))
        .groupBy(*keys)
        .agg((F.sum(F.when(F.col("n_orders") > 1, 1).otherwise(0)) / F.count("*"))
             .alias("repeat_purchase_rate"))
    )
    avg_rating = (
        filter_date_as_of(ratings, as_of_date, "rating_date")
        .filter(F.col("item_id").isNotNull() & F.col("rating_value").isNotNull())
        .groupBy(*keys).agg(F.avg("rating_value").alias("average_rating"))
    )
    waste = (
        filter_date_as_of(wastage, as_of_date, "date")
        .filter(F.col("item_id").isNotNull())
        .groupBy(*keys)
        .agg(F.sum(F.col("cost_of_waste").cast("double")).alias("wastage_cost"))
    )

    df = (
        sales.join(repeat, keys, "left")
        .join(avg_rating, keys, "left")
        .join(waste, keys, "left")
        .fillna(0, subset=["wastage_cost"])
    )
    # same cost-to-portions conversion as item_features; every row here has sales
    wasted = safe_div(F.col("wastage_cost"), F.col("item_cost") / F.col("total_quantity_sold"))
    return df.select(
        *keys,
        (F.col("item_revenue") - F.col("item_cost")).alias("contribution_margin"),
        (safe_div(F.col("item_revenue") - F.col("item_cost"), F.col("item_revenue")) * 100)
        .alias("profit_percentage"),
        F.col("total_quantity_sold").cast("long").alias("total_quantity_sold"),
        "repeat_purchase_rate",
        "average_rating",
        safe_div(wasted, wasted + F.col("total_quantity_sold")).alias("wastage_percentage"),
    )


def order_level(orders, fact, as_of_date):
    """Non-cancelled orders up to as_of_date, one row per order, with basket_size."""
    baskets = fact.groupBy("order_id").agg(F.count(F.lit(1)).alias("basket_size"))
    return (
        filter_timestamp_as_of(orders, as_of_date)
        .filter(F.col("order_status") != CANCELLED)
        .join(baskets, "order_id", "left")
        .withColumn("total_amount", F.col("total_amount").cast("double"))
    )


def customer_features(orders, fact, as_of_date):
    o = order_level(orders, fact, as_of_date).filter(F.col("customer_id").isNotNull())

    base = o.groupBy("customer_id").agg(
        F.datediff(F.lit(_as_date(as_of_date)), F.to_date(F.max("order_datetime")))
        .alias("customer_recency"),
        F.countDistinct("order_id").alias("customer_frequency"),
        F.sum("total_amount").alias("customer_monetary_value"),
        F.avg(is_peak_hour(F.col("order_datetime")).cast("double")).alias("peak_hour_frequency"),
        F.avg(is_weekend(F.col("order_datetime")).cast("double")).alias("weekend_order_ratio"),
        F.avg("basket_size").alias("basket_size_avg"),
    )

    # ties go to the alphabetically first channel so reruns are deterministic
    w = Window.partitionBy("customer_id").orderBy(F.col("n").desc(), F.col("order_channel").asc())
    channel = (
        o.groupBy("customer_id", "order_channel").agg(F.count("*").alias("n"))
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("customer_id", F.col("order_channel").alias("channel_preference"))
    )

    return base.join(channel, "customer_id", "left").select(
        "customer_id",
        "customer_recency",
        F.col("customer_frequency").cast("long").alias("customer_frequency"),
        "customer_monetary_value",
        safe_div(F.col("customer_monetary_value"), F.col("customer_frequency"))
        .alias("average_order_value"),
        "peak_hour_frequency",
        "weekend_order_ratio",
        "channel_preference",
        "basket_size_avg",
    )


def location_features(fact, wastage, restaurants, as_of_date):
    lines = completed_lines(fact, as_of_date)

    sales = lines.groupBy("location_id").agg(
        F.sum("line_total").alias("location_revenue"),
        F.sum(F.col("line_total") - F.col("line_cost")).alias("location_profit"),
        F.countDistinct("order_id").alias("n_orders"),
        F.countDistinct("customer_id").alias("location_customer_count"),
    )

    repeat = (
        lines.filter(F.col("customer_id").isNotNull())
        .groupBy("location_id", "customer_id")
        .agg(F.countDistinct("order_id").alias("n_orders"))
        .groupBy("location_id")
        .agg((F.sum(F.when(F.col("n_orders") > 1, 1).otherwise(0)) / F.count("*"))
             .alias("location_repeat_customer_rate"))
    )

    waste = (
        filter_date_as_of(wastage, as_of_date, "date")
        .groupBy("location_id")
        .agg(F.sum(F.col("cost_of_waste").cast("double")).alias("wastage_cost"))
    )

    df = (
        restaurants.select("location_id", F.col("name").alias("location_name"), "city", "region",
                           "location_type")
        .join(sales, "location_id", "left")
        .join(repeat, "location_id", "left")
        .join(waste, "location_id", "left")
        .fillna(0, subset=["location_revenue", "location_profit", "n_orders",
                           "location_customer_count", "wastage_cost"])
    )

    return df.select(
        "location_id", "location_name", "city", "region", "location_type",
        "location_revenue",
        "location_profit",
        safe_div(F.col("location_revenue"), F.col("n_orders")).alias("location_avg_order_value"),
        F.col("location_customer_count").cast("long").alias("location_customer_count"),
        "location_repeat_customer_rate",
        "wastage_cost",
        safe_div(F.col("wastage_cost"), F.col("location_revenue")).alias("location_wastage_rate"),
    )


def build_all(spark, as_of_date, fact, orders, wastage, dims):
    return {
        "item_features": item_features(
            fact, dims["Menu_Items"], dims["Menu_Categories"], dims["Ratings"], wastage,
            dims["Pricing_History"], as_of_date),
        "customer_features": customer_features(orders, fact, as_of_date),
        "location_features": location_features(fact, wastage, dims["Restaurants"], as_of_date),
    }


def rewrite_fact_with_basket_size(spark, logger):
    path = PARQUET_DIR / "fact_order_line"
    tmp = PARQUET_DIR / "fact_order_line.tmp_basket"
    fact = add_basket_size(spark.read.parquet(str(path)))
    fact.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(str(tmp))
    shutil.rmtree(path)
    tmp.rename(path)
    logger.log(f"rewrote {path} with basket_size")


def main():
    logger = JobLogger("features")
    spark = get_spark("dineiq-features")
    spark.sparkContext.setLogLevel("WARN")

    with logger.timer("attach order-level basket_size to fact_order_line"):
        rewrite_fact_with_basket_size(spark, logger)

    with logger.timer("load fact, orders, wastage and dimension tables"):
        fact = spark.read.parquet(str(PARQUET_DIR / "fact_order_line")).cache()
        orders = spark.read.parquet(str(PARQUET_DIR / "orders")).cache()
        wastage = spark.read.parquet(str(PARQUET_DIR / "wastage")).cache()
        dims = {
            name: load_clean_table(spark, name, PROCESSED_DIR).cache()
            for name in ["Menu_Items", "Menu_Categories", "Ratings", "Pricing_History", "Restaurants"]
        }
        logger.log(f"fact_order_line: {fact.count()} rows, orders: {orders.count()} rows")

    max_date = fact.agg(F.max("order_datetime")).first()[0].date()
    earlier = (spark.createDataFrame([(max_date,)], ["d"])
               .select(F.add_months("d", -3)).first()[0])
    snapshots = [max_date, earlier]
    logger.log(f"snapshots: {[str(d) for d in snapshots]}")

    for as_of in snapshots:
        tables = build_all(spark, as_of, fact, orders, wastage, dims)
        for name, df in tables.items():
            with logger.timer(f"{name} as_of_date={as_of}"):
                out = FEATURES_DIR / name / f"as_of_date={as_of}"
                df.write.mode("overwrite").parquet(str(out))
                written = spark.read.parquet(str(out))
                logger.log(f"wrote {out} ({written.count()} rows)")

    logger.log("feature engineering job complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

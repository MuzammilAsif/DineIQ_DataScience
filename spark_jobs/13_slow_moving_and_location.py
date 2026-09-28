"""Slow-moving dish detection and multi-location intelligence (SRS Steps 32-34).

Reuses Step 4's classification (demand percentile, per-location categories) and Step 3's
location features.

Output: parquet_data/slow_moving/, parquet_data/location_intelligence/, report section 13.
Run: .venv/bin/python spark_jobs/13_slow_moving_and_location.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, get_spark  # noqa: E402


def slow_moving(classification, cfg):
    ranked = F.col("demand_percentile").isNotNull()
    months = F.least(F.lit(12.0), F.col("days_since_launch") / 30.44)
    df = (classification
          .withColumn("repeat_percentile",
                      F.when(ranked, F.percent_rank().over(
                          Window.partitionBy(ranked).orderBy("repeat_purchase_rate"))))
          .withColumn("relative_trend",
                      F.col("sales_trend") / (F.col("total_quantity_sold") / months)))
    return df.withColumn(
        "slow_moving",
        F.coalesce((F.col("demand_percentile") < cfg["demand_percentile_below"])
                   & (F.col("repeat_percentile") < cfg["repeat_percentile_below"])
                   & (F.col("relative_trend") <= cfg["flat_trend_max"]), F.lit(False)))


def location_summary(loc_features, ratings, by_location):
    rating = ratings.filter(F.col("rating_value").isNotNull()).groupBy("location_id").agg(
        F.avg("rating_value").alias("avg_rating"))
    cats = (by_location.groupBy("location_id").pivot(
        "category", ["Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer"])
        .count().fillna(0))
    mismatch = by_location.groupBy("location_id").agg(
        F.avg((F.col("category") != F.col("global_category")).cast("double"))
        .alias("category_mismatch_share"))
    return (loc_features.join(rating, "location_id", "left").join(cats, "location_id", "left")
            .join(mismatch, "location_id", "left"))


def report(slow, loc, as_of, cfg):
    s = slow[slow.slow_moving].sort_values("demand_percentile")
    cols = ["item_name", "category", "total_quantity_sold", "demand_percentile",
            "repeat_purchase_rate", "relative_trend", "average_rating", "profit_percentage"]
    lcols = ["location_name", "city", "location_type", "location_revenue", "location_profit",
             "location_avg_order_value", "location_repeat_customer_rate", "location_wastage_rate",
             "avg_rating", "Profit Driver", "Low Performer", "category_mismatch_share"]
    lfmt = {"location_revenue": "{:,.0f}", "location_profit": "{:,.0f}",
            "location_avg_order_value": "{:,.0f}", "location_repeat_customer_rate": "{:.1%}",
            "location_wastage_rate": "{:.1%}", "avg_rating": "{:.2f}",
            "category_mismatch_share": "{:.0%}"}
    loc = loc.sort_values("location_revenue", ascending=False)
    spread = loc.location_revenue.max() / loc.location_revenue.min()
    return "\n\n".join([
        "## 7. Slow-moving dishes and multi-location intelligence",
        f"Snapshot `as_of_date={as_of}`. An item is slow-moving when its Step 4 demand "
        f"percentile is below {cfg['demand_percentile_below']}, its repeat-purchase percentile "
        f"is below {cfg['repeat_percentile_below']}, and its sales trend is flat or declining: "
        f"a monthly slope at most {cfg['flat_trend_max']:.0%} of its average monthly quantity. "
        "New items (Insufficient History) have no demand percentile and are never flagged.",
        f"{len(s)} slow-moving items:",
        ac.md_table(s[cols], {"total_quantity_sold": "{:,}", "demand_percentile": "{:.2f}",
                              "repeat_purchase_rate": "{:.3f}", "relative_trend": "{:+.1%}",
                              "average_rating": "{:.2f}", "profit_percentage": "{:.1f}"})
        if len(s) else "None.",
        "**Location summary** (Step 3 location features; average rating from Ratings; category "
        "counts and mismatch share from Step 4's per-location classification, "
        "`parquet_data/menu_classification_by_location/`). Mismatch share is the fraction of "
        "items sold at a location whose category there differs from their chain-wide category.",
        ac.md_table(loc[lcols], lfmt),
        f"Revenue spread between the busiest and quietest location: {spread:.1f}x. "
        f"Highest wastage rate: {loc.loc[loc.location_wastage_rate.idxmax(), 'location_name']} "
        f"({loc.location_wastage_rate.max():.1%}). Lowest average rating: "
        f"{loc.loc[loc.avg_rating.idxmin(), 'location_name']} ({loc.avg_rating.min():.2f}).",
    ])


def main():
    cfg = ac.load_config()["slow_moving"]
    logger = JobLogger("slow_location")
    spark = get_spark("dineiq-slow-location")
    spark.sparkContext.setLogLevel("WARN")
    as_of = ac.latest_snapshot("location_features")
    classification = spark.read.parquet(
        str(PARQUET_DIR / "menu_classification" / f"as_of_date={as_of}"))
    by_location = spark.read.parquet(
        str(PARQUET_DIR / "menu_classification_by_location" / f"as_of_date={as_of}"))
    loc_features = spark.read.parquet(
        str(ac.FEATURES_DIR / "location_features" / f"as_of_date={as_of}"))
    ratings = load_clean_table(spark, "Ratings", PROCESSED_DIR).filter(
        F.col("rating_date") <= F.lit(as_of))

    with logger.timer("slow-moving and location summary"):
        slow = slow_moving(classification, cfg).select(
            "item_id", "item_name", "category", "total_quantity_sold", "demand_percentile",
            "repeat_purchase_rate", "repeat_percentile", "sales_trend", "relative_trend",
            "average_rating", "profit_percentage", "slow_moving")
        slow.write.mode("overwrite").parquet(ac.out_path("slow_moving"))
        loc = location_summary(loc_features, ratings, by_location)
        loc.write.mode("overwrite").parquet(ac.out_path("location_intelligence"))
    slow_pdf = spark.read.parquet(ac.out_path("slow_moving")).toPandas()
    loc_pdf = spark.read.parquet(ac.out_path("location_intelligence")).toPandas()
    logger.log(f"slow-moving items: {int(slow_pdf.slow_moving.sum())}")
    ac.write_section("13", report(slow_pdf, loc_pdf, as_of, cfg))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

"""Statistical rating and sales anomaly detection (SRS Steps 29-31).

Rolling z-scores against each series' own recent past (the current period excluded), plus
IQR outliers for single-order values and a duplicate-transaction check.

Output: parquet_data/anomalies/ (one row per flagged anomaly), report section 12.
Run: .venv/bin/python spark_jobs/12_anomaly_detection.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PROCESSED_DIR, get_spark  # noqa: E402

OUT_COLS = ["anomaly_type", "entity_id", "location_id", "period_start", "metric", "value",
            "baseline_mean", "baseline_std", "score"]


def rolling_z(df, keys, order_col, metrics, lower, upper, min_periods, z, anomaly_type):
    """Flags rows whose metric is more than z std devs from the mean of the prior window.

    The window covers order_col values in [current + lower, current + upper] with upper < 0,
    so the current value never sits in its own baseline.
    """
    w = Window.partitionBy(*keys).orderBy(order_col).rangeBetween(lower, upper)
    out = []
    for m in metrics:
        scored = (df.withColumn("baseline_mean", F.avg(m).over(w))
                  .withColumn("baseline_std", F.stddev(m).over(w))
                  .withColumn("n_base", F.count(m).over(w))
                  .withColumn("score", (F.col(m) - F.col("baseline_mean")) / F.col("baseline_std"))
                  .filter((F.col("n_base") >= min_periods) & (F.col("baseline_std") > 0)
                          & (F.abs("score") > z)))
        out.append(scored.select(
            F.lit(anomaly_type).alias("anomaly_type"), F.col(keys[0]).alias("entity_id"),
            (F.col("location_id") if "location_id" in df.columns else F.lit(None).cast("string"))
            .alias("location_id"),
            F.col("period_start"), F.lit(m).alias("metric"), F.col(m).cast("double").alias("value"),
            "baseline_mean", "baseline_std", "score"))
    result = out[0]
    for o in out[1:]:
        result = result.unionByName(o)
    return result


def day_num(col):
    return F.datediff(col, F.lit("2000-01-01"))


def rating_weeks(ratings):
    r = ratings.filter(F.col("item_id").isNotNull() & F.col("rating_value").isNotNull())
    r = r.withColumn("period_start", F.to_date(F.date_trunc("week", "rating_date")))
    per_value = r.groupBy("item_id", "period_start", "rating_value").count()
    top_share = per_value.groupBy("item_id", "period_start").agg(
        F.max("count").alias("top_count"),
        F.max_by("rating_value", "count").alias("top_value"))
    return (r.groupBy("item_id", "period_start")
            .agg(F.count("*").alias("rating_count"),
                 F.avg("rating_value").alias("avg_rating"))
            .join(top_share, ["item_id", "period_start"])
            .withColumn("identical_share", F.col("top_count") / F.col("rating_count"))
            .withColumn("week_num", (day_num("period_start") / 7).cast("int")))


def rating_anomalies(weeks, cfg):
    z = rolling_z(weeks, ["item_id"], "week_num", ["rating_count", "avg_rating"],
                  -cfg["baseline_weeks"], -1, cfg["min_baseline_weeks"], cfg["z_threshold"],
                  "rating_z")
    identical = (weeks.filter((F.col("rating_count") >= cfg["identical_min_ratings"])
                              & (F.col("identical_share") >= cfg["identical_share"]))
                 .select(F.lit("identical_ratings").alias("anomaly_type"),
                         F.col("item_id").alias("entity_id"),
                         F.lit(None).cast("string").alias("location_id"), "period_start",
                         F.concat(F.lit("share_of_rating_"), F.col("top_value").cast("string"))
                         .alias("metric"),
                         F.col("identical_share").alias("value"),
                         F.lit(None).cast("double").alias("baseline_mean"),
                         F.lit(None).cast("double").alias("baseline_std"),
                         F.col("rating_count").cast("double").alias("score")))
    return z.unionByName(identical)


def sales_anomalies(orders, fact, cfg):
    loc_daily = (orders.groupBy("location_id", F.to_date("order_datetime").alias("period_start"))
                 .agg(F.count("*").alias("daily_orders"),
                      F.sum("total_amount").cast("double").alias("daily_revenue"))
                 .withColumn("d", day_num("period_start")))
    item_daily = (fact.groupBy("item_id", F.to_date("order_datetime").alias("period_start"))
                  .agg(F.sum("quantity").alias("daily_quantity"),
                       F.sum("line_total").cast("double").alias("daily_revenue"))
                  .withColumn("d", day_num("period_start")))
    args = (-cfg["baseline_days"], -1, cfg["min_baseline_days"], cfg["z_threshold"])
    return (rolling_z(loc_daily, ["location_id"], "d", ["daily_orders", "daily_revenue"], *args,
                      "location_sales_z")
            .unionByName(rolling_z(item_daily, ["item_id"], "d",
                                   ["daily_quantity", "daily_revenue"], *args, "item_sales_z")))


def high_value_orders(orders, k):
    q1, q3 = orders.approxQuantile("total_amount", [0.25, 0.75], 0.001)
    cutoff = q3 + k * (q3 - q1)
    return cutoff, orders.filter(F.col("total_amount") > cutoff).select(
        F.lit("high_order_value").alias("anomaly_type"), F.col("order_id").alias("entity_id"),
        "location_id", F.to_date("order_datetime").alias("period_start"),
        F.lit("total_amount").alias("metric"), F.col("total_amount").cast("double").alias("value"),
        F.lit(None).cast("double").alias("baseline_mean"),
        F.lit(None).cast("double").alias("baseline_std"),
        (F.col("total_amount") / F.lit(cutoff)).cast("double").alias("score"))


def duplicate_transactions(fact):
    """Orders sharing customer, minute and the same item/quantity list."""
    per_order = (fact.groupBy("order_id", "customer_id", "location_id",
                              F.date_trunc("minute", "order_datetime").alias("minute"))
                 .agg(F.array_sort(F.collect_list(F.concat_ws("x", "item_id", "quantity")))
                      .alias("lines")))
    w = Window.partitionBy("customer_id", "minute", "lines")
    return (per_order.withColumn("n", F.count("*").over(w)).filter("n > 1")
            .select(F.lit("duplicate_transaction").alias("anomaly_type"),
                    F.col("order_id").alias("entity_id"), "location_id",
                    F.to_date("minute").alias("period_start"),
                    F.lit("same_customer_minute_items").alias("metric"),
                    F.col("n").cast("double").alias("value"),
                    F.lit(None).cast("double").alias("baseline_mean"),
                    F.lit(None).cast("double").alias("baseline_std"),
                    F.col("n").cast("double").alias("score")))


def report(pdf, names, cutoff, cfg, promos):
    counts = pdf.groupby(["anomaly_type", "metric"]).size().reset_index(name="flags")
    pdf = pdf.assign(entity=pdf.entity_id.map(lambda e: names.get(e, e)))
    cols = ["entity", "location_id", "period_start", "metric", "value", "baseline_mean", "score"]
    fmt = {"value": "{:,.2f}", "baseline_mean": "{:,.2f}", "score": "{:+.1f}"}

    def top(kind, n=8):
        d = pdf[pdf.anomaly_type == kind]
        return ac.md_table(d.reindex(d.score.abs().sort_values(ascending=False).index).head(n)[cols],
                           fmt) if len(d) else "None flagged."

    busiest = (pdf[pdf.anomaly_type.isin(["location_sales_z", "item_sales_z"])]
               .groupby("period_start").size().sort_values(ascending=False).head(10)
               .reset_index(name="flags"))
    busiest["promotions starting within 1 day"] = busiest.period_start.map(
        lambda d: ", ".join(f"{p.promotion_id} {p.promotion_name}" for p in promos.itertuples()
                            if abs((p.start_date - d).days) <= 1))
    lines = [
        "## 6. Rating and sales anomalies",
        f"Rolling z-scores, flagged when |z| > {cfg['z_threshold']}. The baseline is the series' "
        "own recent past, excluding the current period: the previous "
        f"{cfg['baseline_weeks']} weeks for weekly ratings per item (at least "
        f"{cfg['min_baseline_weeks']}), and the previous {cfg['baseline_days']} days for daily "
        f"sales per location and per item (at least {cfg['min_baseline_days']}). Identical "
        f"ratings: one rating value is at least {cfg['identical_share']:.0%} of an item's week, "
        f"with at least {cfg['identical_min_ratings']} ratings. High order value: "
        f"total_amount above Q3 + {cfg['order_value_iqr_k']} x IQR ({cutoff:,.0f}). Duplicate "
        "transactions: two or more orders with the same customer, minute and item/quantity "
        "list.",
        ac.md_table(counts),
        "**Rating anomalies (largest |z|)**", top("rating_z"),
        "**Identical-rating weeks**", top("identical_ratings"),
        "**Location daily sales anomalies**", top("location_sales_z"),
        "**Item daily sales anomalies**", top("item_sales_z"),
        "**Dates with the most sales anomalies**", ac.md_table(busiest) if len(busiest) else "None.",
        "The dates with the most flags are promotion launches (annotated above) and public "
        "holidays. In 2025, 14 Feb is Valentine's Day, 31 Mar is Eid ul Fitr and 14 Aug is "
        "Independence Day. These spikes are real demand changes, not data errors, so an "
        "operational alert on this output should exclude known event dates.",
        f"**Highest single-order values** (score = value / cutoff; "
        f"{int((pdf.anomaly_type == 'high_order_value').sum())} orders above the cutoff)",
        top("high_order_value", 5),
        "**Duplicate-looking transactions**", top("duplicate_transaction", 5),
    ]
    return "\n\n".join(lines)


def main():
    cfg = ac.load_config()["anomaly"]
    logger = JobLogger("anomaly")
    spark = get_spark("dineiq-anomaly")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    orders = ac.completed_orders(spark)
    ratings = load_clean_table(spark, "Ratings", PROCESSED_DIR)
    names = {r.item_id: r.item_name for r in
             fact.select("item_id", "item_name").distinct().collect()}

    with logger.timer("anomalies"):
        cutoff, high = high_value_orders(orders, cfg["order_value_iqr_k"])
        allx = (rating_anomalies(rating_weeks(ratings), cfg)
                .unionByName(sales_anomalies(orders, fact, cfg))
                .unionByName(high)
                .unionByName(duplicate_transactions(fact)))
        allx.select(*OUT_COLS).write.mode("overwrite").parquet(ac.out_path("anomalies"))
    pdf = spark.read.parquet(ac.out_path("anomalies")).toPandas()
    logger.log(pdf.groupby(["anomaly_type", "metric"]).size().to_string())
    promos = load_clean_table(spark, "Promotions", PROCESSED_DIR).select(
        "promotion_id", "promotion_name", "start_date").toPandas()
    ac.write_section("12", report(pdf, names, cutoff, cfg, promos))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

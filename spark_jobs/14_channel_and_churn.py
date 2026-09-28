"""Ordering-channel analysis and rule-based churn risk (SRS Steps 35-36).

Output: parquet_data/channel_analysis/, parquet_data/churn_risk/, report section 14.
Run: .venv/bin/python spark_jobs/14_channel_and_churn.py  (after 07 for the segment breakdown)
"""
import datetime as dt
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, get_spark  # noqa: E402

PEAK_HOURS = [(12, 14), (19, 22)]


def is_peak(ts):
    h = F.hour(ts)
    cond = F.lit(False)
    for lo, hi in PEAK_HOURS:
        cond = cond | ((h >= lo) & (h < hi))
    return cond


def channel_stats(all_orders, fact):
    margin = (fact.groupBy("order_id")
              .agg(F.sum(F.col("line_total") - F.col("unit_cost") * F.col("quantity"))
                   .cast("double").alias("margin"),
                   F.first("basket_size").alias("basket_size")))
    o = all_orders.join(margin, "order_id", "left")
    done = F.col("order_status") != ac.CANCELLED

    def over_done(expr):
        return F.avg(F.when(done, expr))

    by_channel = o.groupBy("order_channel").agg(
        F.sum(done.cast("int")).alias("orders"),
        F.avg((~done).cast("double")).alias("cancellation_rate"),
        over_done(F.col("basket_size")).alias("avg_basket_size"),
        over_done(F.col("total_amount").cast("double")).alias("avg_order_value"),
        over_done((F.col("discount_amount") > 0).cast("double")).alias("discount_order_share"),
        (F.sum(F.when(done, F.col("discount_amount"))) / F.sum(F.when(done, F.col("subtotal"))))
        .cast("double").alias("discount_rate"),
        (F.sum(F.when(done, F.col("margin"))) / F.sum(F.when(done, F.col("subtotal") - F.col("discount_amount"))))
        .cast("double").alias("margin_pct"),
        over_done(is_peak("order_datetime").cast("double")).alias("peak_hour_share"),
        over_done(F.dayofweek("order_datetime").isin(6, 7, 1).cast("double")).alias("weekend_share"))
    busiest = (o.filter(done).groupBy("order_channel", F.hour("order_datetime").alias("hour"))
               .count().groupBy("order_channel").agg(F.max_by("hour", "count").alias("busiest_hour")))
    total = F.sum("orders").over(Window.partitionBy())
    return by_channel.join(busiest, "order_channel").withColumn("order_share", F.col("orders") / total)


def churn_risk(orders, customer_features, as_of_date, cfg):
    as_of = F.lit(as_of_date)
    per_cust = (orders.filter(F.to_date("order_datetime") <= as_of)
                .groupBy("customer_id")
                .agg(F.count("*").alias("n"),
                     F.datediff(F.max("order_datetime"), F.min("order_datetime")).alias("span"),
                     F.sum(F.when(F.datediff(as_of, F.to_date("order_datetime")) < 91, 1)
                           .otherwise(0)).alias("orders_last_quarter"),
                     F.sum(F.when(F.datediff(as_of, F.to_date("order_datetime")).between(91, 181), 1)
                           .otherwise(0)).alias("orders_prev_quarter")))
    avg_gap = (per_cust.filter("n >= 2").select(F.avg(F.col("span") / (F.col("n") - 1)))
               .first()[0])
    threshold = cfg["gap_multiplier"] * avg_gap
    df = (customer_features.select("customer_id", "customer_recency", "customer_frequency",
                                   "customer_monetary_value")
          .join(per_cust.select("customer_id", "orders_last_quarter", "orders_prev_quarter"),
                "customer_id", "left")
          .fillna({"orders_last_quarter": 0, "orders_prev_quarter": 0})
          .withColumn("recency_threshold", F.lit(threshold))
          .withColumn("churn_risk",
                      (F.col("customer_recency") > threshold)
                      & (F.col("orders_last_quarter") < F.col("orders_prev_quarter"))))
    return df, avg_gap, threshold


def report(ch, churn_summary, by_segment, avg_gap, threshold, n_customers, as_of):
    fmt = {"orders": "{:,}", "order_share": "{:.1%}", "cancellation_rate": "{:.1%}",
           "avg_basket_size": "{:.2f}", "avg_order_value": "{:,.0f}",
           "discount_order_share": "{:.1%}", "discount_rate": "{:.1%}", "margin_pct": "{:.1%}",
           "peak_hour_share": "{:.1%}", "weekend_share": "{:.1%}"}
    cols = ["order_channel", "orders", "order_share", "avg_basket_size", "avg_order_value",
            "discount_order_share", "discount_rate", "margin_pct", "cancellation_rate",
            "peak_hour_share", "weekend_share", "busiest_hour"]
    ch = ch.sort_values("orders", ascending=False)
    top_m, low_m = ch.loc[ch.margin_pct.idxmax()], ch.loc[ch.margin_pct.idxmin()]
    top_c = ch.loc[ch.cancellation_rate.idxmax()]
    lines = [
        "## 8. Ordering channels and churn risk",
        "Completed orders per channel, except cancellation rate, which uses all orders. Margin % "
        "is (line revenue - line cost) over net sales (subtotal - discount). Peak hours are "
        "12:00-14:00 and 19:00-22:00, and the weekend is Fri-Sun, as in Step 3.",
        ac.md_table(ch[cols], fmt),
        (f"Margin is within {(top_m.margin_pct - low_m.margin_pct) * 100:.1f} points across "
         "channels, so no channel is meaningfully more profitable per sale; the differences are "
         "in basket size, order value and cancellations. "
         if top_m.margin_pct - low_m.margin_pct < 0.01 else
         f"Highest margin: {top_m.order_channel} ({top_m.margin_pct:.1%}). Lowest: "
         f"{low_m.order_channel} ({low_m.margin_pct:.1%}). ")
        + f"Highest cancellation rate: {top_c.order_channel} ({top_c.cancellation_rate:.1%}). "
        f"Largest baskets and order values: {ch.loc[ch.avg_order_value.idxmax(), 'order_channel']}.",
        "### Churn risk",
        f"Rule-based, as of {as_of}. The average gap between a customer's orders (customers "
        f"with 2+ orders) is {avg_gap:.1f} days. A customer is at risk when recency exceeds "
        f"{threshold:.0f} days (the multiplier in config times that gap) and they placed fewer "
        "orders in the last 90 days than in the 90 days before.",
        f"{int(churn_summary):,} of {n_customers:,} customers ({churn_summary / n_customers:.1%}) "
        "are at risk.",
    ]
    if by_segment is not None:
        lines += ["At-risk customers by segment (module 07):",
                  ac.md_table(by_segment, {"customers": "{:,}", "at_risk": "{:,}",
                                           "at_risk_share": "{:.1%}"})]
    lines.append("Customers who have already lapsed completely (no orders in either quarter) are "
                 "not flagged, because the rule needs a decline between the two quarters. They "
                 "show up in the At-Risk segment and in low recency scores instead.")
    return "\n\n".join(lines)


def main():
    cfg = ac.load_config()["churn"]
    logger = JobLogger("channel_churn")
    spark = get_spark("dineiq-channel-churn")
    spark.sparkContext.setLogLevel("WARN")
    all_orders = spark.read.parquet(str(PARQUET_DIR / "orders"))
    fact = ac.completed_fact(spark)
    as_of = ac.latest_snapshot("customer_features")
    cf = spark.read.parquet(str(ac.FEATURES_DIR / "customer_features" / f"as_of_date={as_of}"))

    with logger.timer("channels"):
        ch = channel_stats(all_orders, fact)
        ch.write.mode("overwrite").parquet(ac.out_path("channel_analysis"))
    with logger.timer("churn"):
        churn, avg_gap, threshold = churn_risk(ac.completed_orders(spark), cf,
                                               dt.date.fromisoformat(as_of), cfg)
        churn.write.mode("overwrite").parquet(ac.out_path("churn_risk"))
    churn = spark.read.parquet(ac.out_path("churn_risk"))
    n_customers, n_risk = churn.count(), churn.filter("churn_risk").count()
    by_segment = None
    seg_path = PARQUET_DIR / "customer_segments"
    if seg_path.exists():
        by_segment = (churn.join(spark.read.parquet(str(seg_path)).select("customer_id", "segment"),
                                 "customer_id")
                      .groupBy("segment").agg(F.count("*").alias("customers"),
                                              F.sum(F.col("churn_risk").cast("int")).alias("at_risk"))
                      .withColumn("at_risk_share", F.col("at_risk") / F.col("customers"))
                      .orderBy(F.desc("at_risk_share")).toPandas())
    logger.log(f"avg_gap={avg_gap:.2f} threshold={threshold:.1f} at_risk={n_risk}/{n_customers}")
    ac.write_section("14", report(spark.read.parquet(ac.out_path("channel_analysis")).toPandas(),
                                  n_risk, by_segment, avg_gap, threshold, n_customers, as_of))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

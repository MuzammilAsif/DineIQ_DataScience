"""Promotion effectiveness and promotion-trap detection (SRS Steps 27-28).

Each promotion is compared on its own items and locations across three equal-length windows:
the same number of days immediately before, the promotion itself, and immediately after.

Output: parquet_data/promotion_effectiveness/, report section 11.
Run: .venv/bin/python spark_jobs/11_promotion_analysis.py
"""
import sys
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, get_spark  # noqa: E402

FLAGS = ["volume_up_margin_down", "customers_up_margin_per_order_down", "wastage_up",
         "post_promo_drop"]


def scope(promotions, promo_items, promo_locations):
    return (promotions.select("promotion_id", "promotion_name", "promotion_type", "start_date",
                              "end_date",
                              (F.datediff("end_date", "start_date") + 1).alias("days"))
            .join(promo_items, "promotion_id").join(promo_locations, "promotion_id"))


def _period(date_col):
    offset = F.datediff(date_col, "start_date")
    return (F.when((offset >= -F.col("days")) & (offset < 0), "pre")
            .when((offset >= 0) & (offset < F.col("days")), "during")
            .when((offset >= F.col("days")) & (offset < 2 * F.col("days")), "post"))


def window_metrics(fact, wastage, sc):
    lines = (fact.drop("promotion_id").join(sc, ["item_id", "location_id"])
             .withColumn("period", _period(F.to_date("order_datetime")))
             .filter(F.col("period").isNotNull())
             .groupBy("promotion_id", "period")
             .agg(F.countDistinct("order_id").alias("orders"),
                  F.sum("quantity").alias("quantity"),
                  F.sum("line_total").cast("double").alias("revenue"),
                  F.sum(F.col("line_total") - F.col("unit_cost") * F.col("quantity"))
                  .cast("double").alias("margin"),
                  F.countDistinct("customer_id").alias("customers")))
    waste = (wastage.join(sc, ["item_id", "location_id"])
             .withColumn("period", _period(F.col("date")))
             .filter(F.col("period").isNotNull())
             .groupBy("promotion_id", "period")
             .agg(F.sum("cost_of_waste").cast("double").alias("wastage_cost")))
    return (lines.join(waste, ["promotion_id", "period"], "left")
            .fillna({"wastage_cost": 0.0})
            .withColumn("revenue_per_order", F.col("revenue") / F.col("orders"))
            .withColumn("margin_per_order", F.col("margin") / F.col("orders"))
            .withColumn("margin_pct", F.col("margin") / F.col("revenue")))


def compare(metrics, promotions, data_start, data_end, cfg):
    wide = metrics.groupBy("promotion_id").pivot("period", ["pre", "during", "post"]).agg(
        *[F.first(c).alias(c) for c in ["orders", "quantity", "revenue", "margin", "customers",
                                        "wastage_cost", "revenue_per_order", "margin_per_order",
                                        "margin_pct"]])
    p = promotions.select("promotion_id", "promotion_name", "promotion_type", "start_date",
                          "end_date", (F.datediff("end_date", "start_date") + 1).alias("days"))
    df = p.join(wide, "promotion_id", "left")
    pre_ok = F.date_sub("start_date", F.col("days")) >= F.lit(data_start)
    post_ok = F.date_add("end_date", F.col("days")) <= F.lit(data_end)

    def flag(cond, ok):
        return F.when(ok, F.coalesce(cond, F.lit(False)))

    df = (df.withColumn("pre_window_in_data", pre_ok)
          .withColumn("post_window_in_data", post_ok)
          .withColumn("volume_up_margin_down", flag(
              ((F.col("during_revenue") > F.col("pre_revenue"))
               | (F.col("during_orders") > F.col("pre_orders")))
              & (F.col("during_margin") < F.col("pre_margin")), pre_ok))
          .withColumn("customers_up_margin_per_order_down", flag(
              (F.col("during_customers") > F.col("pre_customers"))
              & (F.col("during_margin_per_order")
                 < F.col("pre_margin_per_order") * (1 - cfg["margin_per_order_drop"])), pre_ok))
          .withColumn("wastage_up", flag(
              F.col("during_wastage_cost") > F.col("pre_wastage_cost") * (1 + cfg["wastage_rise"]),
              pre_ok))
          .withColumn("post_promo_drop", flag(
              F.col("post_quantity") < F.col("pre_quantity") * (1 - cfg["post_drop"]),
              pre_ok & post_ok)))
    n_flags = sum(F.coalesce(F.col(f).cast("int"), F.lit(0)) for f in FLAGS)
    return (df.withColumn("trap_flags", n_flags)
            .withColumn("is_promotion_trap", F.col("trap_flags") > 0)
            .orderBy("start_date"))


def _chg(a, b):
    return (a - b) / b if b else float("nan")


def report(pdf, cfg):
    rows = []
    for _, r in pdf.iterrows():
        if not r.pre_window_in_data:
            rows.append({"promotion": f"{r.promotion_id} {r.promotion_name}",
                         "type": r.promotion_type, "days": int(r.days),
                         "flags": "not evaluated: pre window starts before the data"})
            continue
        rows.append({
            "promotion": f"{r.promotion_id} {r.promotion_name}",
            "type": r.promotion_type, "days": int(r.days),
            "orders": _chg(r.during_orders, r.pre_orders),
            "revenue": _chg(r.during_revenue, r.pre_revenue),
            "margin": _chg(r.during_margin, r.pre_margin),
            "customers": _chg(r.during_customers, r.pre_customers),
            "margin/order": _chg(r.during_margin_per_order, r.pre_margin_per_order),
            "wastage": _chg(r.during_wastage_cost, r.pre_wastage_cost),
            "post qty": _chg(r.post_quantity, r.pre_quantity),
            "flags": ", ".join(f for f in FLAGS if r[f] is True) or "-",
        })
    pct = {k: "{:+.0%}" for k in ["orders", "revenue", "margin", "customers", "margin/order",
                                  "wastage", "post qty"]}
    lines = [
        "## 5. Promotion effectiveness and promotion traps",
        "Each promotion is measured on its own items at its own locations (all orders, not only "
        "those that used the code) over three windows of equal length: the days immediately "
        "before, the promotion, and the days immediately after. Values below are changes during "
        "the promotion against the pre window. `post qty` compares the post window with the pre "
        "window.",
        "A promotion is flagged as a trap if any of these hold: revenue or orders up but total "
        f"margin down; customers up but margin per order down by more than "
        f"{cfg['margin_per_order_drop']:.0%}; wastage cost up by more than "
        f"{cfg['wastage_rise']:.0%}; post-promotion quantity more than {cfg['post_drop']:.0%} "
        "below the pre-promotion baseline.",
        ac.md_table(pd.DataFrame(rows), pct),
    ]
    traps = pdf[pdf.trap_flags > 0].assign(
        growth=lambda d: d.during_orders / d.pre_orders).sort_values(
        ["trap_flags", "growth"], ascending=False)
    margin_flags = [f for f in FLAGS if f != "wastage_up"]
    wastage_only = traps[~traps[margin_flags].fillna(False).astype(bool).any(axis=1)]
    strong = traps[traps[margin_flags].fillna(False).astype(bool).any(axis=1)]
    lines.append(
        f"{len(traps)} of {len(pdf)} promotions are flagged. {len(wastage_only)} of them are "
        "flagged only for wastage: kitchens over-prepare for every promotion, so on its own "
        "that flag is a weak signal. The ones that also hurt margin or post-promotion demand "
        "are the real traps: "
        + (", ".join(f"{r.promotion_id} {r.promotion_name}" for _, r in strong.iterrows())
           or "none") + ".")
    examples = []
    if len(traps):
        examples.append(traps.iloc[0])
        worst_waste = strong.assign(w=strong.during_wastage_cost / strong.pre_wastage_cost) \
            .sort_values("w", ascending=False)
        if len(worst_waste) and worst_waste.iloc[0].promotion_id != traps.iloc[0].promotion_id:
            examples.append(worst_waste.iloc[0])
    for i, t in enumerate(examples):
        label = "Most-flagged" if i == 0 else "Largest wastage rise among margin traps"
        lines.append(
            f"**{label}: {t.promotion_id} {t.promotion_name}** ({t.start_date} to {t.end_date}). "
            f"Orders on its items went from {t.pre_orders:,.0f} to {t.during_orders:,.0f} and "
            f"customers from {t.pre_customers:,.0f} to {t.during_customers:,.0f}. Margin rate "
            f"went from {t.pre_margin_pct:.1%} to {t.during_margin_pct:.1%}, margin per order "
            f"from {t.pre_margin_per_order:,.0f} to {t.during_margin_per_order:,.0f}, and "
            f"wastage cost went from {t.pre_wastage_cost:,.0f} to {t.during_wastage_cost:,.0f}. "
            f"Flags: {', '.join(f for f in FLAGS if t[f] is True) or 'none'}.")
    lines.append(
        "Caveats: promotions overlap (e.g. PROMO009 runs June to September), so a pre window can "
        "include another promotion. The windows also don't control for seasonality. Promotions "
        "whose pre window starts before 1 January 2025 can't be evaluated, and neither can the "
        "post window of promotions ending in the last weeks of December.")
    return "\n\n".join(lines)


def main():
    cfg = ac.load_config()["promotion"]
    logger = JobLogger("promotion")
    spark = get_spark("dineiq-promotion")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    wastage = spark.read.parquet(str(PARQUET_DIR / "wastage"))
    t = {n: load_clean_table(spark, n, PROCESSED_DIR)
         for n in ["Promotions", "Promotion_Items", "Promotion_Locations"]}
    bounds = fact.agg(F.to_date(F.min("order_datetime")), F.to_date(F.max("order_datetime"))).first()

    with logger.timer("promotion windows"):
        sc = scope(t["Promotions"], t["Promotion_Items"], t["Promotion_Locations"])
        df = compare(window_metrics(fact, wastage, sc), t["Promotions"], bounds[0], bounds[1], cfg)
        df.write.mode("overwrite").parquet(ac.out_path("promotion_effectiveness"))
    pdf = spark.read.parquet(ac.out_path("promotion_effectiveness")).orderBy("start_date").toPandas()
    logger.log(pdf[["promotion_id", *FLAGS]].to_string())
    ac.write_section("11", report(pdf, cfg))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

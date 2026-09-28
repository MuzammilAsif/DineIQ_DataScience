"""Price change impact and price-sensitivity classification (SRS Steps 25-26).

Output: parquet_data/price_sensitivity/{events,items}/, report section 10.
Run: .venv/bin/python spark_jobs/10_price_intelligence.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PROCESSED_DIR, get_spark  # noqa: E402

HIGH, MODERATE, LOW, INCONCLUSIVE, NOT_EVALUATED = (
    "Highly Price Sensitive", "Moderately Price Sensitive", "Low Price Sensitivity",
    "Inconclusive", "Not Evaluated")
CLASSES = [HIGH, MODERATE, LOW, INCONCLUSIVE, NOT_EVALUATED]


def price_changes(pricing):
    # chain-wide rows only; location rows are one-off premiums, not a price trajectory
    w = Window.partitionBy("item_id").orderBy("effective_start_date")
    return (pricing.filter(F.col("location_id").isNull())
            .withColumn("old_price", F.lag("price").over(w))
            .filter(F.col("old_price").isNotNull() & (F.col("price") != F.col("old_price")))
            .select("item_id", F.col("effective_start_date").alias("change_date"),
                    F.col("old_price").cast("double"), F.col("price").cast("double").alias("new_price")))


def daily_quantity(fact):
    return fact.groupBy("item_id", "category_name", F.to_date("order_datetime").alias("date")).agg(
        F.sum("quantity").alias("qty"))


def event_impact(changes, daily, data_start, data_end, cfg):
    """Average daily quantity before vs after each change, against a same-category control.

    daily: item_id, category_name, date, qty. The control is the rest of the item's category
    over the same windows, which takes out seasonality shared by the category.
    """
    window = cfg["window_days"]
    cats = daily.select("item_id", "category_name").distinct()
    cat_daily = daily.groupBy("category_name", "date").agg(F.sum("qty").alias("cat_qty"))
    j = (changes.join(cats, "item_id")
         .join(cat_daily, "category_name")
         .join(daily.select("item_id", "date", "qty"), ["item_id", "date"], "left")
         .fillna({"qty": 0}))
    offset = F.datediff("date", "change_date")
    before = (offset >= -window) & (offset < 0)
    after = (offset >= 0) & (offset < window)

    def total(cond, col):
        return F.sum(F.when(cond, F.col(col)).otherwise(0))

    ev = (j.groupBy("item_id", "category_name", "change_date", "old_price", "new_price")
          .agg(total(before, "qty").alias("qty_before"), total(after, "qty").alias("qty_after"),
               (total(before, "cat_qty") - total(before, "qty")).alias("control_before"),
               (total(after, "cat_qty") - total(after, "qty")).alias("control_after")))
    full_window = ((F.date_sub("change_date", window) >= F.lit(data_start))
                   & (F.date_add("change_date", window - 1) <= F.lit(data_end)))
    price_pct = (F.col("new_price") - F.col("old_price")) / F.col("old_price")
    return (ev.withColumn("demand_before", F.col("qty_before") / window)
            .withColumn("demand_after", F.col("qty_after") / window)
            .withColumn("price_change_pct", price_pct)
            .withColumn("demand_change_pct",
                        (F.col("demand_after") - F.col("demand_before")) / F.col("demand_before"))
            .withColumn("control_change_pct",
                        F.col("control_after") / F.col("control_before") - 1)
            .withColumn("adjusted_demand_change_pct",
                        (1 + F.col("demand_change_pct")) / (1 + F.col("control_change_pct")) - 1)
            .withColumn("evaluable", full_window & (F.col("qty_before") > 0)
                        & (F.col("control_before") > 0)
                        & (F.abs(price_pct) >= cfg["min_price_change"]))
            .withColumn("elasticity",
                        F.when(F.col("evaluable"),
                               F.col("adjusted_demand_change_pct") / F.col("price_change_pct"))))


def classify_items(events, items, cfg):
    """One row per item: the evaluable change with the largest price move decides.

    A non-negative elasticity (demand moved with the price) is Inconclusive: something other
    than price drove demand. Tertiles are taken over the negative ones.
    """
    w = Window.partitionBy("item_id").orderBy(F.desc(F.abs("price_change_pct")))
    best = (events.filter("evaluable").withColumn("rn", F.row_number().over(w))
            .filter("rn = 1").drop("rn"))
    negative = F.col("elasticity") < 0
    pct = F.percent_rank().over(Window.partitionBy(negative).orderBy(F.abs("elasticity")))
    best = best.withColumn("sensitivity_percentile", F.when(negative, pct)).withColumn(
        "price_sensitivity",
        F.when(~negative, INCONCLUSIVE)
        .when(F.col("sensitivity_percentile") > cfg["high_above"], HIGH)
        .when(F.col("sensitivity_percentile") < cfg["low_below"], LOW)
        .otherwise(MODERATE))
    return (items.join(best, "item_id", "left")
            .withColumn("price_sensitivity", F.coalesce("price_sensitivity", F.lit(NOT_EVALUATED))))


def report(items_pdf, n_events, n_evaluable, cfg):
    window = cfg["window_days"]
    counts = items_pdf.price_sensitivity.value_counts()
    ev = items_pdf[items_pdf.elasticity.notna()].copy()
    cols = ["item_name", "change_date", "old_price", "new_price", "price_change_pct",
            "demand_before", "demand_after", "demand_change_pct", "control_change_pct",
            "elasticity", "price_sensitivity"]
    fmt = {"old_price": "{:,.0f}", "new_price": "{:,.0f}", "price_change_pct": "{:+.1%}",
           "demand_before": "{:.1f}", "demand_after": "{:.1f}", "demand_change_pct": "{:+.1%}",
           "control_change_pct": "{:+.1%}", "elasticity": "{:+.2f}"}
    neg = ev[ev.elasticity < 0].sort_values("elasticity")
    inconclusive = ev[ev.elasticity >= 0].sort_values("elasticity", ascending=False)
    return "\n\n".join([
        "## 4. Price intelligence and price sensitivity",
        f"{n_events} chain-wide price changes in Pricing_History. {n_evaluable} are evaluable: "
        f"a price move of at least {cfg['min_price_change']:.0%}, a full {window}-day window "
        "on both sides inside the order data, and sales before the change. Smaller changes are "
        "routine 2-5% inflation revisions, where demand noise swamps the price effect.",
        f"Demand is average daily quantity sold chain-wide, {window} days before vs {window} "
        "days from the change. To take out seasonality, the item's demand change is divided by "
        "the change in the rest of its category over the same windows (`control_change_pct`). "
        "Elasticity = adjusted % demand change / % price change. Where an item has several "
        "evaluable changes, the largest one is used.",
        "A zero or positive elasticity (demand moved with the price) is **Inconclusive**: "
        "something other than price drove demand. The negative ones are split by the percentile "
        f"of |elasticity|: above {cfg['high_above']} is Highly, below {cfg['low_below']} is "
        "Low, else Moderately Price Sensitive. Items without an evaluable change are "
        "**Not Evaluated**, and no class is forced on them.",
        ", ".join(f"{c}: {int(counts.get(c, 0))}" for c in CLASSES) + ".",
        "**Price-sensitivity evidence (negative elasticity), most sensitive first**",
        ac.md_table(neg[cols], fmt) if len(neg) else "None.",
        "**Inconclusive changes**",
        ac.md_table(inconclusive[cols], fmt) if len(inconclusive) else "None.",
        "Caveats: the category control removes seasonality that the whole category shares, "
        "but not seasonality specific to one item, or promotions on that item alone. Some "
        "control items had their own price change on the same date, which dampens the "
        "adjustment. Treat these as rough elasticities.",
    ])


def main():
    cfg = ac.load_config()["price"]
    logger = JobLogger("price")
    spark = get_spark("dineiq-price")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    pricing = load_clean_table(spark, "Pricing_History", PROCESSED_DIR)
    items = (load_clean_table(spark, "Menu_Items", PROCESSED_DIR)
             .filter("is_active").select("item_id", "item_name"))
    bounds = fact.agg(F.to_date(F.min("order_datetime")), F.to_date(F.max("order_datetime"))).first()

    with logger.timer("price impact"):
        events = event_impact(price_changes(pricing), daily_quantity(fact), bounds[0], bounds[1],
                              cfg).cache()
        per_item = classify_items(events, items, cfg).cache()
        events.write.mode("overwrite").parquet(ac.out_path("price_sensitivity/events"))
        per_item.write.mode("overwrite").parquet(ac.out_path("price_sensitivity/items"))
    pdf = per_item.toPandas()
    n_events, n_eval = events.count(), events.filter("evaluable").count()
    logger.log(f"events={n_events} evaluable={n_eval} "
               f"{pdf.price_sensitivity.value_counts().to_dict()}")
    ac.write_section("10", report(pdf, n_events, n_eval, cfg))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

"""Wastage analysis and wastage-risk prediction (SRS Steps 23-24).

Output: parquet_data/wastage_analysis/, parquet_data/wastage_risk/ (test-period predictions),
models/spark/wastage_risk/<version>/, report section 09.
Run: .venv/bin/python spark_jobs/09_wastage_analysis.py
"""
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, ROOT, get_spark  # noqa: E402

MODEL_DIR = ROOT / "models" / "spark" / "wastage_risk"

FEATURES = ["prepared_quantity", "recent_demand", "prep_to_demand", "day_of_week", "month",
            "is_weekend", "promo_active", "item_popularity", "location_idx", "category_idx"]
# The label and anything computed from the day's waste or its outcome.
LEAKAGE_COLUMNS = ["wastage_percentage", "quantity_wasted", "cost_of_waste", "high_wastage_risk",
                   "consumed_stock", "closing_stock", "label", "item_wastage_percentage"]
DOW = ["", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]


def daily_grain(fact, wastage, coverage, menu):
    """item x location x date with COGS, wastage cost and whether a promotion covered it."""
    sales = (fact.groupBy("item_id", "location_id", F.to_date("order_datetime").alias("date"))
             .agg(F.sum(F.col("unit_cost") * F.col("quantity")).cast("double").alias("cogs")))
    waste = wastage.groupBy("item_id", "location_id", "date").agg(
        F.sum("cost_of_waste").cast("double").alias("wastage_cost"))
    promo = coverage.select("item_id", "location_id", "date").distinct().withColumn(
        "promo_active", F.lit(1))
    return (sales.join(waste, ["item_id", "location_id", "date"], "full")
            .join(promo, ["item_id", "location_id", "date"], "left")
            .join(menu, "item_id", "left")
            .fillna({"cogs": 0.0, "wastage_cost": 0.0, "promo_active": 0}))


def wastage_by(daily, keys):
    # cost-based rate, because wasted quantities are in kg/l/pcs and sales are in portions
    return (daily.groupBy(*keys)
            .agg(F.sum("wastage_cost").alias("wastage_cost"), F.sum("cogs").alias("cogs"))
            .withColumn("wastage_rate",
                        F.try_divide(F.col("wastage_cost"), F.col("wastage_cost") + F.col("cogs"))))


def risk_frame(inventory, wastage, coverage, menu, item_pop, cfg):
    waste = wastage.groupBy("item_id", "location_id", "date").agg(
        F.sum("quantity_wasted").alias("quantity_wasted"))
    promo = coverage.select("item_id", "location_id", "date").distinct().withColumn(
        "promo_active", F.lit(1))
    day_num = F.datediff("date", F.lit("2000-01-01"))
    w = (Window.partitionBy("item_id", "location_id").orderBy(day_num)
         .rangeBetween(-cfg["demand_lookback_days"], -1))
    df = (inventory.filter(F.col("prepared_quantity") > 0)
          .join(waste, ["item_id", "location_id", "date"], "left")
          .join(promo, ["item_id", "location_id", "date"], "left")
          .join(menu.select("item_id", "category_name"), "item_id", "left")
          .join(item_pop, "item_id", "left")
          .fillna({"quantity_wasted": 0, "promo_active": 0, "item_popularity": 0.0})
          .withColumn("prepared_quantity", F.col("prepared_quantity").cast("double"))
          .withColumn("wastage_percentage",
                      F.col("quantity_wasted").cast("double") / F.col("prepared_quantity"))
          .withColumn("recent_demand", F.avg(F.col("consumed_stock").cast("double")).over(w))
          .withColumn("prep_to_demand", F.when(F.col("recent_demand") > 0,
                                               F.col("prepared_quantity") / F.col("recent_demand")))
          .withColumn("day_of_week", F.dayofweek("date"))
          .withColumn("month", F.month("date"))
          .withColumn("is_weekend", F.col("day_of_week").isin(6, 7, 1).cast("int")))
    df = df.withColumn("label",
                       (F.col("wastage_percentage") > cfg["high_risk_above"]).cast("double"))
    # no history (first days) or no recent consumption; -1 is out of range for both columns
    return df.fillna({"recent_demand": -1.0, "prep_to_demand": -1.0})


def pipeline(cfg):
    return Pipeline(stages=[
        StringIndexer(inputCol="location_id", outputCol="location_idx", handleInvalid="keep"),
        StringIndexer(inputCol="category_name", outputCol="category_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=FEATURES, outputCol="features"),
        RandomForestClassifier(numTrees=cfg["num_trees"], maxDepth=cfg["max_depth"],
                               seed=cfg["seed"], weightCol="weight", maxBins=64),
    ])


def add_weights(df):
    pos = df.agg(F.avg("label")).first()[0]
    return df.withColumn("weight", F.when(F.col("label") == 1, 0.5 / pos).otherwise(0.5 / (1 - pos)))


def evaluate(pred):
    counts = {(r.label, r.prediction): r["count"] for r in
              pred.groupBy("label", "prediction").count().collect()}
    tp, fp = counts.get((1.0, 1.0), 0), counts.get((0.0, 1.0), 0)
    fn, tn = counts.get((1.0, 0.0), 0), counts.get((0.0, 0.0), 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    auc = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(pred)
    pr = BinaryClassificationEvaluator(metricName="areaUnderPR").evaluate(pred)
    n = tp + fp + fn + tn
    return dict(n=n, tp=tp, fp=fp, fn=fn, tn=tn, accuracy=(tp + tn) / n, precision=precision,
                recall=recall, f1=2 * precision * recall / (precision + recall) if precision + recall else 0.0,
                auc_roc=auc, auc_pr=pr, positive_rate=(tp + fn) / n)


def report(tables, m, train_n, train_pos, importances, cfg, version, item_snapshot):
    t = tables
    fmt = {"wastage_cost": "{:,.0f}", "cogs": "{:,.0f}", "wastage_rate": "{:.1%}"}
    lines = [
        "## 3. Wastage analysis and wastage-risk prediction",
        "Wastage rate = wastage cost / (wastage cost + cost of goods sold). The rate is cost-based "
        "because wasted quantities are in kg, litres or pieces while sales are in portions. "
        "\"Promotion active\" means a promotion covered that item at that location on that day.",
        "**Top 10 items by wastage cost**", ac.md_table(t["item"], fmt),
        "**By category**", ac.md_table(t["category"], fmt),
        "**Locations, highest and lowest 5 by wastage rate**", ac.md_table(t["location"], fmt),
        "**By day of week**", ac.md_table(t["dow"], fmt),
        "**Promotion active vs not**", ac.md_table(t["promo"], fmt),
        "### Wastage-risk model",
        f"Grain: item x location x day from Inventory (rows with prepared quantity > 0), joined "
        f"to Wastage. Label `high_wastage_risk` = wasted / prepared > {cfg['high_risk_above']}. "
        "A top-quartile cut was not usable: 95% of rows have no waste, and the 75th percentile "
        "of the rest is exactly 1.0 (the whole preparation wasted).",
        f"Features: {', '.join(f'`{c}`' for c in FEATURES)}. `recent_demand` is the mean "
        f"consumed quantity over the previous {cfg['demand_lookback_days']} days, excluding the "
        "day itself. `prep_to_demand` is prepared quantity over that mean. `item_popularity` comes "
        f"from the `as_of_date={item_snapshot}` item features, before the test period. Excluded "
        "as leakage: the day's wasted quantity, waste cost, wastage percentage, consumed and "
        "closing stock, and item-level wastage features. "
        "`tests/test_analytics.py` asserts that none of them reach the feature vector.",
        f"Time-based split: train before {cfg['test_start']} ({train_n:,} rows, "
        f"{train_pos:.1%} positive), test from {cfg['test_start']} ({m['n']:,} rows, "
        f"{m['positive_rate']:.1%} positive). Spark MLlib `RandomForestClassifier` "
        f"(numTrees={cfg['num_trees']}, maxDepth={cfg['max_depth']}) with balanced class weights. "
        "Step 5 already covers the three-algorithm comparison, so one algorithm is used here.",
        ac.md_table(pd.DataFrame([{
            "accuracy": m["accuracy"], "precision": m["precision"], "recall": m["recall"],
            "F1": m["f1"], "ROC AUC": m["auc_roc"], "PR AUC": m["auc_pr"]}]),
            {k: "{:.3f}" for k in ["accuracy", "precision", "recall", "F1", "ROC AUC", "PR AUC"]}),
        f"Confusion matrix (test): TP {m['tp']:,}, FP {m['fp']:,}, FN {m['fn']:,}, "
        f"TN {m['tn']:,}. Always predicting \"not high risk\" would score "
        f"{1 - m['positive_rate']:.1%} accuracy, so accuracy is not a useful measure here. "
        f"PR AUC against the {m['positive_rate']:.1%} base rate is the more honest one.",
        "Feature importances: " + ", ".join(f"`{f}` {v:.3f}" for f, v in importances) + ".",
        f"Item-level features (`item_popularity`, `category_idx`) carry "
        f"{sum(v for f, v in importances if f in ('item_popularity', 'category_idx')):.0%} of the "
        "importance, so the model mostly learns which items are wasteful rather than which "
        "days are risky. Day-level signals (promotion, weekday, month) add little. It is "
        "useful for ranking item-location pairs to watch, not for day-by-day prep decisions.",
        f"Model version `{version}` in `models/spark/wastage_risk/`. Test-period predictions are "
        "in `parquet_data/wastage_risk/`.",
    ]
    return "\n\n".join(lines)


def main():
    cfg = ac.load_config()["wastage"]
    logger = JobLogger("wastage")
    spark = get_spark("dineiq-wastage")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    wastage = spark.read.parquet(str(PARQUET_DIR / "wastage"))
    inventory = spark.read.parquet(str(PARQUET_DIR / "inventory"))
    t = {n: load_clean_table(spark, n, PROCESSED_DIR)
         for n in ["Promotions", "Promotion_Items", "Promotion_Locations", "Menu_Items",
                   "Menu_Categories"]}
    coverage = ac.promo_coverage(t["Promotions"], t["Promotion_Items"], t["Promotion_Locations"])
    menu = (t["Menu_Items"].join(t["Menu_Categories"], "category_id")
            .select("item_id", "item_name", "category_name"))

    with logger.timer("wastage analysis"):
        daily = daily_grain(fact, wastage, coverage, menu).cache()
        by_item = wastage_by(daily, ["item_name", "category_name"])
        by_loc = wastage_by(daily, ["location_id"])
        daily_dow = daily.withColumn("dow", F.dayofweek("date"))
        tables = {
            "item": by_item.orderBy(F.desc("wastage_cost")).limit(10).toPandas(),
            "category": wastage_by(daily, ["category_name"]).orderBy(F.desc("wastage_rate")).toPandas(),
            "dow": wastage_by(daily_dow, ["dow"]).orderBy("dow").toPandas(),
            "promo": wastage_by(daily, ["promo_active"]).orderBy("promo_active").toPandas(),
        }
        loc = by_loc.orderBy(F.desc("wastage_rate")).toPandas()
        tables["location"] = pd.concat([loc.head(5), loc.tail(5)])
        tables["dow"]["dow"] = tables["dow"]["dow"].map(lambda d: DOW[d])
        tables["promo"]["promo_active"] = tables["promo"]["promo_active"].map({0: "no", 1: "yes"})
        (wastage_by(daily, ["item_id", "location_id", "category_name", "promo_active"])
         .write.mode("overwrite").parquet(ac.out_path("wastage_analysis")))

    snapshots = sorted(p.name.split("=", 1)[1]
                       for p in (ac.FEATURES_DIR / "item_features").glob("as_of_date=*"))
    item_snapshot = max(s for s in snapshots if s < cfg["test_start"])
    item_pop = spark.read.parquet(
        str(ac.FEATURES_DIR / "item_features" / f"as_of_date={item_snapshot}")).select(
        "item_id", "item_popularity")

    with logger.timer("wastage-risk model"):
        df = risk_frame(inventory, wastage, coverage, menu, item_pop, cfg)
        train = add_weights(df.filter(F.col("date") < cfg["test_start"])).cache()
        test = df.filter(F.col("date") >= cfg["test_start"]).cache()
        train_n, train_pos = train.count(), train.agg(F.avg("label")).first()[0]
        model = pipeline(cfg).fit(train)
        pred = model.transform(test).cache()
        m = evaluate(pred)
        logger.log(f"train={train_n} pos={train_pos:.4f} test={m}")
        rf = model.stages[-1]
        importances = sorted(zip(FEATURES, rf.featureImportances.toArray()), key=lambda x: -x[1])

        version = "v" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out = MODEL_DIR / version
        model.write().overwrite().save(str(out / "model"))
        (out / "model_version.txt").write_text(json.dumps(
            dict(version=version, algorithm="RandomForestClassifier", features=FEATURES,
                 item_snapshot=item_snapshot, **{k: v for k, v in cfg.items()},
                 metrics=m), indent=2, default=str) + "\n")
        (MODEL_DIR / "LATEST").write_text(version + "\n")
        (pred.select("item_id", "location_id", "date", "prepared_quantity", "wastage_percentage",
                     F.col("label").cast("int").alias("high_wastage_risk"),
                     F.col("prediction").cast("int").alias("predicted_risk"),
                     F.round(vector_to_array("probability")[1], 4).alias("risk_probability"),
                     F.lit(version).alias("model_version"))
         .write.mode("overwrite").parquet(ac.out_path("wastage_risk")))

    ac.write_section("09", report(tables, m, train_n, train_pos, importances, cfg, version,
                                  item_snapshot))
    logger.log("wastage complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

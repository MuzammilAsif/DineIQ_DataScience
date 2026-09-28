"""Step 4 — menu profitability analysis and performance classification (SRS Steps 9-11).

Every threshold comes from config/classification_thresholds.yaml.

Outputs:
  parquet_data/menu_classification/as_of_date=<date>/              one row per active item
  parquet_data/menu_classification_by_location/as_of_date=<date>/  item x location categories
  reports/menu_classification_report.md

Run: .venv/bin/python spark_jobs/05_menu_classification.py [as_of_date]
     (defaults to the latest item_features snapshot)
"""
import importlib.util
import shutil
import sys
from pathlib import Path

import pyspark.sql.functions as F
import yaml
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, REPORTS_DIR, ROOT, get_spark  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "feature_engineering", Path(__file__).resolve().parent / "04_feature_engineering.py")
fe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fe)

CONFIG_PATH = ROOT / "config" / "classification_thresholds.yaml"
OUT_DIR = PARQUET_DIR / "menu_classification"
BY_LOCATION_DIR = PARQUET_DIR / "menu_classification_by_location"

PROFIT_DRIVER = "Profit Driver"
VOLUME_DRIVER = "Volume Driver"
HIDDEN_OPPORTUNITY = "Hidden Opportunity"
LOW_PERFORMER = "Low Performer"
INSUFFICIENT_HISTORY = "Insufficient History"
CATEGORIES = [PROFIT_DRIVER, VOLUME_DRIVER, HIDDEN_OPPORTUNITY, LOW_PERFORMER, INSUFFICIENT_HISTORY]

FLAGS = [
    "loss_making",
    "rarely_purchased_high_margin",
    "excessive_wastage",
    "high_rating_low_profit",
    "low_rating_high_sales",
    "promotion_dependent",
    "weekend_skewed",
    "seasonal_item",
    "location_inconsistent",
    "insufficient_history",
]

SCORES = [
    ("profit_percentage", "profitability"),
    ("total_quantity_sold", "demand"),
    ("quality_score", "quality"),
    ("wastage_percentage", "wastage"),
]


def load_thresholds(path=CONFIG_PATH):
    with open(path) as fh:
        return yaml.safe_load(fh)


def tier(p, t):
    return (F.when(p.isNull(), F.lit(None))
            .when(p < t["tiers"]["low_below"], "Low")
            .when(p > t["tiers"]["high_above"], "High")
            .otherwise("Medium"))


def classify(df, t, partition_cols=()):
    """Adds percentiles, tiers, excessive_wastage and category.

    Needs total_quantity_sold, profit_percentage, average_rating, repeat_purchase_rate,
    wastage_percentage and days_since_launch. Percentiles are ranked within partition_cols.
    """
    rating_part = F.col("average_rating") / 5
    df = df.withColumn(
        "quality_score",
        F.when(rating_part.isNotNull() & F.col("repeat_purchase_rate").isNotNull(),
               (rating_part + F.col("repeat_purchase_rate")) / 2)
        .otherwise(F.coalesce(rating_part, F.col("repeat_purchase_rate"))),
    )

    new_item = F.col("days_since_launch") < t["new_item_days"]
    sold = F.coalesce(F.col("total_quantity_sold"), F.lit(0)) > 0
    # Unsold and new items are left out of the ranking population so their partial
    # numbers don't shift the percentiles of established items.
    ranked = sold & ~new_item
    for metric, name in SCORES:
        in_pop = ranked & F.col(metric).isNotNull()
        w = Window.partitionBy(*partition_cols, in_pop).orderBy(metric)
        df = df.withColumn(f"{name}_percentile", F.when(in_pop, F.percent_rank().over(w)))
        df = df.withColumn(f"{name}_tier", tier(F.col(f"{name}_percentile"), t))

    demand_high = F.col("demand_tier") == "High"
    profit_high = F.col("profitability_tier") == "High"
    df = df.withColumn(
        "excessive_wastage",
        F.coalesce((F.col("wastage_tier") == "High")
                   & (F.col("wastage_percentage") > t["excessive_wastage_min"]), F.lit(False)),
    )
    return df.withColumn(
        "category",
        F.when(new_item, INSUFFICIENT_HISTORY)
        .when(~sold, LOW_PERFORMER)
        .when(demand_high & profit_high & ~F.col("excessive_wastage"), PROFIT_DRIVER)
        .when(demand_high, VOLUME_DRIVER)
        .when(F.coalesce(profit_high | (F.col("quality_tier") == "High"), F.lit(False)),
              HIDDEN_OPPORTUNITY)
        .otherwise(LOW_PERFORMER),
    )


def add_flags(df, t):
    """The item-level tricky-case flags. location_inconsistent is added separately."""
    def flag(cond):
        return F.coalesce(cond, F.lit(False))

    return (
        df.withColumn("loss_making", flag(F.col("contribution_margin") < 0))
        .withColumn("rarely_purchased_high_margin",
                    flag((F.col("demand_tier") == "Low") & (F.col("profitability_tier") == "High")))
        .withColumn("high_rating_low_profit",
                    flag((F.col("average_rating") >= t["high_rating_min"])
                         & (F.col("profitability_tier") == "Low")))
        .withColumn("low_rating_high_sales",
                    flag((F.col("average_rating") <= t["low_rating_max"])
                         & (F.col("demand_tier") == "High")))
        .withColumn("promotion_dependent",
                    flag(F.col("promotion_dependency") > t["promotion_dependent_above"]))
        .withColumn("weekend_skewed",
                    flag(F.col("weekend_order_ratio") > t["weekend_skewed_above"]))
        .withColumn("seasonal_item", flag(F.col("is_seasonal")))
        .withColumn("insufficient_history",
                    flag(F.col("days_since_launch") < t["new_item_days"]))
    )


def location_consistency(item_categories, loc_metrics, t):
    """Per-location categories, plus the location_inconsistent flag per item.

    item_categories: item_id, category, days_since_launch.
    loc_metrics: item x location metrics (item_location_features) for the same items.
    """
    by_loc = classify(
        loc_metrics.join(item_categories.select("item_id", "days_since_launch"), "item_id"),
        t, ["location_id"],
    ).join(item_categories.select("item_id", F.col("category").alias("global_category")), "item_id")

    per_item = by_loc.groupBy("item_id").agg(
        F.count("*").alias("n_locations_sold"),
        F.avg((F.col("category") != F.col("global_category")).cast("double"))
        .alias("location_mismatch_share"),
        F.map_from_entries(F.sort_array(F.collect_list(F.struct("location_id", "category"))))
        .alias("location_categories"),
    )
    checked = F.col("n_locations_sold") >= t["location"]["min_locations"]
    per_item = per_item.select(
        "item_id",
        "n_locations_sold",
        F.when(checked, F.col("location_mismatch_share")).alias("location_mismatch_share"),
        F.when(checked, F.col("location_categories")).alias("location_categories"),
        F.coalesce(checked & (F.col("location_mismatch_share") >= t["location"]["inconsistent_share"]),
                   F.lit(False)).alias("location_inconsistent"),
    )
    return per_item, by_loc


def build(item_feats, menu_items, fact, ratings, wastage, as_of_date, t):
    """Returns (classification, by_location) DataFrames for active items as of as_of_date."""
    items = (
        item_feats.join(
            menu_items.filter(F.col("is_active"))
            .select("item_id", "launch_date", "is_seasonal"), "item_id")
        .withColumn("days_since_launch",
                    F.datediff(F.lit(fe._as_date(as_of_date)), F.col("launch_date")))
    )
    items = add_flags(classify(items, t), t)

    loc_metrics = fe.item_location_features(fact, ratings, wastage, as_of_date).join(
        items.select("item_id"), "item_id")
    per_item, by_loc = location_consistency(items, loc_metrics, t)

    out = (
        items.join(per_item, "item_id", "left")
        .withColumn("location_inconsistent", F.coalesce("location_inconsistent", F.lit(False)))
        .withColumn("n_locations_sold", F.coalesce("n_locations_sold", F.lit(0)))
    )
    classification = out.select(
        "item_id", "item_name", "category_name", "category",
        *FLAGS,
        "profitability_percentile", "demand_percentile", "quality_percentile", "wastage_percentile",
        "profitability_tier", "demand_tier", "quality_tier", "wastage_tier",
        "profit_percentage", "contribution_margin", "item_revenue", "item_cost",
        "total_quantity_sold", "average_rating", "repeat_purchase_rate", "quality_score",
        "wastage_percentage", "promotion_dependency", "weekend_order_ratio", "sales_trend",
        "rating_trend", "price_change_percentage",
        "launch_date", "days_since_launch", "is_seasonal",
        "n_locations_sold", "location_mismatch_share", "location_categories",
    )
    by_location = by_loc.select(
        "item_id", "location_id", "category", "global_category",
        "profitability_percentile", "demand_percentile", "quality_percentile", "wastage_percentile",
        "profit_percentage", "contribution_margin", "total_quantity_sold", "average_rating",
        "repeat_purchase_rate", "quality_score", "wastage_percentage",
    )
    return classification, by_location


def _fmt(v, kind):
    if v is None:
        return "null"
    if kind == "pct":
        return f"{v * 100:.1f}%"
    if kind == "p":
        return f"{v:.2f}"
    if kind == "money":
        return f"{v:,.0f}"
    if kind == "int":
        return f"{int(v):,}"
    if kind == "r":
        return f"{v:.2f}"
    return str(v)


def _top(df, where, col, ascending):
    rows = df[where].sort_values(col, ascending=ascending)
    return rows.iloc[0] if len(rows) else None


def _gap_loss_making(df, t):
    low = _top(df, df.item_id.notna(), "contribution_margin", True)
    return (f"lowest contribution margin is {_fmt(low.contribution_margin, 'money')} "
            f"({low.item_name}); every item sells above unit cost on aggregate.")


def _gap_high_rating_low_profit(df, t):
    top = _top(df, df.item_id.notna(), "average_rating", False)
    low = _top(df, df.profitability_tier == "Low", "average_rating", False)
    closest = f"{low.item_name} at {_fmt(low.average_rating, 'r')}" if low is not None else "none"
    return (f"the highest average rating in the dataset is {_fmt(top.average_rating, 'r')} "
            f"({top.item_name}), below the {t['high_rating_min']} threshold, so no item counts "
            f"as highly rated. The highest-rated item in the Low profitability tier is {closest}.")


def _gap_low_rating_high_sales(df, t):
    low = _top(df, df.demand_tier == "High", "average_rating", True)
    closest = f"{low.item_name} at {_fmt(low.average_rating, 'r')}" if low is not None else "none"
    return (f"no High-demand item has an average rating at or below {t['low_rating_max']}; "
            f"the lowest-rated High-demand item is {closest}.")


def _gap_max(col, kind):
    def note(df, t):
        top = _top(df, df[col].notna(), col, False)
        return f"highest {col} is {_fmt(top[col], kind)} ({top.item_name})."
    return note


def _gap_fixed(text):
    return lambda df, t: text


# Per flag: the metrics that explain it, which item to show (sort column, descending?),
# and how to describe the closest miss when no item triggers it.
EXAMPLES = {
    "loss_making": (
        [("contribution_margin", "money"), ("profit_percentage", "r"), ("total_quantity_sold", "int"),
         ("demand_percentile", "p")],
        ("contribution_margin", False), _gap_loss_making),
    "rarely_purchased_high_margin": (
        [("profit_percentage", "r"), ("profitability_percentile", "p"), ("total_quantity_sold", "int"),
         ("demand_percentile", "p")],
        ("profitability_percentile", True),
        _gap_fixed("no item is both Low demand and High profitability tier.")),
    "excessive_wastage": (
        [("wastage_percentage", "pct"), ("wastage_percentile", "p"), ("total_quantity_sold", "int"),
         ("demand_percentile", "p")],
        ("wastage_percentage", True), _gap_max("wastage_percentage", "pct")),
    "high_rating_low_profit": (
        [("average_rating", "r"), ("profit_percentage", "r"), ("profitability_percentile", "p")],
        ("average_rating", True), _gap_high_rating_low_profit),
    "low_rating_high_sales": (
        [("average_rating", "r"), ("total_quantity_sold", "int"), ("demand_percentile", "p")],
        ("total_quantity_sold", True), _gap_low_rating_high_sales),
    "promotion_dependent": (
        [("promotion_dependency", "pct"), ("total_quantity_sold", "int")],
        ("promotion_dependency", True), _gap_max("promotion_dependency", "pct")),
    "weekend_skewed": (
        [("weekend_order_ratio", "pct"), ("total_quantity_sold", "int")],
        ("weekend_order_ratio", True), _gap_max("weekend_order_ratio", "pct")),
    "seasonal_item": (
        [("sales_trend", "r"), ("total_quantity_sold", "int"), ("demand_percentile", "p")],
        ("total_quantity_sold", True), _gap_fixed("no active item is marked seasonal.")),
    "location_inconsistent": (
        [("n_locations_sold", "int"), ("location_mismatch_share", "pct")],
        ("location_mismatch_share", True), _gap_max("location_mismatch_share", "pct")),
    "insufficient_history": (
        [("launch_date", "str"), ("days_since_launch", "int"), ("total_quantity_sold", "int")],
        ("days_since_launch", False),
        _gap_fixed("every active item launched before the new-item cutoff.")),
}


def write_report(pdf, as_of_date, t, path):
    lines = [
        "# Menu Classification Report",
        "",
        f"Snapshot `as_of_date={as_of_date}`, {len(pdf)} active items. Generated by "
        "`spark_jobs/05_menu_classification.py` from "
        "`parquet_data/menu_classification/`. Thresholds come from "
        "`config/classification_thresholds.yaml`:",
        "",
        "```yaml",
        yaml.safe_dump(t, sort_keys=False).strip(),
        "```",
        "",
        "## Items per category",
        "",
        "| Category | Items |",
        "|---|---|",
    ]
    counts = pdf.category.value_counts()
    lines += [f"| {c} | {int(counts.get(c, 0))} |" for c in CATEGORIES]

    lines += ["", "## Items per flag", "", "| Flag | Items |", "|---|---|"]
    lines += [f"| `{f}` | {int(pdf[f].sum())} |" for f in FLAGS]

    lines += [
        "",
        "## Worked examples",
        "",
        "For each tricky case, one real item from this snapshot that triggered the flag, with "
        "the numbers behind it. Where more than one item triggered it, the most extreme one is "
        "shown. Where no item triggered it, the case is listed as a known gap.",
    ]
    gaps = []
    for flag in FLAGS:
        cols, (sort_col, desc), gap_note = EXAMPLES[flag]
        hits = pdf[pdf[flag]]
        lines += ["", f"### `{flag}`", ""]
        if hits.empty:
            note = gap_note(pdf, t)
            lines.append(f"**No item triggered this flag.** Known gap: {note}")
            gaps.append(flag)
            continue
        ex = hits.sort_values(sort_col, ascending=not desc, na_position="last").iloc[0]
        lines.append(f"{len(hits)} item(s). Example: **{ex['item_name']}** (`{ex['item_id']}`, "
                     f"{ex['category_name']}), category **{ex['category']}**.")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines += [f"| `{c}` | {_fmt(ex[c], k)} |" for c, k in cols]
        if flag == "location_inconsistent":
            cats = dict(ex["location_categories"])
            lines.append("")
            lines.append(f"Global category {ex['category']}; per location: "
                         + ", ".join(f"{loc} {cat}" for loc, cat in sorted(cats.items())) + ".")
        others = hits[hits.item_id != ex["item_id"]].item_name.tolist()
        if others:
            shown = ", ".join(others[:10]) + (f", and {len(others) - 10} more" if len(others) > 10 else "")
            lines += ["", f"Also flagged: {shown}."]

    if gaps:
        lines += ["", "## Known gaps", "",
                  "These cases are implemented and covered by synthetic tests in "
                  "`tests/test_menu_classification.py`, but the generated dataset has no item "
                  "that triggers them at the current thresholds:", ""]
        lines += [f"- `{g}`" for g in gaps]

    path.write_text("\n".join(lines) + "\n")


def latest_snapshot():
    return max(p.name.split("=", 1)[1]
               for p in (fe.FEATURES_DIR / "item_features").glob("as_of_date=*"))


def main():
    as_of = sys.argv[1] if len(sys.argv) > 1 else latest_snapshot()
    t = load_thresholds()
    logger = JobLogger("classification")
    spark = get_spark("dineiq-classification")
    spark.sparkContext.setLogLevel("WARN")
    logger.log(f"as_of_date={as_of}, thresholds from {CONFIG_PATH}: {t}")

    with logger.timer("load item features, fact and dimension tables"):
        item_feats = spark.read.parquet(str(fe.FEATURES_DIR / "item_features" / f"as_of_date={as_of}"))
        fact = spark.read.parquet(str(PARQUET_DIR / "fact_order_line")).cache()
        wastage = spark.read.parquet(str(PARQUET_DIR / "wastage"))
        menu_items = load_clean_table(spark, "Menu_Items", PROCESSED_DIR)
        ratings = load_clean_table(spark, "Ratings", PROCESSED_DIR)

    classification, by_location = build(item_feats, menu_items, fact, ratings, wastage, as_of, t)

    out = OUT_DIR / f"as_of_date={as_of}"
    with logger.timer(f"write {out}"):
        classification.write.mode("overwrite").parquet(str(out))
        # the thresholds used travel with the output; Spark and pyarrow skip _-prefixed files
        shutil.copy(CONFIG_PATH, out / "_thresholds.yaml")
        written = spark.read.parquet(str(out))
        logger.log(f"wrote {out} ({written.count()} rows)")

    loc_out = BY_LOCATION_DIR / f"as_of_date={as_of}"
    with logger.timer(f"write {loc_out}"):
        by_location.write.mode("overwrite").parquet(str(loc_out))
        logger.log(f"wrote {loc_out} ({spark.read.parquet(str(loc_out)).count()} rows)")

    pdf = written.toPandas()
    for c in CATEGORIES:
        logger.log(f"category {c}: {int((pdf.category == c).sum())}")
    for f in FLAGS:
        logger.log(f"flag {f}: {int(pdf[f].sum())}")

    report = REPORTS_DIR / "menu_classification_report.md"
    write_report(pdf, as_of, t, report)
    logger.log(f"wrote {report}")
    logger.log("menu classification job complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

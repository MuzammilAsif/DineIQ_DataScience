"""Demand forecasting at item x location x day, rolled up to item, category and location (SRS
Steps 20-22).

Output: parquet_data/demand_forecast/{item_location_day,item_day,category_day,location_day}/,
models/spark/demand_forecast/<version>/, reports/forecast_report.md
Run: .venv/bin/python spark_jobs/15_demand_forecasting.py
"""
import datetime as dt
import json
import math
import sys
from pathlib import Path

import pyspark.sql.functions as F
import yaml
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from schemas import load_clean_table  # noqa: E402
from spark_utils import JobLogger, PARQUET_DIR, PROCESSED_DIR, REPORTS_DIR, ROOT, get_spark  # noqa: E402

CONFIG_PATH = ROOT / "config" / "forecast_config.yaml"
OUT_DIR = PARQUET_DIR / "demand_forecast"
MODEL_DIR = ROOT / "models" / "spark" / "demand_forecast"
REPORT_PATH = REPORTS_DIR / "forecast_report.md"
KEYS = ["item_id", "location_id"]
CALENDAR = ["day_of_week", "is_weekend", "month", "is_seasonal_item", "promotion_active", "price"]


def load_config(path=CONFIG_PATH):
    with open(path) as fh:
        return yaml.safe_load(fh)


def feature_columns(cfg):
    return ([f"lag_{k}" for k in cfg["lags"]] + [f"rolling_mean_{w}" for w in cfg["rolling_windows"]]
            + ["same_weekday_last_week"] + CALENDAR)


def baseline_offset(horizon):
    """Days back from the target date to the latest same-weekday value known at origin."""
    return 7 * math.ceil(horizon / 7)


def daily_quantity(grid, fact):
    """grid: item_id, location_id, date for every day the item was stocked at the location.
    Sales on a stocked day with no order are a real zero."""
    sold = (fact.filter(F.col("item_id").isNotNull())
            .groupBy(*KEYS, F.to_date("order_datetime").alias("date"))
            .agg(F.sum("quantity").cast("double").alias("qty")))
    return (grid.select(*KEYS, "date").distinct()
            .join(sold, [*KEYS, "date"], "left").fillna({"qty": 0.0}))


def _shifted(daily, days, name, date_col):
    """qty moved so that each row lands on date_col = its own date + days."""
    return daily.select(*KEYS, F.date_add("date", days).alias(date_col), F.col("qty").alias(name))


def build_table(daily, cfg):
    """One row per (item, location, origin_date) with lag, rolling and target columns.

    Lags and rolling means join on exact calendar offsets, so a missing day gives a null
    rather than borrowing a neighbouring day. Rows with any null history are dropped.
    """
    h = cfg["horizon_days"]
    df = daily.select(*KEYS, F.col("date").alias("origin_date"))
    for k in cfg["lags"]:
        df = df.join(_shifted(daily, k, f"lag_{k}", "origin_date"), [*KEYS, "origin_date"], "left")
    day = F.datediff("date", F.lit("2000-01-01"))
    for w in cfg["rolling_windows"]:
        # the w-day window ending on a date is the rolling mean for the next day's origin
        win = Window.partitionBy(*KEYS).orderBy(day).rangeBetween(-(w - 1), 0)
        roll = (daily.withColumn("s", F.sum("qty").over(win))
                .withColumn("n", F.count("qty").over(win))
                .select(*KEYS, F.date_add("date", 1).alias("origin_date"),
                        F.when(F.col("n") == w, F.col("s") / w).alias(f"rolling_mean_{w}")))
        df = df.join(roll, [*KEYS, "origin_date"], "left")
    df = df.withColumn("target_date", F.date_add("origin_date", h - 1))
    df = df.join(daily.select(*KEYS, F.col("date").alias("target_date"), F.col("qty").alias("target")),
                 [*KEYS, "target_date"], "inner")
    df = df.join(_shifted(daily, baseline_offset(h), "same_weekday_last_week", "target_date"),
                 [*KEYS, "target_date"], "left")
    history = [f"lag_{k}" for k in cfg["lags"]] + [f"rolling_mean_{w}" for w in cfg["rolling_windows"]]
    return df.dropna(subset=history + ["same_weekday_last_week"])


def add_calendar(df, menu, coverage, prices):
    promo = (coverage.select(*KEYS, F.col("date").alias("target_date")).distinct()
             .withColumn("promotion_active", F.lit(1)))
    t = F.col("target_date")
    return (df.join(promo, [*KEYS, "target_date"], "left")
            .join(menu.select("item_id", F.col("is_seasonal").cast("int").alias("is_seasonal_item")),
                  "item_id", "left")
            .join(prices, [*KEYS, "target_date"], "left")
            .fillna({"promotion_active": 0, "is_seasonal_item": 0})
            .withColumn("day_of_week", F.dayofweek(t))
            .withColumn("is_weekend", F.dayofweek(t).isin(6, 7, 1).cast("int"))
            .withColumn("month", F.month(t)))


def price_in_effect(pricing, grid):
    """Price on each (item, location, date): a location override whose window covers the date,
    otherwise the latest chain-wide price that started on or before it. Chain rows are carried
    forward over gaps between one row's end and the next row's start."""
    far = F.lit(dt.date(9999, 12, 31))
    p = pricing.select("item_id", F.col("location_id").alias("p_loc"),
                       F.col("price").cast("double").alias("price"), "effective_start_date",
                       F.coalesce("effective_end_date", far).alias("end"))
    j = (grid.select(*KEYS, F.col("date").alias("target_date")).distinct().join(p, "item_id")
         .filter((F.col("target_date") >= F.col("effective_start_date"))
                 & (F.col("p_loc").isNull()
                    | ((F.col("p_loc") == F.col("location_id")) & (F.col("target_date") <= F.col("end"))))))
    local = F.col("p_loc").isNotNull()
    return (j.groupBy(*KEYS, "target_date")
            .agg(F.coalesce(F.max_by(F.when(local, F.col("price")), F.when(local, F.col("effective_start_date"))),
                            F.max_by(F.when(~local, F.col("price")), F.when(~local, F.col("effective_start_date"))))
                 .alias("price")))


def chronological_split(df, test_days):
    last = df.agg(F.max("target_date")).first()[0]
    test_start = last - dt.timedelta(days=test_days - 1)
    return (df.filter(F.col("target_date") < F.lit(test_start)),
            df.filter(F.col("target_date") >= F.lit(test_start)), test_start)


def metrics(df, pred_col, actual_col="actual"):
    """MAE, RMSE, R² over all rows; MAPE over rows with a non-zero actual."""
    err = F.col(pred_col) - F.col(actual_col)
    r = df.agg(F.avg(F.abs(err)).alias("mae"), F.sqrt(F.avg(err * err)).alias("rmse"),
               F.avg(F.when(F.col(actual_col) != 0, F.abs(err) / F.abs(F.col(actual_col))))
               .alias("mape"),
               F.sum(err * err).alias("sse"), F.var_pop(actual_col).alias("var"),
               F.count("*").alias("n"),
               F.sum((F.col(actual_col) == 0).cast("int")).alias("zero_actuals")).first()
    r2 = 1 - r.sse / (r["var"] * r.n) if r["var"] else None
    return dict(n=r.n, mae=r.mae, rmse=r.rmse, mape=r.mape, r2=r2, zero_actuals=r.zero_actuals)


def pipeline(cfg):
    g = cfg["gbt"]
    return Pipeline(stages=[
        VectorAssembler(inputCols=feature_columns(cfg), outputCol="features", handleInvalid="keep"),
        GBTRegressor(labelCol="target", maxIter=g["max_iter"], maxDepth=g["max_depth"],
                     stepSize=g["step_size"], subsamplingRate=g["subsampling_rate"], seed=g["seed"]),
    ])


def rollups(pred, menu, cfg):
    recent = f"rolling_mean_{max(cfg['rolling_windows'])}"
    base = pred.select(*KEYS, "origin_date", "target_date", F.col("target").alias("actual"),
                       F.greatest(F.col("prediction"), F.lit(0.0)).alias("forecast"),
                       F.col("same_weekday_last_week").alias("baseline"),
                       F.col(recent).alias("recent_average"))
    cat = base.join(menu.select("item_id", "category_name"), "item_id", "left")

    def roll(df, keys):
        return df.groupBy(*keys).agg(*[F.sum(c).alias(c) for c in
                                       ["actual", "forecast", "baseline", "recent_average"]])
    return {"item_location_day": base,
            "item_day": roll(base, ["item_id", "target_date"]),
            "category_day": roll(cat, ["category_name", "target_date"]),
            "location_day": roll(base, ["location_id", "target_date"]),
            "overall_day": roll(base, ["target_date"])}


def mape_by_volume(item_day):
    """APE of model and baseline by actual daily item volume, non-zero actuals only."""
    bucket = (F.when(F.col("actual") <= 5, "1-5").when(F.col("actual") <= 20, "6-20")
              .when(F.col("actual") <= 50, "21-50").otherwise("51+"))
    ape = lambda c: F.abs(F.col(c) - F.col("actual")) / F.col("actual")  # noqa: E731
    d = item_day.filter(F.col("actual") > 0).withColumn("bucket", bucket)
    rows = (d.groupBy("bucket").agg(F.count("*").alias("n"), F.avg(ape("forecast")).alias("model"),
                                    F.avg(ape("baseline")).alias("baseline"),
                                    F.avg((F.col("forecast") > F.col("actual")).cast("double"))
                                    .alias("over")).collect())
    medians = d.select(F.percentile_approx(ape("forecast"), 0.5).alias("m"),
                       F.percentile_approx(ape("baseline"), 0.5).alias("b")).first()
    order = ["1-5", "6-20", "21-50", "51+"]
    return sorted(rows, key=lambda r: order.index(r.bucket)), medians


def _fmt(v, kind):
    if v is None:
        return "n/a"
    return f"{v:.1%}" if kind == "pct" else f"{v:,.3f}" if kind == "r2" else f"{v:,.2f}"


def write_report(level_metrics, cfg, info, importances, version, volume):
    h = cfg["horizon_days"]
    lines = [
        "# Demand Forecast Report",
        "",
        f"Generated by `spark_jobs/15_demand_forecasting.py`. Settings from "
        f"`config/forecast_config.yaml`: horizon {h} day(s), lags {cfg['lags']}, rolling windows "
        f"{cfg['rolling_windows']}, test period the last {cfg['test_days']} days. "
        f"Model version `{version}`.",
        "",
        "## Setup",
        "",
        "- **Grain:** item x location x day, over every day the item was stocked at the location "
        "(Inventory rows). A stocked day with no sales is a real zero.",
        f"- **Rows:** a row is a forecast made on `origin_date` with data up to the day before. "
        f"The target is the quantity sold on `target_date` = origin + {h - 1} day(s), so "
        f"horizon 1 means the origin day itself.",
        "- **History features** (known at the origin): `lag_k` = quantity k days before the "
        "origin; `rolling_mean_w` = mean of the w days before the origin; "
        f"`same_weekday_last_week` = quantity {baseline_offset(h)} days before the target date, "
        "the most recent same-weekday value known at the origin.",
        "- **Calendar features** (describe the target date and are known in advance): day of "
        "week, weekend flag, month, seasonal-item flag, promotion active, price in effect.",
        "- Lags and windows join on exact calendar offsets. A row whose history has a gap, or "
        "that is too close to the start of the data, is dropped, not zero-filled. So is a row "
        f"with no price on or before its target date ({info['no_price']:,} rows). In total "
        f"{info['dropped']:,} of {info['candidates']:,} candidate rows are dropped.",
        f"- **Split:** chronological on the target date. Training targets run to "
        f"{info['train_end']}, test targets run from {info['test_start']} to {info['test_end']}. "
        f"Train rows: {info['train_rows']:,}; test rows: {info['test_rows']:,}. There is no "
        "shuffling. Test rows use history from before their own origin only, which can include "
        "late-training days; that data is known at forecast time, so it is not leakage.",
        f"- **Baseline:** `same_weekday_last_week` used directly as the forecast (with horizon ≤ "
        "7 this is `lag_7`). No model.",
        f"- **Model:** Spark MLlib `GBTRegressor` (maxIter {cfg['gbt']['max_iter']}, maxDepth "
        f"{cfg['gbt']['max_depth']}, stepSize {cfg['gbt']['step_size']}). Negative predictions "
        "are clipped to 0 in the outputs and in the metrics.",
        "",
        "## Results on the test period",
        "",
        "MAPE is the mean of |error| / actual over rows with a non-zero actual. At the base grain "
        "many item-location-days sell nothing, so MAPE covers only part of those rows (count "
        "shown). The rolled-up levels have few or no zero days, so MAPE is more meaningful "
        "there.",
        "",
        "| Level | Rows | Zero actuals | Baseline MAE | Model MAE | Baseline RMSE | Model RMSE | "
        "Baseline MAPE | Model MAPE | Baseline R² | Model R² |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for level, (b, m) in level_metrics.items():
        lines.append(
            f"| {level} | {m['n']:,} | {m['zero_actuals']:,} | {_fmt(b['mae'], 'n')} | "
            f"{_fmt(m['mae'], 'n')} | {_fmt(b['rmse'], 'n')} | {_fmt(m['rmse'], 'n')} | "
            f"{_fmt(b['mape'], 'pct')} | {_fmt(m['mape'], 'pct')} | {_fmt(b['r2'], 'r2')} | "
            f"{_fmt(m['r2'], 'r2')} |")
    b, m = level_metrics["item_location_day"]
    beats = m["mae"] < b["mae"] and m["rmse"] < b["rmse"]
    lines += [
        "",
        f"At the base grain the model {'beats' if beats else 'does not beat'} the baseline: MAE "
        f"{m['mae']:.2f} vs {b['mae']:.2f} ({(m['mae'] - b['mae']) / b['mae']:+.1%}), RMSE "
        f"{m['rmse']:.2f} vs {b['rmse']:.2f} ({(m['rmse'] - b['rmse']) / b['rmse']:+.1%}).",
        "",
        "R² is shown at every level; it is n/a only where the actuals have zero variance.",
    ]
    losses = [(lvl, k) for lvl, (bb, mm) in level_metrics.items() for k in ("mae", "rmse", "mape")
              if mm[k] is not None and bb[k] is not None and mm[k] > bb[k]]
    if losses:
        rows, med = volume
        lines += [
            "",
            "### Where the model loses to the baseline",
            "",
            "Worse than the baseline on: " + ", ".join(f"{k.upper()} at {lvl}" for lvl, k in losses)
            + ". Item-day percentage error by daily volume (non-zero actuals):",
            "",
            "| Actual units per item-day | Days | Model MAPE | Baseline MAPE | Model over-forecasts |",
            "|---|---|---|---|---|",
        ]
        lines += [f"| {r.bucket} | {r.n:,} | {r.model:.1%} | {r.baseline:.1%} | {r.over:.0%} |"
                  for r in rows]
        lines += [
            "",
            "The model's forecasts are smoothed averages, so on the few days an item sells only "
            "1-5 units it over-forecasts and the percentage error is large. MAPE averages those "
            "percentages, so a small number of tiny-volume days dominate it. On higher-volume "
            "days the model is clearly better, and the median percentage error favours it "
            f"({med.m:.1%} vs {med.b:.1%} for the baseline). "
            + ("MAE and RMSE, which weight errors by units, improve at every level. "
               if not any(k in ("mae", "rmse") for _, k in losses) else "")
            + "For low-volume items, planners should read the forecast as a unit estimate, "
            "not a percentage.",
        ]
    lines += [
        "",
        "Feature importances: " + ", ".join(f"`{f}` {v:.3f}" for f, v in importances) + "."
        + (f" With horizon {h}, `same_weekday_last_week` is the same day as "
           f"`lag_{baseline_offset(h) - (h - 1)}`, so the trees use that one instead."
           if baseline_offset(h) - (h - 1) in cfg["lags"] else ""),
        "",
        "## Outputs",
        "",
        "`parquet_data/demand_forecast/`, test period only, each with `actual`, `forecast` "
        "(model), `baseline` and `recent_average` (the longest rolling mean at the origin, "
        "i.e. the recent daily level):",
        "",
        "- `item_location_day/`: the base grain, with `origin_date` and `target_date`.",
        "- `item_day/`: summed over locations.",
        "- `category_day/`: summed over the items in each menu category.",
        "- `location_day/`: summed over items.",
        "- `overall_day/`: chain total per day.",
        "",
        "Change the horizon or windows in `config/forecast_config.yaml` and rerun; no code "
        "changes are needed.",
    ]
    REPORT_PATH.write_text("\n".join(lines) + "\n")


def main():
    cfg = load_config()
    logger = JobLogger("forecast")
    spark = get_spark("dineiq-forecast")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    inventory = spark.read.parquet(str(PARQUET_DIR / "inventory"))
    t = {n: load_clean_table(spark, n, PROCESSED_DIR)
         for n in ["Menu_Items", "Menu_Categories", "Pricing_History", "Promotions",
                   "Promotion_Items", "Promotion_Locations"]}
    menu = t["Menu_Items"].join(t["Menu_Categories"], "category_id").select(
        "item_id", "is_seasonal", "category_name")
    coverage = ac.promo_coverage(t["Promotions"], t["Promotion_Items"], t["Promotion_Locations"])

    with logger.timer("build training table"):
        daily = daily_quantity(inventory, fact).cache()
        candidates = daily.count()
        sold_total = fact.filter(F.col("item_id").isNotNull()).agg(F.sum("quantity")).first()[0]
        covered = daily.agg(F.sum("qty")).first()[0]
        logger.log(f"daily rows {candidates:,}; quantity covered by stocked days "
                   f"{covered / sold_total:.4%}")
        table = add_calendar(build_table(daily, cfg), menu, coverage,
                             price_in_effect(t["Pricing_History"], inventory))
        table = table.cache()
        no_price = table.filter(F.col("price").isNull()).count()
        logger.log(f"rows without any price on or before the target date: {no_price:,} (dropped)")
        table = table.filter(F.col("price").isNotNull()).cache()
        n_rows = table.count()
        train, test, test_start = chronological_split(table, cfg["test_days"])
        train, test = train.cache(), test.cache()
        info = dict(candidates=candidates, dropped=candidates - n_rows, no_price=no_price,
                    train_rows=train.count(), test_rows=test.count(),
                    train_end=train.agg(F.max("target_date")).first()[0],
                    test_start=test_start, test_end=test.agg(F.max("target_date")).first()[0])
        assert info["train_end"] < test_start
        logger.log(str(info))

    with logger.timer("train GBTRegressor"):
        model = pipeline(cfg).fit(train)
        pred = model.transform(test).cache()

    views = rollups(pred, menu, cfg)
    level_metrics = {}
    for level in ["item_location_day", "item_day", "category_day", "location_day", "overall_day"]:
        v = views[level]
        level_metrics[level] = (metrics(v, "baseline"), metrics(v, "forecast"))
        logger.log(f"{level}: baseline={level_metrics[level][0]} model={level_metrics[level][1]}")

    version = "v" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    with logger.timer("write outputs"):
        for level, v in views.items():
            v.withColumn("model_version", F.lit(version)).write.mode("overwrite").parquet(
                str(OUT_DIR / level))
        out = MODEL_DIR / version
        model.write().overwrite().save(str(out / "model"))
        (out / "model_version.txt").write_text(json.dumps(
            dict(version=version, algorithm="GBTRegressor", config=cfg,
                 features=feature_columns(cfg), test_start=str(test_start),
                 metrics={k: {"baseline": b, "model": m} for k, (b, m) in level_metrics.items()}),
            indent=2, default=str) + "\n")
        (MODEL_DIR / "LATEST").write_text(version + "\n")

    gbt = model.stages[-1]
    importances = sorted(zip(feature_columns(cfg), gbt.featureImportances.toArray()),
                         key=lambda x: -x[1])
    write_report(level_metrics, cfg, info, importances, version, mape_by_volume(views["item_day"]))
    logger.log(f"wrote {REPORT_PATH}")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

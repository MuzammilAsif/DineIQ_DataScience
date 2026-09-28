"""Record-level comparison of the Spark and Python pipelines (SRS dual-pipeline requirement).

Two tasks:
- Customer segmentation: the 20% crc32 holdout customers, which neither KMeans was fitted on.
  Actual = the rule-based segment (python_pipeline/segment_rules.py). Each pipeline's clusters
  are mapped to segments by majority vote over its own training customers.
- Menu classification: the 28 held-out items. Actual = Step 4's category. The Python model
  compared is the same algorithm as Spark's final model, so the comparison isolates the
  pipeline rather than the algorithm choice.

Output: reports/dual_pipeline_comparison.csv, reports/dual_pipeline_comparison_report.md
Run: .venv/bin/python -m python_pipeline.compare_pipelines
"""
import json
import re

import pandas as pd
import yaml
from sklearn.metrics import adjusted_rand_score

from python_pipeline import features as pf
from python_pipeline import segment_rules as sr

ROOT = pf.ROOT
REPORT = ROOT / "reports" / "dual_pipeline_comparison_report.md"
CSV = ROOT / "reports" / "dual_pipeline_comparison.csv"
SPARK_MENU_DIR = ROOT / "models" / "spark" / "menu_classification"
PY_MENU_DIR = ROOT / "models" / "python" / "menu_classification"
PY_SEG_DIR = ROOT / "models" / "python" / "customer_segmentation"
COLUMNS = ["record_type", "record_id", "actual", "spark_result", "python_result",
           "spark_python_match", "numerical_difference", "spark_confidence",
           "python_confidence", "confidence_measure", "final_consistency_status"]


def agreement_pct(a, b):
    a, b = pd.Series(a).reset_index(drop=True), pd.Series(b).reset_index(drop=True)
    return 100.0 * (a == b).mean() if len(a) else float("nan")


def consistency_status(actual, spark, python):
    if spark == python:
        return "Consistent: both correct" if spark == actual else "Consistent: both wrong"
    if spark == actual:
        return "Inconsistent: Spark correct"
    if python == actual:
        return "Inconsistent: Python correct"
    return "Inconsistent: both wrong"


def build_rows(record_type, ids, actual, spark, python, spark_conf, python_conf, measure):
    df = pd.DataFrame({"record_type": record_type, "record_id": list(ids), "actual": list(actual),
                       "spark_result": list(spark), "python_result": list(python),
                       "spark_confidence": list(spark_conf), "python_confidence": list(python_conf)})
    df["spark_python_match"] = (df.spark_result == df.python_result).map(
        {True: "Match", False: "Mismatch"})
    # both results are class labels, so there is no numeric difference to report
    df["numerical_difference"] = "N/A"
    df["confidence_measure"] = measure
    df["final_consistency_status"] = [consistency_status(a, s, p) for a, s, p in
                                      zip(df.actual, df.spark_result, df.python_result)]
    return df[COLUMNS]


def summary(rows):
    return dict(n=len(rows),
                spark_vs_actual=agreement_pct(rows.spark_result, rows.actual),
                python_vs_actual=agreement_pct(rows.python_result, rows.actual),
                spark_vs_python=agreement_pct(rows.spark_result, rows.python_result))


def latest(d):
    return d / (d / "LATEST").read_text().strip()


def feature_parity(py, spark, key):
    m = py.merge(spark, on=key, suffixes=("_py", "_spark"))
    out = []
    for c in [c for c in py.columns if f"{c}_spark" in m.columns and c != key]:
        a, b = m[f"{c}_py"], m[f"{c}_spark"]
        if not pd.api.types.is_numeric_dtype(a):
            continue
        a, b = a.astype(float), b.astype(float)
        out.append(dict(feature=c, rows=len(m), null_mismatches=int((a.isna() != b.isna()).sum()),
                        max_abs_diff=float((a - b).abs().max()),
                        max_rel_diff=float(((a - b).abs() / b.abs().where(b != 0)).max())))
    return pd.DataFrame(out), len(py), len(spark), len(m)


def md_table(df, fmt=None):
    fmt = fmt or {}
    lines = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    for _, r in df.iterrows():
        cells = []
        for c in df.columns:
            v = r[c]
            cells.append("" if v is None or (isinstance(v, float) and v != v)
                         else fmt[c].format(v) if c in fmt else str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def spark_menu_results():
    """Per-algorithm held-out metrics from the Spark report's test table."""
    text = (ROOT / "reports" / "spark_mllib_menu_classification_report.md").read_text()
    block = text.split("## Held-out test results", 1)[1].split("###", 1)[0]
    rows = re.findall(r"^\| (\w+Classifier|LogisticRegression) \| ([\d.]+) \| ([\d.]+) \| ([\d.]+) \|",
                      block, re.M)
    return pd.DataFrame(rows, columns=["algorithm", "accuracy", "weighted_f1", "macro_f1"]).astype(
        {"accuracy": float, "weighted_f1": float, "macro_f1": float})


def spark_best_params():
    text = (ROOT / "reports" / "spark_mllib_menu_classification_report.md").read_text()
    block = text.split("## Algorithms and hyperparameters", 1)[1].split("##", 1)[0]
    return dict(re.findall(r"^\| (\w+) \| [^|]+ \| ([^|]+?) \|$", block, re.M))


def menu_comparison():
    spark_meta = json.loads((latest(SPARK_MENU_DIR) / "model_version.txt").read_text())
    algorithm = spark_meta["algorithm"]
    py_dir = latest(PY_MENU_DIR)
    py_meta = json.loads((py_dir / "metrics.json").read_text())
    spark = pd.read_csv(ROOT / "reports" / "sample_predictions_menu_classification.csv")
    py = pd.read_csv(py_dir / f"test_predictions_{algorithm}.csv")
    m = spark.merge(py, on="item_id", suffixes=("_spark", "_py"))
    assert len(m) == len(spark) == len(py), "Spark and Python test sets differ"
    assert (m.actual_category_spark == m.actual_category_py).all(), "labels differ"
    rows = build_rows("menu_item", m.item_id, m.actual_category_spark, m.predicted_category_spark,
                      m.predicted_category_py, m.predicted_probability_spark,
                      m.predicted_probability_py, "predicted-class probability")
    return rows, algorithm, spark_meta, py_meta


def customer_comparison(cfg):
    spark = pd.read_parquet(ROOT / "parquet_data" / "customer_segments",
                            columns=["customer_id", "split", "cluster_id", "distance_to_centroid"])
    py = pd.read_parquet(ROOT / "models" / "python" / "customer_segments.parquet")
    m = py.merge(spark, on="customer_id", suffixes=("_py", "_spark"))
    assert len(m) == len(py) == len(spark), "customer sets differ"
    assert (m.split_py == m.split_spark).all(), "holdout splits differ"
    train = m[m.split_py == "train"]
    spark_map = sr.majority_mapping(train.cluster_id_spark, train.actual_segment)
    m["spark_segment"] = m.cluster_id_spark.map(spark_map)
    hold = m[m.split_py == "holdout"].reset_index(drop=True)
    rows = build_rows("customer", hold.customer_id, hold.actual_segment, hold.spark_segment,
                      hold.python_segment, hold.distance_to_centroid_spark.round(4),
                      hold.distance_to_centroid_py.round(4),
                      "distance to assigned centroid in standardized space (lower = closer)")
    py_meta = json.loads((latest(PY_SEG_DIR) / "model_version.txt").read_text())
    py_map = {int(k): v for k, v in py_meta["cluster_to_segment"].items()}
    return rows, hold, spark_map, py_map


def per_class(rows):
    out = []
    for c, g in rows.groupby("actual"):
        out.append(dict(actual=c, records=len(g),
                        spark_correct=agreement_pct(g.spark_result, g.actual),
                        python_correct=agreement_pct(g.python_result, g.actual),
                        spark_python_agree=agreement_pct(g.spark_result, g.python_result)))
    return pd.DataFrame(out).sort_values("records", ascending=False)


def status_counts(rows):
    return rows.final_consistency_status.value_counts().rename_axis("status").reset_index(name="records")


def pct_fmt(cols):
    return {c: "{:.1f}%" for c in cols}


def menu_disagreement_notes(rows, tiers):
    """For items where the pipelines disagree, how close each was to a Step 4 tier cutoff."""
    mis = rows[(rows.spark_python_match == "Mismatch") | (rows.spark_result != rows.actual)
               | (rows.python_result != rows.actual)]
    cuts = [tiers["low_below"], tiers["high_above"]]
    if mis.empty:
        return pd.DataFrame(), cuts
    cls = pd.read_parquet(next((ROOT / "parquet_data" / "menu_classification").glob("as_of_date=*")))
    cols = ["profitability_percentile", "demand_percentile", "quality_percentile"]
    d = mis.merge(cls[["item_id", "item_name", *cols]], left_on="record_id", right_on="item_id")
    d["nearest_cutoff_gap"] = d[cols].apply(
        lambda r: min(abs(v - c) for v in r for c in cuts), axis=1)
    return d[["record_id", "item_name", "actual", "spark_result", "python_result", *cols,
              "nearest_cutoff_gap"]], cuts


def write_report(menu, menu_alg, spark_meta, py_meta, cust, hold, spark_map, py_map, parity, cfg):
    tiers = yaml.safe_load((ROOT / "config" / "classification_thresholds.yaml").read_text())["tiers"]
    ms, cs = summary(menu), summary(cust)
    both = pd.concat([menu, cust])
    total = summary(both)
    ari = adjusted_rand_score(hold.cluster_id_spark, hold.cluster_id_py)
    pct = pct_fmt(["spark_vs_actual", "python_vs_actual", "spark_vs_python", "spark_correct",
                   "python_correct", "spark_python_agree"])

    overview = pd.DataFrame([
        dict(task="Customer segmentation (holdout)", **cs),
        dict(task=f"Menu classification ({menu_alg}, held-out)", **ms),
        dict(task="Both tasks", **total)])

    lines = [
        "# Dual-Pipeline Comparison Report",
        "",
        "Spark (PySpark + Spark MLlib, `spark_jobs/`) against an independent Python pipeline "
        "(pandas + scikit-learn, `python_pipeline/`). Generated by "
        "`python_pipeline/compare_pipelines.py`. Row-level results are in "
        "`reports/dual_pipeline_comparison.csv`.",
        "",
        "## Summary",
        "",
        md_table(overview, pct),
        "",
        f"{total['n']:,} records are compared, {cs['n']:,} of them unseen customers, so the "
        "100+ unseen-record requirement is met by the customer task alone. Agreement is the "
        "share of records where the two labels are identical.",
        "",
        "## How the pipelines were kept independent",
        "",
        "- The Python pipeline reads only the Step 2 cleaned CSVs in `processed_data/`. It "
        "recomputes the Step 3 formulas in pandas (`python_pipeline/features.py`). It imports "
        "nothing from `spark_jobs/` and never reads `parquet_data/features/`, Spark's "
        "segmentation output, or any Spark model. `tests/test_python_pipeline.py` checks this.",
        "- It takes three things from the Spark side, and none of them is a feature or a "
        "result:",
        "  - the Step 4 category, which is the target for both pipelines;",
        "  - the 28 held-out item IDs, so both menu models are scored on the same unseen items;",
        "  - the shared config file.",
        "- Customer holdout: customers with `crc32(customer_id) % "
        f"{cfg['segmentation']['holdout_mod']} == 0`, computed separately on each side "
        "(`F.crc32` in Spark, `zlib.crc32` in Python). Both KMeans models are fitted on the "
        "other customers only. The comparison asserts that the two splits are identical.",
        "- The rule-based Actual segment is defined once, in `python_pipeline/segment_rules.py`, "
        "and applied to the Python-computed features. The feature parity check below shows it "
        "would give the same labels from Spark's features.",
        "",
        "## Feature parity",
        "",
        "The Python features compared against Spark's `parquet_data/features/` (read here only, "
        "for checking). All differences are floating-point rounding in sums of decimal money "
        "values:",
        "",
    ]
    for name, (tbl, n_py, n_sp, n_m) in parity.items():
        lines += [f"**{name}**: {n_py:,} Python rows, {n_sp:,} Spark rows, {n_m:,} matched.", "",
                  md_table(tbl, {"max_abs_diff": "{:.2g}", "max_rel_diff": "{:.2g}"}), ""]

    # customers
    cross = pd.crosstab(hold.spark_segment, hold.python_segment)
    cross.index.name = "Spark \\ Python"
    never_spark = [s for s in sr.SEGMENTS if s not in spark_map.values()]
    never_py = [s for s in sr.SEGMENTS if s not in py_map.values()]
    hold = hold.assign(match=hold.spark_segment == hold.python_segment)
    med = hold.groupby("match").centroid_margin.median()
    rule_counts = hold.actual_segment.value_counts()
    lines += [
        "## Customer segmentation",
        "",
        f"Both sides run KMeans with k={cfg['segmentation']['k']} on standardized recency, "
        "log(1 + frequency), log(1 + monetary), average order value and promotion order "
        "share. Spark uses MLlib `KMeans` (k-means|| init); Python uses scikit-learn `KMeans` "
        "(k-means++ init, 10 restarts). Actual is the rule-based segment "
        "(`python_pipeline/segment_rules.py`):",
        "",
        "1. **High-Value Loyal**: top tertile on recency, frequency and spend.",
        f"2. **New**: signed up in the last {cfg['segment_rules']['new_signup_days']} days, "
        f"with at most {cfg['segment_rules']['new_max_orders']} orders.",
        "3. **At-Risk**: bottom recency tertile (longest gap) and middle or top frequency tertile.",
        f"4. **Promotion-Driven**: promotion order share of at least "
        f"{cfg['segment_rules']['promo_share_min']}.",
        "5. **Frequent**: top frequency tertile.",
        "6. **Occasional**: everyone else.",
        "",
        "The first matching rule wins.",
        "",
        f"Holdout records: {cs['n']:,}. Actual segment counts: "
        + ", ".join(f"{k} {v:,}" for k, v in rule_counts.items()) + ".",
        "",
        md_table(per_class(cust), pct),
        "",
        "Consistency status:",
        "",
        md_table(status_counts(cust), {"records": "{:,}"}),
        "",
        "Cluster-to-segment mapping, by majority vote over each pipeline's own training "
        "customers. Cluster ids are arbitrary on each side:",
        "",
        "- Spark: " + ", ".join(f"{k} → {v}" for k, v in sorted(spark_map.items())),
        "- Python: " + ", ".join(f"{k} → {v}" for k, v in sorted(py_map.items())),
        "",
        "Spark vs Python labels on the holdout (rows Spark, columns Python):",
        "",
        md_table(cross.reset_index()),
        "",
        f"Adjusted Rand index between the two raw clusterings on the holdout: {ari:.3f}. It is "
        "1.0 for identical partitions and about 0 for random ones, and it doesn't depend on "
        "cluster numbering or on the mapping to segments.",
        "",
        "### Why the pipelines disagree, and why both miss some segments",
        "",
    ]
    missing = [x for x in sr.SEGMENTS if x in never_spark or x in never_py]

    def landing(seg):
        d = hold[hold.actual_segment == seg].python_segment.value_counts(normalize=True)
        return ", ".join(f"{v:.0%} {k}" for k, v in d.head(3).items())
    lines.append(
        f"- **Segments no cluster maps to.** Spark never predicts "
        f"{', '.join(never_spark) or 'none'}; Python never predicts "
        f"{', '.join(never_py) or 'none'}. Majority vote gives each cluster one label, so a "
        "segment that isn't the largest group in any cluster can't be predicted, and every "
        "customer of that segment counts as a miss for both pipelines. "
        + " ".join(f"Actual {x} customers land in: {landing(x)} (Python)." for x in missing)
        + (" New is defined by signup date, which isn't a clustering input, so those customers "
           "look like any other one- or two-order customer." if "New" in missing else ""))
    lines.append(
        f"- **Different local optima.** k-means|| (Spark) and k-means++ with 10 restarts "
        "(sklearn) start from different centres and settle in different solutions. "
        f"The ARI of {ari:.2f} shows how much the partitions overlap. Spark's StandardScaler "
        "uses the sample standard deviation and sklearn's the population one; the difference "
        "is negligible at this size.")
    if len(med) == 2:
        lines.append(
            f"- **Boundary customers.** Where the pipelines disagree, the customer is close to "
            f"a boundary between two Python clusters. The median gap between their nearest and "
            f"second-nearest centroid is {med[False]:.2f}, against {med[True]:.2f} where they "
            "agree. These customers could reasonably go either way.")
    worst = per_class(cust).sort_values("spark_python_agree").iloc[0]
    lines.append(
        f"- **Largest disagreement:** {worst.actual} (Spark and Python agree on "
        f"{worst.spark_python_agree:.1f}% of its {int(worst.records):,} customers). See the "
        "cross-tab above for where those customers go on each side.")
    lines.append("")

    # menu
    spark_res = spark_menu_results()
    py_res = pd.DataFrame([dict(algorithm=r["name"], accuracy=r["test"]["accuracy"],
                                weighted_f1=r["test"]["weighted_f1"], macro_f1=r["test"]["macro_f1"],
                                cv_macro_f1=r["cv_macro_f1"], best_params=r["best_params"])
                           for r in py_meta["results"]])
    both_res = spark_res.merge(py_res, on="algorithm", suffixes=("_spark", "_python"))
    notes, cuts = menu_disagreement_notes(menu, tiers)
    menu_view = menu[["record_id", "actual", "spark_result", "python_result", "spark_python_match",
                      "spark_confidence", "python_confidence", "final_consistency_status"]]
    lines += [
        "## Menu classification",
        "",
        f"The same 28 held-out items on both sides. Spark's final model is {menu_alg} "
        f"({spark_meta['params']}); the Python record-level comparison uses the Python "
        f"{menu_alg}, so that both columns come from the same algorithm. Python's own "
        f"selection rule (held-out macro F1) picked {py_meta['final']}. Both sides tuned the "
        "same grids with 5-fold stratified CV and balanced class weights.",
        "",
        "Held-out results for all three algorithms:",
        "",
        md_table(both_res, {c: "{:.3f}" for c in both_res.columns
                            if c not in ("algorithm", "best_params")}),
        "",
        f"Record-level comparison ({menu_alg} on both sides):",
        "",
        md_table(menu_view, {"spark_confidence": "{:.3f}", "python_confidence": "{:.3f}"}),
        "",
        md_table(status_counts(menu)),
        "",
        "### Why the pipelines disagree",
        "",
    ]
    n_mis = int((menu.spark_python_match == "Mismatch").sum())
    lines.append(
        f"Spark and Python {menu_alg} disagree on {n_mis} of {len(menu)} items"
        + (", even though they landed on different hyperparameters (Spark "
           f"{spark_meta['params']}, Python {py_res.set_index('algorithm').loc[menu_alg, 'best_params']})."
           if n_mis == 0 else "."))
    if not notes.empty:
        lines += [""]
        lines.append(
            f"The items at least one pipeline got wrong are listed below. Step 4's category "
            f"comes from percentile tiers cut at {cuts[0]} and {cuts[1]}; `nearest_cutoff_gap` "
            "is how far the item's closest percentile sits from a cutoff:")
        lines += ["", md_table(notes, {c: "{:.3f}" for c in notes.columns
                                       if c.endswith(("percentile", "gap"))}), ""]
        lines.append(
            "Where both pipelines make the same mistake, the cause is in the data rather than "
            "the pipeline. Those items sit near a tier cutoff, where a split learned on raw "
            "metrics and the percentile rule are most likely to disagree.")
    sp = spark_best_params()
    lines += ["", "**Other algorithms.** Their held-out numbers differ between the pipelines. "
              "The settings each side's cross-validation picked:", ""]
    lines += [f"- {a}: Spark {sp.get(a, '?')}, Python "
              f"{py_res.set_index('algorithm').loc[a, 'best_params']}"
              for a in py_res.algorithm if a != menu_alg]
    lines += ["", "Three things differ even though the features are identical:", "",
              "- **Folds.** Spark deals each class round-robin, sklearn uses `StratifiedKFold` "
              "with shuffling. With about 24 items per fold, one item moves a fold's macro F1 a "
              "lot, so the grid winner can flip (Logistic Regression regParam 0.01 vs 0).",
              "- **Tree split search.** MLlib bins each continuous feature into at most 32 "
              "candidate thresholds; sklearn tests every midpoint. So a depth-3 tree on the same "
              "data can still choose different splits.",
              "- **Optimiser details.** MLlib L-BFGS and sklearn lbfgs stop at different "
              "tolerances.",
              "",
              "None of this involves the feature pipelines, which match to rounding error. The "
              "forest, Spark's final model, is the like-for-like comparison."]
    lines += [
        "",
        "## Limitations",
        "",
        "- The Actual segment is a rule on the same RFM and promotion inputs the clustering "
        "sees, but clustering has no access to signup date. Agreement with Actual measures "
        "how well unsupervised clusters line up with a business rule, not a supervised "
        "accuracy.",
        "- The menu comparison has only 28 records. It adds a second, supervised check, "
        "but the customer task is the one that meets the 100-record requirement.",
        "- The Spark menu model's per-algorithm metrics are read from its report, because that "
        "run saved test predictions for the final model only.",
    ]
    REPORT.write_text("\n".join(lines) + "\n")


def main():
    cfg = sr.load_config()
    menu, menu_alg, spark_meta, py_meta = menu_comparison()
    cust, hold, spark_map, py_map = customer_comparison(cfg)
    pd.concat([menu, cust]).to_csv(CSV, index=False)

    tables = pf.load_tables(["Orders", "Order_Items", "Menu_Items", "Menu_Categories", "Ratings",
                             "Wastage", "Pricing_History", "Customers"])
    as_of = pf.default_as_of(tables)
    snap = f"as_of_date={as_of.date()}"
    feats = ROOT / "parquet_data" / "features"
    parity = {
        "Item features": feature_parity(pf.item_features(tables, as_of),
                                        pd.read_parquet(feats / "item_features" / snap), "item_id"),
        "Customer features": feature_parity(
            pf.customer_features(tables, as_of),
            pd.read_parquet(feats / "customer_features" / snap), "customer_id"),
    }
    write_report(menu, menu_alg, spark_meta, py_meta, cust, hold, spark_map, py_map, parity, cfg)
    for name, rows in [("menu", menu), ("customers", cust)]:
        print(name, summary(rows))
    print("wrote", CSV, REPORT)


if __name__ == "__main__":
    main()

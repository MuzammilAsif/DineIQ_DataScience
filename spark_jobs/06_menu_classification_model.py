"""Step 5 — Spark MLlib menu performance classification model (SRS Step 12).

Predicts the Step 4 category from Step 3 raw item metrics. Trains LogisticRegression,
DecisionTreeClassifier and RandomForestClassifier, tunes each with 5-fold CrossValidator
on a stratified 80% split, and picks the final model by macro F1 on the held-out 20%.

Outputs:
  models/spark/menu_classification/<version>/model/            winning PipelineModel
  models/spark/menu_classification/<version>/model_version.txt
  models/spark/menu_classification/LATEST
  reports/spark_mllib_menu_classification_report.md
  reports/sample_predictions_menu_classification.csv

Run: .venv/bin/python spark_jobs/06_menu_classification_model.py [as_of_date]
"""
import datetime as dt
import json
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (DecisionTreeClassifier, LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.evaluation import Evaluator
from pyspark.ml.feature import (Imputer, IndexToString, StandardScaler, StringIndexer,
                                VectorAssembler)
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spark_utils import JobLogger, PARQUET_DIR, REPORTS_DIR, ROOT, get_spark  # noqa: E402

FEATURES_DIR = PARQUET_DIR / "features" / "item_features"
LABELS_DIR = PARQUET_DIR / "menu_classification"
MODEL_DIR = ROOT / "models" / "spark" / "menu_classification"
REPORT_PATH = REPORTS_DIR / "spark_mllib_menu_classification_report.md"
SAMPLE_PATH = REPORTS_DIR / "sample_predictions_menu_classification.csv"

FEATURES = [
    "item_revenue", "item_cost", "contribution_margin", "profit_percentage",
    "total_quantity_sold", "item_popularity", "order_frequency", "repeat_purchase_rate",
    "average_rating", "rating_trend", "wastage_percentage", "promotion_dependency",
    "discount_percentage", "price_change_percentage", "weekend_order_ratio", "sales_trend",
]

# Nulls here mean "no change" / "no clear trend", not missing data.
ZERO_FILL = ["price_change_percentage", "rating_trend", "sales_trend"]

# Step 4's decision inputs. None of these may reach the model.
LEAKAGE_COLUMNS = [
    "category", "quality_score",
    "profitability_percentile", "demand_percentile", "quality_percentile", "wastage_percentile",
    "profitability_tier", "demand_tier", "quality_tier", "wastage_tier",
    "loss_making", "rarely_purchased_high_margin", "excessive_wastage", "high_rating_low_profit",
    "low_rating_high_sales", "promotion_dependent", "weekend_skewed", "seasonal_item",
    "location_inconsistent", "insufficient_history",
]

EXCLUDED_CATEGORY = "Insufficient History"
SEED = 42
TEST_FRACTION = 0.2
N_FOLDS = 5
MIN_TRUSTED_TEST = 10

IMPUTED = [f"{c}_imp" for c in FEATURES]


def load_dataset(spark, as_of_date):
    feats = spark.read.parquet(str(FEATURES_DIR / f"as_of_date={as_of_date}"))
    labels = spark.read.parquet(str(LABELS_DIR / f"as_of_date={as_of_date}")).select(
        "item_id", "category")
    return prepare(feats.join(labels, "item_id"))


def prepare(df):
    df = df.filter(F.col("category") != EXCLUDED_CATEGORY)
    df = df.fillna(0.0, subset=ZERO_FILL)
    return df.select("item_id", "category",
                     *[F.col(c).cast("double").alias(c) for c in FEATURES])


def label_indexer(df):
    # alphabetical order keeps label indices stable across reruns
    return StringIndexer(inputCol="category", outputCol="label",
                         stringOrderType="alphabetAsc").fit(df)


def stratified_split(df, fraction=TEST_FRACTION, seed=SEED):
    cats = [r.category for r in df.select("category").distinct().collect()]
    test = df.sampleBy("category", {c: fraction for c in cats}, seed)
    train = df.join(test.select("item_id"), "item_id", "left_anti")
    return train, test


def add_folds(df, n_folds=N_FOLDS, seed=SEED):
    """Deals each class round-robin across folds so every fold holds every class."""
    w = Window.partitionBy("category").orderBy(F.rand(seed))
    return df.withColumn("fold", ((F.row_number().over(w) - 1) % n_folds).cast("int"))


def add_class_weights(df):
    """Balanced weights, n / (k * n_class), so small classes count in the loss."""
    counts = {r.category: r["count"] for r in df.groupBy("category").count().collect()}
    n, k = sum(counts.values()), len(counts)
    mapping = F.create_map(*[x for c, cnt in counts.items() for x in (F.lit(c), F.lit(n / (k * cnt)))])
    return df.withColumn("weight", mapping[F.col("category")])


def per_class_metrics(pred_label_pairs, n_classes):
    """pred_label_pairs: RDD of (prediction, label). Returns dict with confusion matrix and scores.

    Macro F1 averages fMeasure over the classes present in the actual labels. MulticlassMetrics
    raises for a class that is only predicted, never actual; a class never predicted gets F1 0.
    """
    m = MulticlassMetrics(pred_label_pairs)
    present = sorted({lbl for _, lbl in pred_label_pairs.collect()})
    per_class = {}
    for i in range(n_classes):
        if float(i) in present:
            per_class[i] = dict(precision=m.precision(float(i)), recall=m.recall(float(i)),
                                f1=m.fMeasure(float(i)))
    macro = sum(v["f1"] for v in per_class.values()) / len(per_class)
    return dict(accuracy=m.accuracy, weighted_f1=m.weightedFMeasure(), macro_f1=macro,
                per_class=per_class, confusion=m.confusionMatrix().toArray().tolist(),
                labels=present)


def to_pairs(predictions):
    return predictions.select(F.col("prediction").cast("double"),
                              F.col("label").cast("double")).rdd.map(tuple)


def macro_f1(predictions, n_classes):
    return per_class_metrics(to_pairs(predictions), n_classes)["macro_f1"]


class MacroF1Evaluator(Evaluator):
    """CrossValidator evaluator. MulticlassClassificationEvaluator's "f1" is weighted F1."""

    def __init__(self, n_classes):
        super().__init__()
        self.n_classes = n_classes

    def _evaluate(self, dataset):
        return macro_f1(dataset, self.n_classes)

    def isLargerBetter(self):
        return True


def build_pipeline(classifier, labels, scale):
    stages = [
        Imputer(inputCols=FEATURES, outputCols=IMPUTED, strategy="median"),
        VectorAssembler(inputCols=IMPUTED, outputCol="raw_features" if scale else "features"),
    ]
    if scale:
        stages.append(StandardScaler(inputCol="raw_features", outputCol="features",
                                     withMean=True, withStd=True))
    stages += [classifier,
               IndexToString(inputCol="prediction", outputCol="predicted_category", labels=labels)]
    return Pipeline(stages=stages)


def candidates(labels, rf_trees=(30, 80), rf_depth=(5, 10)):
    """(name, pipeline, param grid, grid description) per algorithm."""
    lr = LogisticRegression(family="multinomial", weightCol="weight", maxIter=200)
    dt_ = DecisionTreeClassifier(weightCol="weight", seed=SEED)
    rf = RandomForestClassifier(weightCol="weight", seed=SEED)
    return [
        ("LogisticRegression", build_pipeline(lr, labels, scale=True),
         ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.01, 0.1]).build(),
         {"regParam": [0.0, 0.01, 0.1]}),
        ("DecisionTreeClassifier", build_pipeline(dt_, labels, scale=False),
         ParamGridBuilder().addGrid(dt_.maxDepth, [3, 5, 8]).build(),
         {"maxDepth": [3, 5, 8]}),
        ("RandomForestClassifier", build_pipeline(rf, labels, scale=False),
         ParamGridBuilder().addGrid(rf.numTrees, list(rf_trees))
         .addGrid(rf.maxDepth, list(rf_depth)).build(),
         {"numTrees": list(rf_trees), "maxDepth": list(rf_depth)}),
    ]


def param_str(param_map):
    return ", ".join(f"{p.name}={v}" for p, v in param_map.items())


def train_and_evaluate(train, test, labels, rf_trees=(30, 80), rf_depth=(5, 10)):
    """Tunes every candidate on train, scores on test. Returns one result dict per algorithm."""
    n_classes = len(labels)
    evaluator = MacroF1Evaluator(n_classes)
    results = []
    for name, pipeline, grid, grid_desc in candidates(labels, rf_trees, rf_depth):
        cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator,
                            numFolds=N_FOLDS, foldCol="fold", seed=SEED)
        cv_model = cv.fit(train)
        best_idx = max(range(len(grid)), key=lambda i: cv_model.avgMetrics[i])
        best = cv_model.bestModel
        train_metrics = per_class_metrics(to_pairs(best.transform(train)), n_classes)
        test_pred = best.transform(test)
        results.append(dict(
            name=name, model=best, grid=grid_desc,
            cv_scores=[(param_str(pm), m, s) for pm, m, s in
                       zip(grid, cv_model.avgMetrics, cv_model.stdMetrics)],
            best_params=param_str(grid[best_idx]), cv_macro_f1=cv_model.avgMetrics[best_idx],
            train=train_metrics, test=per_class_metrics(to_pairs(test_pred), n_classes),
            test_predictions=test_pred,
        ))
    return results


def select_final(results):
    # Held-out macro F1 decides; CV macro F1 breaks ties, which are likely on ~30 test rows.
    return max(results, key=lambda r: (round(r["test"]["macro_f1"], 6), r["cv_macro_f1"]))


def save_model(model, version, meta, base=MODEL_DIR):
    out = base / version
    model.write().overwrite().save(str(out / "model"))
    (out / "model_version.txt").write_text(json.dumps(dict(version=version, **meta), indent=2) + "\n")
    (base / "LATEST").write_text(version + "\n")
    return out


def load_model(version, base=MODEL_DIR):
    return PipelineModel.load(str(base / version / "model"))


def sample_predictions(test_pred, version, n=None):
    df = (test_pred
          .withColumn("predicted_probability",
                      F.array_max(vector_to_array(F.col("probability"))))
          .select("item_id", F.col("category").alias("actual_category"), "predicted_category",
                  F.round("predicted_probability", 4).alias("predicted_probability"),
                  F.lit(version).alias("model_version"))
          .orderBy("actual_category", "item_id"))
    return df.limit(n) if n else df


def _f(v, d=3):
    return f"{v:.{d}f}"


def write_report(path, as_of, counts, train_counts, test_counts, results, final, labels, version,
                 model_path):
    lines = [
        "# Spark MLlib Menu Classification Report",
        "",
        f"Snapshot `as_of_date={as_of}`. Generated by `spark_jobs/06_menu_classification_model.py`. "
        f"Final model version `{version}`, saved to `{model_path.relative_to(ROOT)}/`.",
        "",
        "## Task",
        "",
        "Predict the Step 4 menu performance category (Profit Driver, Volume Driver, Hidden "
        "Opportunity, Low Performer) from Step 3 raw item metrics. Items labelled Insufficient "
        "History are dropped: that label comes from item age, not performance.",
        "",
        "| Category | All | Train (80%) | Test (20%) |",
        "|---|---|---|---|",
    ]
    lines += [f"| {c} | {counts.get(c, 0)} | {train_counts.get(c, 0)} | {test_counts.get(c, 0)} |"
              for c in labels]
    lines += [f"| **Total** | {sum(counts.values())} | {sum(train_counts.values())} | "
              f"{sum(test_counts.values())} |"]

    lines += [
        "",
        "## Features used",
        "",
        "Only raw metrics from `parquet_data/features/item_features/`:",
        "",
        ", ".join(f"`{c}`" for c in FEATURES) + ".",
        "",
        "Excluded because Step 4 built the label from them: the four percentiles and tiers "
        "(profitability, demand, quality, wastage), `quality_score`, all ten flags and "
        "`category` itself. `tests/test_menu_classification_model.py` asserts none of them "
        "reaches the feature vector.",
        "",
        "The label is still a deterministic function of percentile ranks of `profit_percentage`, "
        "`total_quantity_sold`, `wastage_percentage`, `average_rating` and `repeat_purchase_rate`, "
        "all of which are features here in raw form. A percentile is a monotonic transform of "
        "the raw value within one snapshot, so tree models can approximate the rule's cut points "
        "closely. High scores are expected for that reason. The learned cut points are absolute "
        "values, while the rule's are relative to each snapshot's population, so the model "
        "needs retraining when the menu's overall distribution shifts.",
        "",
        "**Preprocessing:**",
        "",
        f"- Nulls in {', '.join(f'`{c}`' for c in ZERO_FILL)} are set to 0. A null price change "
        "means the price never changed, and a null trend means no clear trend (under 3 months "
        "of data). These are values, not defects, so no rows are dropped.",
        "- Any remaining null is imputed with the training median (`Imputer` inside the "
        "pipeline). The current snapshot has none after the step above.",
        "- `StringIndexer` on `category`, alphabetical order, fitted once on the whole set so "
        "label indices are fixed. `IndexToString` at the end of the pipeline decodes predictions.",
        "- `VectorAssembler` over the 16 features. `StandardScaler` (mean and std) only in the "
        "Logistic Regression pipeline; the tree pipelines use the unscaled vector.",
        "- Balanced class weights, `n / (k * n_class)`, computed on the training portion and "
        "passed as `weightCol`. Profit Driver has about 5x fewer items than Low Performer, and "
        "macro F1 weights every class equally.",
        "",
        "## Split and cross-validation",
        "",
        f"- Held-out test set: `sampleBy(\"category\", {TEST_FRACTION} per class, seed={SEED})`. "
        "`sampleBy` samples each row independently, so class counts are close to 20% but not "
        "exact.",
        f"- The remaining rows go to `CrossValidator` with {N_FOLDS} folds. Folds are assigned "
        "per class, round-robin in random order (`foldCol`), so every fold holds every class. "
        "Spark's default random folds could leave a fold with no Profit Driver.",
        "- Tuning metric: macro F1 through a custom evaluator. Spark's "
        "`MulticlassClassificationEvaluator(metricName=\"f1\")` returns weighted F1, which "
        "over-weights the large classes.",
        "- Macro F1 is the unweighted mean of `MulticlassMetrics.fMeasure(label)` over the four "
        "classes.",
        "",
        "## Algorithms and hyperparameters",
        "",
        "GBTClassifier is left out because Spark MLlib's GBT is binary-only and this is a "
        "4-class problem.",
        "",
        "| Algorithm | Grid | Best (by CV macro F1) |",
        "|---|---|---|",
    ]
    for r in results:
        grid = "; ".join(f"{k} in {v}" for k, v in r["grid"].items())
        lines.append(f"| {r['name']} | {grid} | {r['best_params']} |")

    lines += ["", "## Training and cross-validation results", "",
              "Mean and standard deviation of macro F1 across the 5 validation folds, per grid "
              "point.", ""]
    for r in results:
        lines += [f"**{r['name']}**", "", "| Params | CV macro F1 (mean) | Std |", "|---|---|---|"]
        lines += [f"| {p} | {_f(m)} | {_f(s)} |" for p, m, s in r["cv_scores"]]
        lines.append("")
    lines += ["Best configuration per algorithm, refit on the full training portion:", "",
              "| Algorithm | CV macro F1 | Train accuracy | Train macro F1 |", "|---|---|---|---|"]
    lines += [f"| {r['name']} | {_f(r['cv_macro_f1'])} | {_f(r['train']['accuracy'])} | "
              f"{_f(r['train']['macro_f1'])} |" for r in results]

    lines += ["", "## Held-out test results", "",
              f"{sum(test_counts.values())} items never seen during training or tuning.", "",
              "| Algorithm | Accuracy | Weighted F1 | Macro F1 |", "|---|---|---|---|"]
    lines += [f"| {r['name']} | {_f(r['test']['accuracy'])} | {_f(r['test']['weighted_f1'])} | "
              f"{_f(r['test']['macro_f1'])} |" for r in results]

    for r in results:
        lines += ["", f"### {r['name']}", "",
                  "| Class | Test n | Precision | Recall | F1 |", "|---|---|---|---|---|"]
        for i, c in enumerate(labels):
            pc = r["test"]["per_class"].get(i)
            if pc is None:
                lines.append(f"| {c} | 0 | - | - | - |")
                continue
            lines.append(f"| {c} | {test_counts.get(c, 0)} | {_f(pc['precision'])} | "
                         f"{_f(pc['recall'])} | {_f(pc['f1'])} |")
        lines += ["", "Confusion matrix (rows actual, columns predicted):", "",
                  "| | " + " | ".join(labels) + " |", "|---" * (len(labels) + 1) + "|"]
        conf = r["test"]["confusion"]
        present = [int(x) for x in r["test"]["labels"]]
        for i, c in enumerate(labels):
            row = conf[present.index(i)] if i in present else [0] * len(present)
            cells = [int(row[present.index(j)]) if j in present else 0 for j in range(len(labels))]
            lines.append(f"| **{c}** | " + " | ".join(str(x) for x in cells) + " |")

    small = [c for c in labels if test_counts.get(c, 0) < MIN_TRUSTED_TEST]
    fm = final["test"]
    total = sum(counts.values())
    big, tiny = max(counts, key=counts.get), min(counts, key=counts.get)
    cv_std = next(s for p, m, s in final["cv_scores"] if p == final["best_params"])
    gap = abs(fm["macro_f1"] - final["cv_macro_f1"])
    min_n = min(test_counts.get(c, 0) for c in labels)
    best_cv = max(results, key=lambda r: r["cv_macro_f1"])
    lines += [
        "",
        "## Final model",
        "",
        f"**{final['name']}** ({final['best_params']}): held-out macro F1 {_f(fm['macro_f1'])}, "
        f"accuracy {_f(fm['accuracy'])}, weighted F1 {_f(fm['weighted_f1'])}.",
        "",
        "Selected by held-out macro F1, with CV macro F1 as the tie-break. Accuracy is not the "
        f"selection metric: with {big} at {counts[big] / total:.0%} of items and {tiny} at "
        f"{counts[tiny] / total:.0%}, a model can score well on accuracy while missing {tiny}s, "
        "and macro F1 penalises that.",
        "",
        f"SRS target: accuracy >= 0.85 and macro F1 >= 0.80. Result: accuracy "
        f"{_f(fm['accuracy'])} ({'met' if fm['accuracy'] >= 0.85 else 'not met'}), macro F1 "
        f"{_f(fm['macro_f1'])} ({'met' if fm['macro_f1'] >= 0.80 else 'not met'}). These are "
        "the real held-out numbers; no hyperparameter was tuned on the test set.",
        "",
        "Choosing among the three tuned models by test score is itself a selection on the test "
        "set, so the winner's test figure is somewhat optimistic."
        + (f" {best_cv['name']} had the best CV macro F1 ({_f(best_cv['cv_macro_f1'])}) but a "
           f"test macro F1 of {_f(best_cv['test']['macro_f1'])}, which shows how much a "
           f"{sum(test_counts.values())}-item test set can reorder the models."
           if best_cv is not final else ""),
        "",
        "## Reliability of these numbers",
        "",
    ]
    if small:
        lines += [
            f"Every class with fewer than {MIN_TRUSTED_TEST} test items is too small for its "
            "per-class metric to be trusted: "
            + ", ".join(f"{c} ({test_counts.get(c, 0)})" for c in small) + ".",
            "",
            "One misclassified item moves a class's recall by 1/n: for the smallest class "
            f"({min_n} items) that is {1 / min_n:.0%}. The test set has {sum(test_counts.values())} items, so "
            f"one error moves accuracy by {1 / sum(test_counts.values()):.1%}. Treat the per-class "
            "test figures as indicative only. The CV macro F1 (5 folds, every training item "
            f"scored once) is the more stable estimate: {_f(final['cv_macro_f1'])} +/- "
            f"{_f(cv_std)} for the final model, against {_f(fm['macro_f1'])} held out, a gap of "
            f"{_f(gap)} ({'within' if gap <= cv_std else 'outside'} one fold standard deviation).",
            "",
        ]
    lines += [
        "There are only ~150 labelled items, so this task will not be used for the SRS's "
        "100+ unseen-record dual-pipeline comparison. That will be a higher-row task in a later "
        "step. This step covers the requirement to train and compare at least three Spark MLlib "
        "algorithms.",
        "",
        "## Model version tracking",
        "",
        f"- `models/spark/menu_classification/{version}/model/`: the fitted `PipelineModel` "
        "(imputer, assembler, scaler if any, classifier, label decoder).",
        f"- `models/spark/menu_classification/{version}/model_version.txt`: version, algorithm, "
        "parameters, training snapshot and test metrics.",
        "- `models/spark/menu_classification/LATEST`: the current version string.",
        f"- `{SAMPLE_PATH.relative_to(ROOT)}`: held-out predictions, each row tagged with "
        "`model_version`.",
    ]
    path.write_text("\n".join(lines) + "\n")


def latest_snapshot():
    feats = {p.name.split("=", 1)[1] for p in FEATURES_DIR.glob("as_of_date=*")}
    labels = {p.name.split("=", 1)[1] for p in LABELS_DIR.glob("as_of_date=*")}
    return max(feats & labels)


def main():
    as_of = sys.argv[1] if len(sys.argv) > 1 else latest_snapshot()
    logger = JobLogger("menu_model")
    spark = get_spark("dineiq-menu-model")
    spark.sparkContext.setLogLevel("WARN")
    logger.log(f"as_of_date={as_of}")

    with logger.timer("load and prepare"):
        data = load_dataset(spark, as_of)
        indexer = label_indexer(data)
        labels = list(indexer.labels)
        data = indexer.transform(data).cache()
        train, test = stratified_split(data)
        train = add_class_weights(add_folds(train)).cache()
        test = test.cache()
        counts = {r.category: r["count"] for r in data.groupBy("category").count().collect()}
        train_counts = {r.category: r["count"] for r in train.groupBy("category").count().collect()}
        test_counts = {r.category: r["count"] for r in test.groupBy("category").count().collect()}
        logger.log(f"labels={labels} all={counts} train={train_counts} test={test_counts}")

    with logger.timer("cross-validate and evaluate 3 algorithms"):
        results = train_and_evaluate(train, test, labels)
    for r in results:
        logger.log(f"{r['name']} best={r['best_params']} cv_macro_f1={r['cv_macro_f1']:.4f} "
                   f"test_acc={r['test']['accuracy']:.4f} test_weighted_f1="
                   f"{r['test']['weighted_f1']:.4f} test_macro_f1={r['test']['macro_f1']:.4f}")

    final = select_final(results)
    version = "v" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    meta = dict(algorithm=final["name"], params=final["best_params"], as_of_date=as_of,
                features=FEATURES, labels=labels, seed=SEED,
                cv_macro_f1=final["cv_macro_f1"],
                test_accuracy=final["test"]["accuracy"],
                test_weighted_f1=final["test"]["weighted_f1"],
                test_macro_f1=final["test"]["macro_f1"],
                trained_at=dt.datetime.now().isoformat(timespec="seconds"))
    with logger.timer(f"save final model {version}"):
        model_path = save_model(final["model"], version, meta)
        logger.log(f"final={final['name']} saved to {model_path}")

    sample_predictions(final["test_predictions"], version).toPandas().to_csv(SAMPLE_PATH, index=False)
    logger.log(f"wrote {SAMPLE_PATH}")
    write_report(REPORT_PATH, as_of, counts, train_counts, test_counts, results, final, labels,
                 version, model_path)
    logger.log(f"wrote {REPORT_PATH}")
    logger.log("menu classification model job complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

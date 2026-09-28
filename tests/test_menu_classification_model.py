"""Tests for spark_jobs/06_menu_classification_model.py on small synthetic feature rows."""
import importlib.util
import random
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "menu_classification_model",
    Path(__file__).resolve().parent.parent / "spark_jobs" / "06_menu_classification_model.py",
)
mm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mm)

CATS = ["Hidden Opportunity", "Low Performer", "Profit Driver", "Volume Driver"]


def synthetic_rows(n_per_class=10, seed=0):
    """Each class sits in its own region of (quantity, profit %) so a model can separate them."""
    rng = random.Random(seed)
    centres = {"Profit Driver": (800, 80.0), "Volume Driver": (800, 30.0),
               "Hidden Opportunity": (100, 80.0), "Low Performer": (100, 30.0)}
    rows = []
    for cat, (qty, profit) in centres.items():
        for i in range(n_per_class):
            r = {c: rng.uniform(0, 1) for c in mm.FEATURES}
            r["total_quantity_sold"] = qty + rng.uniform(-50, 50)
            r["profit_percentage"] = profit + rng.uniform(-5, 5)
            r["price_change_percentage"] = None if i % 3 == 0 else r["price_change_percentage"]
            r["rating_trend"] = None if i % 4 == 0 else r["rating_trend"]
            r.update(item_id=f"{cat[:2]}{i}", category=cat)
            rows.append(r)
    rows.append({**rows[0], "item_id": "NEW", "category": mm.EXCLUDED_CATEGORY})
    return rows


@pytest.fixture(scope="module")
def data(spark):
    schema = "item_id string, category string, " + ", ".join(f"{c} double" for c in mm.FEATURES)
    cols = ["item_id", "category", *mm.FEATURES]
    df = mm.prepare(spark.createDataFrame([tuple(r[c] for c in cols) for r in synthetic_rows()],
                                          schema))
    indexer = mm.label_indexer(df)
    return indexer.transform(df), list(indexer.labels)


@pytest.fixture(scope="module")
def trained(data):
    df, labels = data
    train, test = mm.stratified_split(df, fraction=0.3)
    train = mm.add_class_weights(mm.add_folds(train))
    results = mm.train_and_evaluate(train, test, labels, rf_trees=(5,), rf_depth=(3,))
    return results, test, labels


def test_prepare_drops_insufficient_history_and_fills_zero(data):
    df, labels = data
    assert labels == CATS
    assert df.filter(df.category == mm.EXCLUDED_CATEGORY).count() == 0
    for c in mm.ZERO_FILL:
        assert df.filter(df[c].isNull()).count() == 0


def test_folds_hold_every_class(data):
    df, _ = data
    folds = mm.add_folds(df)
    per_fold = {r.fold: r.n for r in
                folds.groupBy("fold").agg({"category": "approx_count_distinct"})
                .withColumnRenamed("approx_count_distinct(category)", "n").collect()}
    assert sorted(per_fold) == list(range(mm.N_FOLDS))
    assert all(n == len(CATS) for n in per_fold.values())


def test_pipeline_runs_end_to_end(trained):
    results, test, labels = trained
    assert [r["name"] for r in results] == [
        "LogisticRegression", "DecisionTreeClassifier", "RandomForestClassifier"]
    for r in results:
        assert 0.0 <= r["test"]["macro_f1"] <= 1.0
        assert r["test_predictions"].count() == test.count()
        assert {x.predicted_category for x in
                r["test_predictions"].select("predicted_category").collect()} <= set(labels)
    final = mm.select_final(results)
    assert final["test"]["macro_f1"] == max(r["test"]["macro_f1"] for r in results)


def assembled_inputs(pipeline_model):
    from pyspark.ml.feature import VectorAssembler
    (assembler,) = [s for s in pipeline_model.stages if isinstance(s, VectorAssembler)]
    return [c.removesuffix("_imp") for c in assembler.getInputCols()]


def test_no_leakage_columns_in_feature_vector(trained):
    assert not set(mm.FEATURES) & set(mm.LEAKAGE_COLUMNS)
    assert not any(c.endswith(("_percentile", "_tier")) for c in mm.FEATURES)
    results, _, _ = trained
    for r in results:
        inputs = assembled_inputs(r["model"])
        assert inputs == mm.FEATURES
        assert not set(inputs) & set(mm.LEAKAGE_COLUMNS)


def test_macro_f1_matches_hand_computed(spark):
    # Confusion matrix (rows actual, cols predicted), 3 classes:
    #   [[3, 1, 0],
    #    [0, 2, 2],
    #    [1, 0, 1]]
    # class 0: P 3/4, R 3/4, F1 0.75
    # class 1: P 2/3, R 2/4, F1 2*(2/3*1/2)/(2/3+1/2) = 4/7
    # class 2: P 1/3, R 1/2, F1 2*(1/3*1/2)/(1/3+1/2) = 2/5
    conf = [[3, 1, 0], [0, 2, 2], [1, 0, 1]]
    pairs = [(float(p), float(a)) for a, row in enumerate(conf) for p, n in enumerate(row)
             for _ in range(n)]
    out = mm.per_class_metrics(spark.sparkContext.parallelize(pairs), 3)
    expected = (0.75 + 4 / 7 + 2 / 5) / 3
    assert out["macro_f1"] == pytest.approx(expected)
    assert out["per_class"][1]["f1"] == pytest.approx(4 / 7)
    assert out["confusion"] == [[float(x) for x in row] for row in conf]
    # weighted F1 differs, which is why Spark's "f1" evaluator isn't used for selection
    weighted = (4 * 0.75 + 4 * 4 / 7 + 2 * 2 / 5) / 10
    assert out["weighted_f1"] == pytest.approx(weighted)
    assert out["macro_f1"] != pytest.approx(out["weighted_f1"])


def test_class_never_predicted_scores_zero(spark):
    pairs = [(0.0, 0.0), (0.0, 0.0), (1.0, 1.0), (0.0, 2.0)]
    out = mm.per_class_metrics(spark.sparkContext.parallelize(pairs), 3)
    assert out["per_class"][2]["f1"] == 0.0
    assert out["macro_f1"] == pytest.approx((0.8 + 1.0 + 0.0) / 3)


def test_saved_model_reloads_with_same_predictions(trained, tmp_path):
    results, test, _ = trained
    final = mm.select_final(results)
    out = mm.save_model(final["model"], "vtest", {"algorithm": final["name"]}, base=tmp_path)
    assert (out / "model_version.txt").exists()
    assert (tmp_path / "LATEST").read_text().strip() == "vtest"
    reloaded = mm.load_model("vtest", base=tmp_path)

    sample = test.orderBy("item_id").select("item_id", *mm.FEATURES)
    cols = ["item_id", "prediction", "predicted_category", "probability"]
    before = final["model"].transform(sample).select(*cols).orderBy("item_id").collect()
    after = reloaded.transform(sample).select(*cols).orderBy("item_id").collect()
    assert [r.prediction for r in before] == [r.prediction for r in after]
    assert [r.predicted_category for r in before] == [r.predicted_category for r in after]
    for b, a in zip(before, after):
        assert list(b.probability) == pytest.approx(list(a.probability))

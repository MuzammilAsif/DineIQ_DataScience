"""Scikit-learn menu performance classification, mirroring spark_jobs/06_menu_classification_model.py.

Features are recomputed in pandas (python_pipeline/features.py). The target is Step 4's rule-based
category. To compare predictions record by record, the held-out items are the same 28 the Spark
model was tested on; only their item_ids are taken from the Spark run, never any feature.

Output: models/python/menu_classification/<version>/ (final model, version, metrics, and
        test predictions for each of the three algorithms)
Run: .venv/bin/python -m python_pipeline.menu_classification_model
"""
import datetime as dt
import json

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (accuracy_score, confusion_matrix, f1_score,
                             precision_recall_fscore_support)
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier

from python_pipeline import features as pf

MODEL_DIR = pf.ROOT / "models" / "python" / "menu_classification"
LABELS_DIR = pf.ROOT / "parquet_data" / "menu_classification"
SPARK_TEST_IDS = pf.ROOT / "reports" / "sample_predictions_menu_classification.csv"

FEATURES = [
    "item_revenue", "item_cost", "contribution_margin", "profit_percentage",
    "total_quantity_sold", "item_popularity", "order_frequency", "repeat_purchase_rate",
    "average_rating", "rating_trend", "wastage_percentage", "promotion_dependency",
    "discount_percentage", "price_change_percentage", "weekend_order_ratio", "sales_trend",
]
ZERO_FILL = ["price_change_percentage", "rating_trend", "sales_trend"]
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
REG_PARAMS = [0.0, 0.01, 0.1]


def load_labels(as_of):
    """Only item_id and the Step 4 category (the target) are read."""
    return pd.read_parquet(LABELS_DIR / f"as_of_date={as_of}", columns=["item_id", "category"])


def prepare(item_feats, labels):
    df = item_feats.merge(labels, on="item_id")
    df = df[df.category != EXCLUDED_CATEGORY].copy()
    df[ZERO_FILL] = df[ZERO_FILL].fillna(0.0)
    return df[["item_id", "category", *FEATURES]].reset_index(drop=True)


def candidates(n_train):
    # Spark's regParam minimises mean loss + regParam * ||w||^2 / 2; sklearn's C multiplies the
    # summed loss, so the same penalty is C = 1 / (regParam * n_train). regParam 0 = C inf.
    lr_grid = {"clf__C": [1 / (r * n_train) if r else np.inf for r in REG_PARAMS]}
    imp = ("impute", SimpleImputer(strategy="median"))
    return [
        ("LogisticRegression",
         Pipeline([imp, ("scale", StandardScaler()),
                   ("clf", LogisticRegression(class_weight="balanced", max_iter=5000))]),
         lr_grid, {"regParam": REG_PARAMS}),
        ("DecisionTreeClassifier",
         Pipeline([imp, ("clf", DecisionTreeClassifier(class_weight="balanced", random_state=SEED))]),
         {"clf__max_depth": [3, 5, 8]}, {"maxDepth": [3, 5, 8]}),
        ("RandomForestClassifier",
         Pipeline([imp, ("clf", RandomForestClassifier(class_weight="balanced", random_state=SEED))]),
         {"clf__n_estimators": [30, 80], "clf__max_depth": [5, 10]},
         {"numTrees": [30, 80], "maxDepth": [5, 10]}),
    ]


def evaluate(y_true, y_pred, labels):
    p, r, f, n = precision_recall_fscore_support(y_true, y_pred, labels=labels, zero_division=0)
    return dict(accuracy=accuracy_score(y_true, y_pred),
                macro_f1=f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0),
                weighted_f1=f1_score(y_true, y_pred, labels=labels, average="weighted", zero_division=0),
                per_class={c: dict(precision=p[i], recall=r[i], f1=f[i], n=int(n[i]))
                           for i, c in enumerate(labels)},
                confusion=confusion_matrix(y_true, y_pred, labels=labels).tolist())


def describe_params(name, params, n_train):
    if name == "LogisticRegression":
        return f"regParam={1 / (params['clf__C'] * n_train):.2g}"
    keys = {"clf__max_depth": "maxDepth", "clf__n_estimators": "numTrees"}
    return ", ".join(f"{keys[k]}={v}" for k, v in sorted(params.items()))


def train_and_evaluate(train, test, labels, folds=5):
    X, y = train[FEATURES], train.category
    cv = StratifiedKFold(n_splits=folds, shuffle=True, random_state=SEED)
    results = []
    for name, pipe, grid, grid_desc in candidates(len(train)):
        search = GridSearchCV(pipe, grid, scoring="f1_macro", cv=cv).fit(X, y)
        best = search.best_estimator_
        proba = best.predict_proba(test[FEATURES])
        pred = best.classes_[proba.argmax(axis=1)]
        results.append(dict(
            name=name, model=best, grid=grid_desc,
            best_params=describe_params(name, search.best_params_, len(train)),
            cv_macro_f1=float(search.best_score_),
            train=evaluate(y, best.predict(X), labels),
            test=evaluate(test.category, pred, labels),
            predictions=pd.DataFrame({"item_id": test.item_id.values,
                                      "actual_category": test.category.values,
                                      "predicted_category": pred,
                                      "predicted_probability": proba.max(axis=1).round(4)})))
    return results


def select_final(results):
    return max(results, key=lambda r: (round(r["test"]["macro_f1"], 6), r["cv_macro_f1"]))


def main():
    tables = pf.load_tables(["Orders", "Order_Items", "Menu_Items", "Menu_Categories", "Ratings",
                             "Wastage", "Pricing_History"])
    as_of = pf.default_as_of(tables)
    df = prepare(pf.item_features(tables, as_of), load_labels(as_of.date()))
    labels = sorted(df.category.unique())
    test_ids = set(pd.read_csv(SPARK_TEST_IDS, usecols=["item_id"]).item_id)
    test, train = df[df.item_id.isin(test_ids)], df[~df.item_id.isin(test_ids)]
    assert len(test) == len(test_ids)

    results = train_and_evaluate(train, test, labels)
    final = select_final(results)
    version = "v" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = MODEL_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    joblib.dump(final["model"], path / "model.joblib")
    summary = [{k: r[k] for k in ["name", "grid", "best_params", "cv_macro_f1", "train", "test"]}
               for r in results]
    (path / "metrics.json").write_text(json.dumps(
        dict(version=version, final=final["name"], labels=labels, n_train=len(train),
             n_test=len(test), results=summary), indent=2, default=float) + "\n")
    (path / "model_version.txt").write_text(json.dumps(
        dict(version=version, algorithm=final["name"], params=final["best_params"],
             as_of_date=str(as_of.date()), features=FEATURES,
             test_macro_f1=final["test"]["macro_f1"], test_accuracy=final["test"]["accuracy"]),
        indent=2) + "\n")
    for r in results:
        r["predictions"].assign(algorithm=r["name"], model_version=version).to_csv(
            path / f"test_predictions_{r['name']}.csv", index=False)
    (MODEL_DIR / "LATEST").write_text(version + "\n")
    for r in results:
        print(f"{r['name']:24s} {r['best_params']:24s} cv={r['cv_macro_f1']:.3f} "
              f"test_acc={r['test']['accuracy']:.3f} test_macro_f1={r['test']['macro_f1']:.3f}")
    print("final:", final["name"], version, np.round(final["test"]["macro_f1"], 3))


if __name__ == "__main__":
    main()

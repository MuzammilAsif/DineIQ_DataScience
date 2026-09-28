"""Scikit-learn customer segmentation, mirroring spark_jobs/07_customer_segmentation.py.

Same inputs (recency, log1p frequency, log1p monetary, average order value, promo order
share), same k, same crc32 holdout split. Clusters are mapped to the rule-based Actual
segment by majority vote over training customers.

Output: models/python/customer_segmentation/<version>/, models/python/customer_segments.parquet
Run: .venv/bin/python -m python_pipeline.customer_segmentation_model
"""
import datetime as dt
import json

import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from python_pipeline import features as pf
from python_pipeline import segment_rules as sr

MODEL_DIR = pf.ROOT / "models" / "python" / "customer_segmentation"
SEGMENTS_PATH = pf.ROOT / "models" / "python" / "customer_segments.parquet"
CLUSTER_INPUTS = ["customer_recency", "log_frequency", "log_monetary", "average_order_value",
                  "promo_order_share"]


def cluster_inputs(df):
    return df.assign(log_frequency=np.log1p(df.customer_frequency),
                     log_monetary=np.log1p(df.customer_monetary_value))[CLUSTER_INPUTS]


def fit_and_assign(df, k, seed, holdout):
    """Fits on non-holdout rows; returns (model, cluster ids, distance, margin) for all rows."""
    X = cluster_inputs(df)
    model = Pipeline([("scale", StandardScaler()),
                      ("kmeans", KMeans(n_clusters=k, n_init=10, random_state=seed))])
    model.fit(X[~holdout])
    dists = model.transform(X)
    order = np.sort(dists, axis=1)
    return model, dists.argmin(axis=1), order[:, 0], order[:, 1] - order[:, 0]


def run(tables, as_of, cfg):
    feats = pf.customer_features(tables, as_of)
    actual, tertiles = sr.rule_segments(feats, cfg["segment_rules"])
    holdout = sr.holdout_mask(feats.customer_id, cfg["segmentation"]["holdout_mod"]).values
    model, cluster, dist, margin = fit_and_assign(
        feats, cfg["segmentation"]["k"], cfg["segmentation"]["seed"], holdout)
    mapping = sr.majority_mapping(pd.Series(cluster[~holdout]), actual[~holdout].values)
    out = feats.assign(
        split=np.where(holdout, "holdout", "train"), actual_segment=actual.values,
        cluster_id=cluster, python_segment=pd.Series(cluster).map(mapping).values,
        distance_to_centroid=dist, centroid_margin=margin, **tertiles)
    return out, model, mapping


def main():
    cfg = sr.load_config()
    tables = pf.load_tables(["Orders", "Customers"])
    as_of = pf.default_as_of(tables)
    out, model, mapping = run(tables, as_of, cfg)

    version = "v" + dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = MODEL_DIR / version
    path.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, path / "model.joblib")
    hold = out[out.split == "holdout"]
    meta = dict(version=version, algorithm="sklearn KMeans", k=cfg["segmentation"]["k"],
                inputs=CLUSTER_INPUTS, as_of_date=str(as_of.date()),
                cluster_to_segment={int(k): v for k, v in mapping.items()},
                holdout_customers=len(hold),
                holdout_agreement_with_actual=float((hold.python_segment == hold.actual_segment).mean()))
    (path / "model_version.txt").write_text(json.dumps(meta, indent=2) + "\n")
    (MODEL_DIR / "LATEST").write_text(version + "\n")
    out.assign(model_version=version).to_parquet(SEGMENTS_PATH, index=False)
    print(json.dumps(meta, indent=2))
    print(out.actual_segment.value_counts())


if __name__ == "__main__":
    main()

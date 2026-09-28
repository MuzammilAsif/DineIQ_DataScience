"""Builds models/model_registry.json from the version files the training jobs already saved.

Run: .venv/bin/python src/model_registry.py
"""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
MODELS = ROOT / "models"
REGISTRY = MODELS / "model_registry.json"


def _latest(d):
    version = (d / "LATEST").read_text().strip()
    return version, json.loads((d / version / "model_version.txt").read_text())


def _from_version(version):
    return datetime.strptime(version, "v%Y%m%d_%H%M%S").isoformat(timespec="seconds")


def build():
    entries = []
    v, m = _latest(MODELS / "spark" / "menu_classification")
    entries.append(dict(model="Menu classification", pipeline="Spark MLlib", algorithm=m["algorithm"],
                        version=v, trained_at=m.get("trained_at") or _from_version(v),
                        key_metric="test macro F1", value=m["test_macro_f1"],
                        path=f"models/spark/menu_classification/{v}/model"))
    v, m = _latest(MODELS / "python" / "menu_classification")
    entries.append(dict(model="Menu classification", pipeline="Python (scikit-learn)",
                        algorithm=m["algorithm"], version=v, trained_at=_from_version(v),
                        key_metric="test macro F1", value=m["test_macro_f1"],
                        path=f"models/python/menu_classification/{v}/model.joblib"))

    seg_dir = max((MODELS / "spark" / "customer_segmentation").glob("as_of_date=*"))
    comparison = pd.read_csv(ROOT / "reports" / "dual_pipeline_comparison.csv")
    cust = comparison[comparison.record_type == "customer"]
    entries.append(dict(model="Customer segmentation", pipeline="Spark MLlib", algorithm="KMeans",
                        version=seg_dir.name,
                        trained_at=datetime.fromtimestamp(seg_dir.stat().st_mtime).isoformat(timespec="seconds"),
                        key_metric="holdout agreement with rule-based segment",
                        value=float((cust.spark_result == cust.actual).mean()),
                        path=f"models/spark/customer_segmentation/{seg_dir.name}"))
    v, m = _latest(MODELS / "python" / "customer_segmentation")
    entries.append(dict(model="Customer segmentation", pipeline="Python (scikit-learn)",
                        algorithm=m["algorithm"], version=v, trained_at=_from_version(v),
                        key_metric="holdout agreement with rule-based segment",
                        value=m["holdout_agreement_with_actual"],
                        path=f"models/python/customer_segmentation/{v}/model.joblib"))

    v, m = _latest(MODELS / "spark" / "demand_forecast")
    grain = m["metrics"]["item_location_day"]
    entries.append(dict(model="Demand forecast", pipeline="Spark MLlib", algorithm=m["algorithm"],
                        version=v, trained_at=_from_version(v),
                        key_metric="test MAE, item x location x day (baseline "
                                   f"{grain['baseline']['mae']:.3f})",
                        value=grain["model"]["mae"], path=f"models/spark/demand_forecast/{v}/model"))
    v, m = _latest(MODELS / "spark" / "wastage_risk")
    entries.append(dict(model="Wastage risk", pipeline="Spark MLlib", algorithm=m["algorithm"],
                        version=v, trained_at=_from_version(v), key_metric="test PR AUC",
                        value=m["metrics"]["auc_pr"], path=f"models/spark/wastage_risk/{v}/model"))
    return dict(generated_at=datetime.now().isoformat(timespec="seconds"), models=entries)


def write():
    reg = build()
    REGISTRY.write_text(json.dumps(reg, indent=2) + "\n")
    return reg


def read():
    return json.loads(REGISTRY.read_text())


if __name__ == "__main__":
    for e in write()["models"]:
        print(f"{e['model']:22s} {e['pipeline']:22s} {e['version']:22s} {e['key_metric']}: {e['value']:.4f}")

"""Tests for the independent Python pipeline and the dual-pipeline comparison (Step 7)."""
import ast
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from python_pipeline import compare_pipelines as cp  # noqa: E402
from python_pipeline import features as pf  # noqa: E402
from python_pipeline import segment_rules as sr  # noqa: E402

PIPELINE_FILES = ["features.py", "segment_rules.py", "menu_classification_model.py",
                  "customer_segmentation_model.py"]
SPARK_MODULES = {"pyspark", "spark_jobs", "spark_utils", "analytics_common", "schemas"}


def _tree(name):
    return ast.parse((ROOT / "python_pipeline" / name).read_text())


@pytest.mark.parametrize("name", PIPELINE_FILES)
def test_pipeline_imports_nothing_from_spark(name):
    for node in ast.walk(_tree(name)):
        if isinstance(node, ast.Import):
            mods = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            mods = [node.module or ""]
        else:
            continue
        for m in mods:
            assert m.split(".")[0] not in SPARK_MODULES, f"{name} imports {m}"


@pytest.mark.parametrize("name", PIPELINE_FILES)
def test_pipeline_never_reads_spark_features_or_segments(name):
    tree = _tree(name)
    docstrings = {id(n.body[0].value) for n in ast.walk(tree)
                  if isinstance(n, (ast.Module, ast.FunctionDef, ast.ClassDef)) and n.body
                  and isinstance(n.body[0], ast.Expr) and isinstance(n.body[0].value, ast.Constant)}
    strings = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
               and isinstance(n.value, str) and id(n) not in docstrings]
    # parquet_data / "features" and parquet_data / "customer_segments" are Spark outputs
    assert "features" not in strings and "customer_segments" not in strings
    assert not any("parquet_data/features" in s or "spark_jobs" in s for s in strings)


def test_features_computed_from_raw_tables():
    ts = pd.Timestamp
    orders = pd.DataFrame({
        "order_id": ["O1", "O2", "O3", "O4"],
        "customer_id": ["C1", "C1", "C2", "C2"],
        # O1 Friday, O2 Monday, O3 Saturday, O4 cancelled
        "order_datetime": [ts("2025-01-03 12:00"), ts("2025-01-06 13:00"),
                           ts("2025-01-04 20:00"), ts("2025-01-05 12:00")],
        "order_status": ["Completed", "Completed", "Completed", "Cancelled"],
        "promotion_id": ["P1", None, None, None],
        "discount_amount": [0.0, 0.0, 0.0, 0.0],
        "total_amount": [200.0, 100.0, 300.0, 999.0]})
    items = pd.DataFrame({
        "order_item_id": ["L1", "L2", "L3", "L4"], "order_id": ["O1", "O2", "O3", "O4"],
        "item_id": ["I1", "I1", "I1", "I1"], "quantity": [2, 1, 3, 5],
        "unit_price": [100.0] * 4, "unit_cost": [40.0] * 4, "discount_amount": [0.0] * 4,
        "line_total": [200.0, 100.0, 300.0, 500.0]})
    empty = lambda cols: pd.DataFrame({c: pd.Series(dtype="object") for c in cols})  # noqa: E731
    ratings = empty(["item_id", "rating_value"]).assign(rating_date=pd.Series(dtype="datetime64[ns]"))
    wastage = empty(["item_id", "cost_of_waste"]).assign(date=pd.Series(dtype="datetime64[ns]"))
    t = {"Orders": orders, "Order_Items": items, "Ratings": ratings, "Wastage": wastage,
         "Pricing_History": pd.DataFrame({
             "item_id": ["I1", "I1"], "location_id": [None, None], "price": [100.0, 110.0],
             "effective_start_date": [ts("2024-01-01"), ts("2025-01-02")]}),
         "Menu_Items": pd.DataFrame({"item_id": ["I1"], "item_name": ["x"], "category_id": ["K"],
                                     "base_cost": [40.0], "launch_date": [ts("2024-01-01")]}),
         "Menu_Categories": pd.DataFrame({"category_id": ["K"], "category_name": ["Mains"]}),
         "Customers": pd.DataFrame({"customer_id": ["C1", "C2"],
                                    "signup_date": [ts("2024-12-01"), ts("2025-01-01")]})}
    it = pf.item_features(t, ts("2025-01-10")).iloc[0]
    assert it.item_revenue == 600 and it.total_quantity_sold == 6  # cancelled O4 excluded
    assert it.contribution_margin == 600 - 6 * 40
    assert it.repeat_purchase_rate == pytest.approx(0.5)  # C1 ordered twice, C2 once
    assert it.weekend_order_ratio == pytest.approx(5 / 6)  # Fri 2 + Sat 3
    assert it.promotion_dependency == pytest.approx(200 / 600)
    assert it.price_change_percentage == pytest.approx(10.0)
    cu = pf.customer_features(t, ts("2025-01-10")).set_index("customer_id")
    assert cu.loc["C1"].customer_recency == 4 and cu.loc["C1"].customer_frequency == 2
    assert cu.loc["C2"].customer_monetary_value == 300.0
    assert cu.loc["C1"].promo_order_share == pytest.approx(0.5)
    assert cu.loc["C2"].signup_days == 9


def test_agreement_percentage_hand_example():
    actual = ["A", "A", "B", "B", "C"]
    spark = ["A", "B", "B", "B", "A"]
    python = ["A", "B", "B", "C", "C"]
    rows = cp.build_rows("x", range(5), actual, spark, python, [0] * 5, [0] * 5, "n/a")
    s = cp.summary(rows)
    assert s["spark_vs_actual"] == pytest.approx(60.0)
    assert s["python_vs_actual"] == pytest.approx(60.0)
    assert s["spark_vs_python"] == pytest.approx(60.0)
    assert list(rows.spark_python_match) == ["Match", "Match", "Match", "Mismatch", "Mismatch"]
    assert list(rows.final_consistency_status) == [
        "Consistent: both correct", "Consistent: both wrong", "Consistent: both correct",
        "Inconsistent: Spark correct", "Inconsistent: Python correct"]
    assert (rows.numerical_difference == "N/A").all()


def test_rule_segments():
    cfg = {"new_signup_days": 90, "new_max_orders": 2, "promo_share_min": 0.5}
    n = 30
    df = pd.DataFrame({
        "customer_recency": [10 + i * 10 for i in range(n)],
        "customer_frequency": [30 - i for i in range(n)],
        "customer_monetary_value": [1000.0 * (n - i) for i in range(n)],
        "promo_order_share": [0.0] * n, "signup_days": [400] * n})
    df.loc[29, ["customer_frequency", "signup_days"]] = [1, 30]       # new
    df.loc[25, "promo_order_share"] = 0.9                               # promo, low RFM
    df.loc[26, ["customer_recency", "customer_frequency"]] = [500, 25]  # lapsed but frequent
    seg, t = sr.rule_segments(df, cfg)
    assert seg[0] == "High-Value Loyal"
    assert seg[29] == "New" and seg[25] == "Promotion-Driven" and seg[26] == "At-Risk"
    assert seg[15] == "Occasional"


def test_holdout_split_matches_spark_crc32(spark):
    import pyspark.sql.functions as F
    ids = [f"CUST{i:07d}" for i in range(1, 400)]
    sdf = spark.createDataFrame([(i,) for i in ids], "customer_id string")
    spark_hold = {r.customer_id for r in sdf.filter(F.crc32("customer_id") % 5 == 0).collect()}
    py_hold = set(pd.Series(ids)[sr.holdout_mask(pd.Series(ids), 5)])
    assert spark_hold == py_hold and 40 < len(py_hold) < 120

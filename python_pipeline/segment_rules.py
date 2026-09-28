"""Rule-based customer segment used as the shared "Actual" label for the dual-pipeline comparison.

Thresholds come from config/analytics_thresholds.yaml (segment_rules). R/F/M tertiles are
percentile ranks over all customers, 3 = best (most recent, most frequent, highest spend).
Rules are applied in this order; the first match wins:

1. High-Value Loyal: R, F and M all in the top tertile.
2. New: signed up within new_signup_days and at most new_max_orders orders.
3. At-Risk: bottom recency tertile (longest gap) and F tertile 2 or 3.
4. Promotion-Driven: promo order share at least promo_share_min.
5. Frequent: top frequency tertile.
6. Occasional: everything else.
"""
import zlib
from pathlib import Path

import pandas as pd
import yaml

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "analytics_thresholds.yaml"
SEGMENTS = ["High-Value Loyal", "New", "At-Risk", "Promotion-Driven", "Frequent", "Occasional"]


def load_config(path=CONFIG_PATH):
    with open(path) as fh:
        return yaml.safe_load(fh)


def tertile(values, higher_is_better=True):
    pct = (values if higher_is_better else -values).rank(pct=True, method="average")
    return pd.cut(pct, [0, 1 / 3, 2 / 3, 1], labels=[1, 2, 3], include_lowest=True).astype(int)


def rule_segments(df, cfg):
    """df: customer_recency, customer_frequency, customer_monetary_value, promo_order_share,
    signup_days. Returns (segment Series, DataFrame of r/f/m tertiles)."""
    t = pd.DataFrame({
        "r_tertile": tertile(df.customer_recency, higher_is_better=False),
        "f_tertile": tertile(df.customer_frequency),
        "m_tertile": tertile(df.customer_monetary_value),
    }, index=df.index)
    new = (df.signup_days <= cfg["new_signup_days"]) & (df.customer_frequency <= cfg["new_max_orders"])
    conditions = [
        (t.r_tertile == 3) & (t.f_tertile == 3) & (t.m_tertile == 3),
        new,
        (t.r_tertile == 1) & (t.f_tertile >= 2),
        df.promo_order_share >= cfg["promo_share_min"],
        t.f_tertile == 3,
    ]
    seg = pd.Series("Occasional", index=df.index)
    assigned = pd.Series(False, index=df.index)
    for label, cond in zip(SEGMENTS, conditions):
        hit = cond.fillna(False) & ~assigned
        seg[hit] = label
        assigned |= hit
    return seg, t


def holdout_mask(customer_ids, mod):
    """Same split as Spark's F.crc32(customer_id) % mod == 0 (CRC-32 of the UTF-8 bytes)."""
    return customer_ids.map(lambda c: zlib.crc32(str(c).encode()) % mod == 0)


def majority_mapping(clusters, actual):
    """Maps each cluster id to the most common actual label among its members."""
    counts = pd.crosstab(clusters, actual)
    return {cid: counts.loc[cid].idxmax() for cid in counts.index}

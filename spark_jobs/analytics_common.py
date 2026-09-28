"""Shared helpers for the Step 6 analytics jobs (07-14)."""
import re

import pyspark.sql.functions as F
import yaml

from spark_utils import PARQUET_DIR, REPORTS_DIR, ROOT

CONFIG_PATH = ROOT / "config" / "analytics_thresholds.yaml"
REPORT_PATH = REPORTS_DIR / "restaurant_intelligence_report.md"
FEATURES_DIR = PARQUET_DIR / "features"
CANCELLED = "Cancelled"

REPORT_HEADER = (
    "# Restaurant Intelligence Report\n\n"
    "One section per analytics module (`spark_jobs/07`-`14`). Each job rewrites only its own "
    "section. Thresholds come from `config/analytics_thresholds.yaml`.\n"
)
LIMITATIONS = """## Limitations

- Nothing from the Step 6 scope was cut. Every module is built and run.
- Segmentation labels come from ranking cluster means, not from ground truth. Fixed cutoffs \
(e.g. recency > 90 days) were tried first and labelled four of six clusters At-Risk, because \
most customers order only once or twice.
- Market-basket associations are weak: the highest lift is below 1.5, and 0.01 / 0.3 \
thresholds gave no rules at all, so they were lowered to 0.005 / 0.1. The strongest pairs \
(pakoras together, summer drinks together) are probably seasonal items bought in the same \
months rather than true product affinity.
- The wastage-risk label uses a fixed threshold (wasted / prepared > 10%) rather than a top \
quartile, because 95% of item-days have no waste.
- Price elasticities are before/after comparisons with a same-category control. They don't \
control for item-specific seasonality or promotions. Changes under 5% are not evaluated.
- Promotion windows can overlap other promotions and aren't seasonally adjusted. The \
wastage flag fires for most promotions because kitchens over-prepare for all of them.
- Anomaly detection is purely statistical. Holiday and promotion-launch spikes are flagged \
along with anything unusual, and there is no labelled set to measure precision against.
- Churn risk is a rule, not a model, and can't flag customers who had already stopped \
ordering before the previous quarter.
"""
_SECTION = re.compile(r"<!-- section:(\S+) -->\n.*?<!-- /section:\1 -->\n", re.S)


def load_config(path=CONFIG_PATH):
    with open(path) as fh:
        return yaml.safe_load(fh)


def latest_snapshot(table):
    return max(p.name.split("=", 1)[1] for p in (FEATURES_DIR / table).glob("as_of_date=*"))


def completed_fact(spark):
    return spark.read.parquet(str(PARQUET_DIR / "fact_order_line")).filter(
        F.col("order_status") != CANCELLED)


def completed_orders(spark):
    return spark.read.parquet(str(PARQUET_DIR / "orders")).filter(
        F.col("order_status") != CANCELLED)


def md_table(pdf, formats=None):
    formats = formats or {}
    cols = list(pdf.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for _, row in pdf.iterrows():
        cells = []
        for c in cols:
            v = row[c]
            fmt = formats.get(c)
            cells.append("" if v is None or (isinstance(v, float) and v != v)
                         else fmt.format(v) if fmt else str(v))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_section(key, markdown, path=REPORT_PATH):
    """Replaces this module's section in the shared report; sections stay sorted by key."""
    text = path.read_text() if path.exists() else REPORT_HEADER
    sections = {m.group(1): m.group(0) for m in _SECTION.finditer(text)}
    for k, md in ((key, markdown), ("99", LIMITATIONS)):
        sections[k] = f"<!-- section:{k} -->\n{md.strip()}\n<!-- /section:{k} -->\n"
    path.write_text(REPORT_HEADER + "\n" + "\n".join(sections[k] for k in sorted(sections)))


def out_path(name):
    return str(PARQUET_DIR / name)



def promo_coverage(promotions, promo_items, promo_locations):
    """One row per (promotion_id, item_id, location_id, date) the promotion covers."""
    days = promotions.select(
        "promotion_id",
        F.explode(F.sequence("start_date", "end_date")).alias("date"))
    return days.join(promo_items, "promotion_id").join(promo_locations, "promotion_id")

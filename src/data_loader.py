"""Cached readers for the Parquet outputs and reports. Each is read from disk once per session."""
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
PQ = ROOT / "parquet_data"
REPORTS = ROOT / "reports"
MODELS = ROOT / "models"
CANCELLED = "Cancelled"


def _read(path, columns=None):
    """Parquet (partitioned or not) to pandas, with decimals as float."""
    t = ds.dataset(path, format="parquet", partitioning="hive").to_table(columns=columns)
    cast = pa.schema([pa.field(f.name, pa.float64()) if pa.types.is_decimal(f.type) else f
                      for f in t.schema])
    return t.cast(cast).to_pandas()


def _snapshot(name):
    return max((PQ / name).glob("as_of_date=*")).name.split("=", 1)[1]


def as_of():
    return _snapshot("menu_classification")


@st.cache_data(show_spinner=False)
def locations():
    d = _read(PQ / "features" / "location_features" / f"as_of_date={_snapshot('features/location_features')}")
    return d


@st.cache_data(show_spinner=False)
def orders():
    """One row per order with its line margin; cancelled orders kept with their status."""
    o = _read(PQ / "orders", ["order_id", "customer_id", "location_id", "order_datetime",
                              "order_channel", "order_status", "promotion_id", "subtotal",
                              "discount_amount", "total_amount"])
    f = _read(PQ / "fact_order_line", ["order_id", "quantity", "unit_cost", "line_total"])
    f["margin"] = f.line_total - f.quantity * f.unit_cost
    m = f.groupby("order_id", as_index=False).agg(margin=("margin", "sum"),
                                                  items=("quantity", "sum"))
    o = o.merge(m, on="order_id", how="left")
    o["date"] = o.order_datetime.dt.normalize()
    return o


@st.cache_data(show_spinner=False)
def daily_item_sales():
    f = _read(PQ / "fact_order_line", ["item_id", "location_id", "category_name", "quantity",
                                       "line_total", "order_datetime", "order_status"])
    f = f[(f.order_status != CANCELLED) & f.item_id.notna()]
    f["date"] = f.order_datetime.dt.normalize()
    return (f.groupby(["date", "item_id", "location_id", "category_name"], as_index=False)
            .agg(quantity=("quantity", "sum"), revenue=("line_total", "sum")))


@st.cache_data(show_spinner=False)
def classification():
    return _read(PQ / "menu_classification" / f"as_of_date={as_of()}")


@st.cache_data(show_spinner=False)
def classification_by_location():
    return _read(PQ / "menu_classification_by_location" / f"as_of_date={as_of()}")


@st.cache_data(show_spinner=False)
def item_features():
    return _read(PQ / "features" / "item_features" / f"as_of_date={as_of()}")


@st.cache_data(show_spinner=False)
def slow_moving():
    return _read(PQ / "slow_moving")


@st.cache_data(show_spinner=False)
def customer_segments():
    seg = _read(PQ / "customer_segments")
    churn = _read(PQ / "churn_risk", ["customer_id", "churn_risk", "orders_last_quarter",
                                      "orders_prev_quarter"])
    cust = pd.read_csv(ROOT / "processed_data" / "Customers.csv",
                       usecols=["customer_id", "home_location_id", "loyalty_member"])
    return seg.merge(churn, on="customer_id", how="left").merge(cust, on="customer_id", how="left")


@st.cache_data(show_spinner=False)
def wastage():
    w = _read(PQ / "wastage", ["item_id", "location_id", "date", "quantity_wasted", "unit",
                               "cost_of_waste", "reason"])
    w["date"] = pd.to_datetime(w.date)
    items = classification()[["item_id", "item_name", "category_name", "category"]]
    return w.merge(items, on="item_id", how="left")


@st.cache_data(show_spinner=False)
def wastage_risk():
    r = _read(PQ / "wastage_risk")
    r["date"] = pd.to_datetime(r.date)
    return r


@st.cache_data(show_spinner=False)
def forecast(level):
    d = _read(PQ / "demand_forecast" / level)
    d["target_date"] = pd.to_datetime(d.target_date)
    return d


@st.cache_data(show_spinner=False)
def forecast_meta():
    d = MODELS / "spark" / "demand_forecast"
    return json.loads((d / (d / "LATEST").read_text().strip() / "model_version.txt").read_text())


@st.cache_data(show_spinner=False)
def dual_comparison():
    return pd.read_csv(REPORTS / "dual_pipeline_comparison.csv")


@st.cache_data(show_spinner=False)
def recommendations():
    return _read(PQ / "recommendations")


@st.cache_data(show_spinner=False)
def anomalies():
    a = _read(PQ / "anomalies")
    a["period_start"] = pd.to_datetime(a.period_start)
    return a


def report_files():
    return sorted(REPORTS.glob("*.md"))


_LINE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] (.*)$")


def job_runs():
    """One row per reports/spark_execution_log_*.txt: last run time, duration, status."""
    rows = []
    for p in sorted(REPORTS.glob("spark_execution_log_*.txt")):
        stamps, started, ended = [], [], []
        for line in p.read_text().splitlines():
            m = _LINE.match(line)
            if not m:
                continue
            stamps.append(datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S"))
            msg = m.group(2)
            if msg.startswith("START"):
                started.append(msg[5:].strip())
            elif msg.startswith("END"):
                ended.append(re.sub(r" \(duration [\d.]+s\)$", "", msg[3:].strip()))
        open_stages = [s for s in started if s not in ended]
        rows.append(dict(
            job=p.stem.replace("spark_execution_log_", ""),
            started=stamps[0] if stamps else None,
            finished=stamps[-1] if stamps else None,
            duration_s=(stamps[-1] - stamps[0]).total_seconds() if stamps else None,
            stages=len(started),
            status="Completed" if stamps and not open_stages else
                   f"Incomplete: {open_stages[0]}" if open_stages else "No entries"))
    return pd.DataFrame(rows)

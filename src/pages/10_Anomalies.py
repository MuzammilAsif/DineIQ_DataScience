import plotly.express as px
import streamlit as st

import data_loader as dl
import ui

LEVELS = ["Critical", "High", "Medium", "Review"]

user, f = ui.page("Anomalies")
st.caption("Flags from spark_jobs/12_anomaly_detection.py: sales and rating z-scores against a "
           "rolling baseline, orders above the IQR outlier cutoff, and weeks where one rating value "
           "dominates. " + ui.SEVERITY_RULE)

with ui.guard("Could not load anomalies."):
    a = ui.filter_df(dl.anomalies(), f, date_col="period_start")
if f["locations"]:
    a = a[a.location_id.isin(f["locations"]) | a.entity_id.isin(f["locations"])]
a = a.assign(severity=[ui.severity(t, s) for t, s in zip(a.anomaly_type, a.score)])
ui.applied(f, ["date", "location"])
if f["locations"]:
    st.caption("With a location selected, only location-level flags remain (item and rating flags "
               "are chain-wide).")

c1, c2 = st.columns([1, 2])
sev = c1.multiselect("Severity", LEVELS, default=["Critical", "High"])
kinds = c2.multiselect("Type", sorted(a.anomaly_type.unique()),
                       format_func=lambda t: ui.ANOMALY_LABELS.get(t, t))
view = a[a.severity.isin(sev or LEVELS)]
if kinds:
    view = view[view.anomaly_type.isin(kinds)]

counts = a.severity.value_counts()
ui.kpis([("Critical", f"{counts.get('Critical', 0):,}", "|z| 6 or more", "accent"),
         ("High", f"{counts.get('High', 0):,}", "|z| 4.5 to 6"),
         ("Medium", f"{counts.get('Medium', 0):,}", "|z| 3 to 4.5"),
         ("Review", f"{counts.get('Review', 0):,}", "no z-score")])
ui.source("parquet_data/anomalies")
if view.empty:
    st.warning("No anomalies match the current filters.")
    st.stop()

st.subheader("Flags per week")
weekly = (view.set_index("period_start").groupby("anomaly_type").resample("W").size()
          .rename("flags").reset_index())
weekly["type"] = weekly.anomaly_type.map(ui.ANOMALY_LABELS)
fig = px.line(weekly, x="period_start", y="flags", color="type",
              labels={"period_start": "Week starting", "flags": "Anomalies flagged", "type": ""})
fig.update_traces(hovertemplate="%{x|%d %b %Y}: %{y} flags")
ui.chart(fig, source_path="parquet_data/anomalies")
top = view.anomaly_type.value_counts()
ui.takeaway(f"{len(view):,} anomalies shown. The most common type is "
            f"{ui.ANOMALY_LABELS.get(top.index[0], top.index[0]).lower()} ({top.iloc[0]:,}). "
            f"The busiest week began {weekly.groupby('period_start').flags.sum().idxmax():%d %b}.")

st.subheader("Largest deviations")
order = {k: i for i, k in enumerate(LEVELS)}
view = view.assign(rank=view.severity.map(order), size=view.score.abs()).sort_values(
    ["rank", "size"], ascending=[True, False])
limit = st.number_input("Show", min_value=1, max_value=len(view), value=min(20, len(view)), step=10)
left, right = st.columns(2)
for i, r in enumerate(view.head(int(limit)).itertuples()):
    ui.anomaly_card(r, f["location_names"], left if i % 2 == 0 else right)
ui.download_df(view.drop(columns=["rank", "size"]), "anomalies")

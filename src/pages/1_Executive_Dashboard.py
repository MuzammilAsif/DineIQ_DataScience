import plotly.graph_objects as go
import streamlit as st

import data_loader as dl
import db
import ui

user, f = ui.page("Executive Dashboard")

with ui.guard("Could not load order data."):
    o = ui.filter_df(dl.orders(), f, date_col="date", loc_col="location_id")
done = o[o.order_status != dl.CANCELLED]
if done.empty:
    st.warning("No completed orders match the current filters.")
    st.stop()

revenue = (done.subtotal - done.discount_amount).sum()
profit = done.margin.sum()
n_orders = len(done)
per_cust = done.groupby("customer_id").size()
active, repeat = len(per_cust), int((per_cust >= 2).sum())

with ui.guard("Could not load wastage data."):
    w = ui.filter_df(dl.wastage(), f, date_col="date", loc_col="location_id")
waste = w.cost_of_waste.sum()

ui.applied(f, ["date", "location"])
ui.kpis([("Net sales", ui.money_short(revenue), None, "accent"),
         ("Gross profit", ui.money_short(profit), f"{profit / revenue:.1%} margin"),
         ("Completed orders", f"{n_orders:,}"),
         ("Average order value", ui.money_short(revenue / n_orders))])
st.write("")
ui.kpis([("Active customers", f"{active:,}"),
         ("Repeat customers", f"{repeat:,}", f"{repeat / active:.1%} of active"),
         ("Wastage cost", ui.money_short(waste), f"{waste / revenue:.1%} of net sales"),
         ("Cancellation rate", f"{(o.order_status == dl.CANCELLED).mean():.1%}")])
ui.source("parquet_data/orders")

st.subheader("Sales trend")
daily = (done.assign(net=done.subtotal - done.discount_amount)
         .groupby("date", as_index=False).agg(net_sales=("net", "sum"), orders=("order_id", "count")))
daily["avg7"] = daily.net_sales.rolling(7, min_periods=1).mean()
fig = go.Figure()
fig.add_scatter(x=daily.date, y=daily.net_sales, name="Daily net sales", line=dict(color=ui.LIGHT_GRAY, width=1.5),
                hovertemplate="%{x|%d %b %Y}<br>Daily: PKR %{y:,.0f}<extra></extra>")
fig.add_scatter(x=daily.date, y=daily.avg7, name="7-day average", line=dict(color=ui.RED, width=2),
                hovertemplate="%{x|%d %b %Y}<br>7-day average: PKR %{y:,.0f}<extra></extra>")
fig.update_layout(xaxis_title="Date", yaxis_title="Net sales (PKR)", hovermode="x unified")
ui.chart(fig, source_path="parquet_data/orders")
monthly = daily.groupby(daily.date.dt.to_period("M").dt.to_timestamp()).net_sales.sum()
if len(monthly) >= 2:
    best = monthly.idxmax()
    ui.takeaway(f"Net sales went from {ui.money(monthly.iloc[0])} in {monthly.index[0]:%b} to "
                f"{ui.money(monthly.iloc[-1])} in {monthly.index[-1]:%b} "
                f"({monthly.iloc[-1] / monthly.iloc[0] - 1:+.0%}). The best month was {best:%B} "
                f"({ui.money(monthly.max())}). Gross margin over the period is {profit / revenue:.1%}, "
                f"and {repeat / active:.0%} of active customers ordered more than once.")

if not f["locations"] or len(f["locations"]) > 1:
    by_loc = (done.assign(net=done.subtotal - done.discount_amount)
              .groupby("location_id", as_index=False).net.sum().sort_values("net", ascending=False))
    by_loc["location"] = by_loc.location_id.map(f["location_names"])
    st.subheader("Net sales by location")
    ui.chart(ui.hbar(by_loc, "net", "location", "Net sales (PKR)"), source_path="parquet_data/orders")
    top, low = by_loc.iloc[0], by_loc.iloc[-1]
    ui.takeaway(f"{top.location} leads with {ui.money(top.net)}, {top.net / low.net:.1f}x "
                f"{low.location} ({ui.money(low.net)}).")

st.subheader("Demand forecast")
with ui.guard("Could not load the demand forecast."):
    level = "location_day" if f["locations"] else "overall_day"
    fc = dl.forecast(level)
    if f["locations"]:
        fc = fc[fc.location_id.isin(f["locations"])]
    fc = fc.groupby("target_date", as_index=False)[["forecast", "actual", "recent_average"]].sum()
days = len(fc)
ui.kpis([("Forecast units, latest window", f"{fc.forecast.sum():,.0f}",
          f"{fc.target_date.min():%d %b} to {fc.target_date.max():%d %b}", "accent"),
         ("Recent level over the same days", f"{fc.recent_average.sum():,.0f}"),
         ("Actual units sold", f"{fc.actual.sum():,.0f}")])
ui.source(f"parquet_data/demand_forecast/{level}")
ui.takeaway(f"The model forecasts {fc.forecast.sum():,.0f} units over the {days}-day window, "
            f"{fc.forecast.sum() / fc.recent_average.sum() - 1:+.0%} against the recent daily level, "
            f"and actual sales came to {fc.actual.sum():,.0f} "
            f"({fc.forecast.sum() / fc.actual.sum() - 1:+.1%} forecast error). This window is the "
            "held-out test period of the forecast model (see Demand Forecast).")

left, right = st.columns(2)
with left:
    st.subheader("Critical recommendations")
    with ui.guard("Could not load recommendations."):
        recs = dl.recommendations()
    crit = recs[recs.priority == "Critical"]
    ui.takeaway(f"{len(crit)} of {len(recs)} recommendations are Critical: "
                + ", ".join(f"{v} {k.lower()}" for k, v in crit.type.value_counts().items()) + ".")
    for r in crit.head(6).itertuples():
        ui.card(r.priority, r.action, f"{r.type} · {r.target_name}", r.evidence[:2],
                r.recommendation_id)
    ui.source("parquet_data/recommendations", recs.as_of_date.iloc[0])
with right:
    st.subheader("Recent anomalies")
    with ui.guard("Could not load anomalies."):
        a = dl.anomalies()
    a = a[a.anomaly_type.isin(["location_sales_z", "high_order_value", "rating_z"])]
    a = ui.filter_df(a, f, date_col="period_start")
    if f["locations"]:
        a = a[a.location_id.isin(f["locations"])]
    a = a.sort_values("period_start", ascending=False)
    if len(a):
        ui.takeaway(f"{len(a):,} anomalies in the selected range; the most recent was "
                    f"{a.iloc[0].anomaly_type.replace('_', ' ')} on {a.iloc[0].period_start:%d %b}. "
                    "The Anomalies page lists them all.")
    for r in a.head(6).itertuples():
        ui.anomaly_card(r, f["location_names"])
    ui.source("parquet_data/anomalies")

st.subheader("Download reports")
files = dl.report_files()
choice = st.selectbox("Report", files, format_func=lambda p: p.name)
st.download_button("Download report", choice.read_bytes(), file_name=choice.name,
                   mime="text/markdown", on_click=db.log,
                   args=(user["id"], "export_report", choice.name))

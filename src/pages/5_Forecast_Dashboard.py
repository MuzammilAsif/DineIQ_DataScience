import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import data_loader as dl
import ui

user, f = ui.page("Demand Forecast")

with ui.guard("Could not load forecast outputs."):
    meta = dl.forecast_meta()
    cfg = meta["config"]
st.caption(f"Spark MLlib GBTRegressor, model {meta['version']}. Horizon {cfg['horizon_days']} "
           f"days; test period starts {meta['test_start']} (the last {cfg['test_days']} days, never "
           "seen in training). The baseline is same-weekday-last-week.")

LEVELS = {"Chain total": ("overall_day", None), "Location": ("location_day", "location_id"),
          "Menu category": ("category_day", "category_name"), "Menu item": ("item_day", "item_id")}
level = st.radio("Level", list(LEVELS), horizontal=True)
table, key = LEVELS[level]
with ui.guard("Could not load the forecast table."):
    fc = dl.forecast(table)
    hist = dl.daily_item_sales()
    items = dl.classification().set_index("item_id").item_name.to_dict()

entity = None
if key == "location_id":
    options = f["locations"] or sorted(fc.location_id.unique())
    entity = st.selectbox("Location", options, format_func=lambda x: f"{x} · {f['location_names'].get(x, x)}")
elif key == "category_name":
    options = f["categories"] or sorted(fc.category_name.dropna().unique())
    entity = st.selectbox("Menu category", options)
elif key == "item_id":
    options = sorted(fc.item_id.unique(), key=lambda i: items.get(i, i))
    entity = st.selectbox("Menu item", options, format_func=lambda i: items.get(i, i))
elif f["locations"] or f["categories"]:
    st.caption("Chain total ignores the location and category filters; pick a level to apply them.")

if key:
    fc = fc[fc[key] == entity]
    hist = hist[hist[key] == entity]
hist = ui.filter_df(hist, f, date_col="date").groupby("date", as_index=False).quantity.sum()
hist = hist[hist.date < fc.target_date.min()]
fc = fc.sort_values("target_date")

st.subheader("History and forecast")
fig = go.Figure()
hover = "%{x|%d %b %Y}: %{y:,.0f} units"
fig.add_scatter(x=hist.date, y=hist.quantity, name="Actual (history)",
                line=dict(color=ui.LIGHT_GRAY, width=1.5), hovertemplate=hover)
fig.add_scatter(x=fc.target_date, y=fc.actual, name="Actual (test period)",
                line=dict(color=ui.INK, width=2), hovertemplate=hover)
fig.add_scatter(x=fc.target_date, y=fc.forecast, name="Model forecast",
                line=dict(color=ui.RED, width=2), hovertemplate=hover)
fig.add_scatter(x=fc.target_date, y=fc.baseline, name="Baseline (same weekday last week)",
                line=dict(color=ui.GRAY, width=1.5, dash="dot"), hovertemplate=hover)
fig.update_layout(xaxis_title="Date", yaxis_title="Units sold per day", hovermode="x unified")
ui.chart(fig, source_path=f"parquet_data/demand_forecast/{table}")


def errs(pred):
    e = fc[pred] - fc.actual
    nz = fc.actual != 0
    return dict(MAE=e.abs().mean(), RMSE=np.sqrt((e ** 2).mean()),
                MAPE=(e.abs()[nz] / fc.actual[nz]).mean() if nz.any() else np.nan)


m, b = errs("forecast"), errs("baseline")
tiles = []
for k in ["MAE", "RMSE", "MAPE"]:
    fmt = "{:.1%}" if k == "MAPE" else "{:,.2f}"
    tone = "pos" if m[k] < b[k] else "neg" if m[k] > b[k] else None
    tiles.append((f"{k} (model)", fmt.format(m[k]), f"baseline {fmt.format(b[k])}", tone))
ui.kpis(tiles)
st.caption("Green means the model error is lower than the baseline's, red means higher.")
better = [k for k in m if m[k] < b[k]]
ui.takeaway(f"For this {level.lower()} view the model beats the baseline on "
            f"{', '.join(better) if better else 'no metric'} over {len(fc)} test days. MAE is "
            f"{m['MAE']:,.1f} units a day against {b['MAE']:,.1f} "
            f"({m['MAE'] / b['MAE'] - 1:+.0%}).")

st.subheader("Accuracy at every level (whole test period)")
rows = []
for lvl, v in meta["metrics"].items():
    rows.append(dict(level=lvl, rows=v["model"]["n"], model_mae=v["model"]["mae"],
                     baseline_mae=v["baseline"]["mae"], model_rmse=v["model"]["rmse"],
                     baseline_rmse=v["baseline"]["rmse"], model_mape=v["model"]["mape"],
                     baseline_mape=v["baseline"]["mape"], model_r2=v["model"]["r2"]))
acc = pd.DataFrame(rows)
st.dataframe(acc, hide_index=True, width="stretch",
             column_config={c: st.column_config.NumberColumn(format="percent")
                            for c in ["model_mape", "baseline_mape"]})
ui.source(f"models/spark/demand_forecast/{meta['version']}/model_version.txt")
ui.download_df(acc, "forecast_accuracy")
base = acc.set_index("level").loc["item_location_day"]
losses = [f"{k.upper()} at {r.level}" for r in acc.itertuples() for k in ("mae", "rmse", "mape")
          if pd.notna(getattr(r, f"model_{k}")) and pd.notna(getattr(r, f"baseline_{k}"))
          and getattr(r, f"model_{k}") > getattr(r, f"baseline_{k}")]
ui.takeaway(f"At the item x location x day grain the model's MAE is {base.model_mae:.2f} against "
            f"{base.baseline_mae:.2f} for the baseline ({base.model_mae / base.baseline_mae - 1:+.0%}). "
            + (f"The baseline wins only on {', '.join(losses)}; reports/forecast_report.md explains why."
               if losses else "The model beats the baseline on every metric at every level."))

st.subheader("Actual vs predicted")
fig = go.Figure(go.Scatter(x=fc.actual, y=fc.forecast, mode="markers", name="Test day",
                           marker=dict(color=ui.RED, size=8, line=dict(width=1, color="#FFFFFF")),
                           customdata=fc.target_date,
                           hovertemplate="%{customdata|%d %b %Y}<br>Actual %{x:,.0f}, "
                                         "forecast %{y:,.0f} units<extra></extra>"))
top = float(max(fc.actual.max(), fc.forecast.max()))
fig.add_scatter(x=[0, top], y=[0, top], mode="lines", name="Perfect forecast",
                line=dict(color=ui.GRAY, dash="dash", width=1.5), hoverinfo="skip")
fig.update_layout(xaxis_title="Actual units per day", yaxis_title="Forecast units per day")
ui.chart(fig, source_path=f"parquet_data/demand_forecast/{table}")

st.subheader("High-demand periods in the forecast window")
with ui.guard("Could not load location forecasts."):
    loc = dl.forecast("location_day")
if f["locations"]:
    loc = loc[loc.location_id.isin(f["locations"])]
loc = loc.assign(vs_recent=loc.forecast / loc.recent_average - 1,
                 location=loc.location_id.map(f["location_names"]))
peaks = loc[loc.vs_recent >= 0.25].sort_values("vs_recent", ascending=False)
peaks = peaks.assign(target_date=peaks.target_date.dt.date)
st.dataframe(peaks[["target_date", "location", "forecast", "recent_average", "vs_recent", "actual"]]
             .head(20), hide_index=True, width="stretch",
             column_config={"vs_recent": st.column_config.NumberColumn(format="percent")})
ui.source("parquet_data/demand_forecast/location_day")
if len(peaks):
    busiest = peaks.target_date.value_counts().index[0]
    ui.takeaway(f"{len(peaks)} location-days are forecast at least 25% above their recent level. "
                f"The most common date is {busiest:%d %b}, so staffing and stock should go up before "
                "it.")
else:
    ui.takeaway("No location-day is forecast 25% or more above its recent level.")

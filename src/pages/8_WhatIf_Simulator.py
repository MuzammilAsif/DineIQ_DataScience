import sys

import pandas as pd
import streamlit as st

import data_loader as dl
import db
import ui

sys.path.insert(0, str(dl.ROOT))
from python_pipeline import what_if_simulator as wi  # noqa: E402

SCENARIOS = {
    "price_change": "Increase or reduce menu price",
    "discount_change": "Change discount %",
    "promotion_frequency": "Increase promotion frequency",
    "remove_item": "Remove the menu item",
    "reduce_prep": "Reduce preparation quantity",
    "demand_change": "Change predicted demand",
    "wastage_assumption": "Change wastage assumption",
}
METRICS = [("revenue", "Revenue", "money"), ("contribution_margin", "Contribution margin", "money"),
           ("demand", "Demand (units)", "units"), ("wastage_cost", "Wastage cost", "money"),
           ("wastage_pct", "Wastage share", "pct"), ("profitability", "Profitability", "pct"),
           ("net_margin_after_wastage", "Net margin after wastage", "money")]

user, f = ui.page("What-If Simulator")
st.caption("Formula-based estimates from each item's current price, cost, sales, wastage and "
           "price elasticity (python_pipeline/what_if_simulator.py). No model is re-run.")

with ui.guard("Could not load the item list."):
    items = ui.filter_df(dl.classification(), f, cat_col="category_name", cls_col="category")
if items.empty:
    st.warning("No items match the menu category and performance class filters.")
    st.stop()
names = dict(zip(items.item_id, items.item_name))
c1, c2 = st.columns(2)
item = c1.selectbox("Menu item", sorted(names, key=names.get), format_func=names.get)
scenario = c2.selectbox("Scenario", list(SCENARIOS), format_func=SCENARIOS.get)

with ui.guard("Could not load this item's baseline."):
    b = wi.load_baseline(item)

params = {}
if scenario == "price_change":
    params["pct"] = st.slider("Price change %", -50, 50, 10) / 100
elif scenario == "discount_change":
    params["new_discount_pct"] = st.slider(
        f"New discount % (current {b.discount_pct:.1%})", 0, 60, int(round(b.discount_pct * 100))) / 100
elif scenario == "promotion_frequency":
    params["factor"] = st.slider(f"Promotion frequency multiplier (promoted revenue share now "
                                 f"{b.promotion_dependency:.0%})", 0.0, 3.0, 1.5, 0.1)
elif scenario == "reduce_prep":
    params["pct"] = st.slider("Reduce preparation by %", 0, 60, 10) / 100
elif scenario == "demand_change":
    params["pct"] = st.slider("Demand change %", -50, 100, 10) / 100
elif scenario == "wastage_assumption":
    params["new_wastage_pct"] = st.slider(f"Wastage share of portions % (current "
                                          f"{b.wastage_pct:.1%})", 0.0, 40.0,
                                          round(b.wastage_pct * 100, 1), 0.5) / 100

with ui.guard("The simulation failed for these inputs."):
    r = wi.SCENARIOS[scenario](b, **params)
run_key = f"{item} {scenario} {params}"
if st.session_state.get("last_what_if") != run_key:
    db.log(user["id"], "what_if", run_key)
    st.session_state["last_what_if"] = run_key

st.warning(f"**{r['note']}.** These numbers come from formulas on current metrics; they are "
           "not a forecast of what will happen.")
st.subheader(f"{names[item]}: {SCENARIOS[scenario].lower()}")


def fmt(v, kind):
    return ui.money(v) if kind == "money" else f"{v:.1%}" if kind == "pct" else f"{v:,.0f}"


tiles = []
for k, label, kind in METRICS[:4] + METRICS[5:6] + METRICS[6:]:
    delta = r["change"][k]
    shown = ui.money_short(r["simulated"][k]) if kind == "money" else fmt(r["simulated"][k], kind)
    text = f"{delta:+.1%}" if kind == "pct" else f"{delta:+,.0f}"
    # Lower wastage cost is the good direction.
    tiles.append((label, shown, *ui.signed(-delta if k == "wastage_cost" else delta, text)))
ui.kpis(tiles[:3])
st.write("")
ui.kpis(tiles[3:])
table = pd.DataFrame([{"metric": label, "baseline": fmt(r["baseline"][k], kind),
                       "simulated": fmt(r["simulated"][k], kind),
                       "change": (f"{r['change'][k]:+.1%}" if kind == "pct"
                                  else f"{r['change'][k]:+,.0f}")} for k, label, kind in METRICS])
st.dataframe(table, hide_index=True, width="stretch")
ui.source("python_pipeline/what_if_simulator.py")

for flag in r["flags"]:
    st.error(flag)
for lim in r["limitations"]:
    st.caption(f"Limitation: {lim}")
d_margin = r["change"]["net_margin_after_wastage"]
ui.takeaway(f"Estimated net margin after wastage goes from "
            f"{ui.money(r['baseline']['net_margin_after_wastage'])} to "
            f"{ui.money(r['simulated']['net_margin_after_wastage'])} ({d_margin:+,.0f}). "
            f"Revenue changes by {r['change']['revenue']:+,.0f} and demand by "
            f"{r['change']['demand']:+,.0f} units.")
with st.expander("Assumptions and baseline inputs"):
    st.write(f"Elasticity {b.elasticity:+.2f} ({b.elasticity_source}); typical promotion margin "
             f"effect {b.promo_margin_effect:+.1%}; period {b.period_days} days; forecast daily "
             f"demand {b.forecast_daily_demand if b.forecast_daily_demand is not None else 'n/a'}.")
    st.json({k: v for k, v in r["assumptions"].items()})
    st.json(b.__dict__)

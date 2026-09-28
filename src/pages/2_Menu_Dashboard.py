import plotly.express as px
import streamlit as st

import data_loader as dl
import ui

user, f = ui.page("Menu Intelligence")
st.caption(f"Menu classification snapshot as of {dl.as_of()}. The date filter does not apply.")

with ui.guard("Could not load menu classification."):
    cls = dl.classification()
    if f["locations"]:
        loc = dl.classification_by_location()
        loc = loc[loc.location_id.isin(f["locations"])]
        cls = (loc.drop(columns=["global_category"])
               .merge(cls[["item_id", "item_name", "category_name"]], on="item_id"))
view = ui.filter_df(cls, f, cat_col="category_name", cls_col="category")
ui.applied(f, ["location", "category", "class"])
if view.empty:
    st.warning("No items match the current filters.")
    st.stop()
if f["locations"]:
    st.caption("Showing per-location classification (each item ranked against the other items "
               "at the same location).")

counts = view.category.value_counts().reindex(ui.CLASSES).dropna().astype(int)
ui.kpis([("Rows shown", f"{len(view):,}")]
        + [(k, v, f"{v / len(view):.0%} of rows", "accent" if k == "Profit Driver" else None)
           for k, v in counts.items()])
src = "parquet_data/menu_classification_by_location" if f["locations"] else "parquet_data/menu_classification"
ui.source(src, dl.as_of())

st.subheader("Performance map")
fig = px.scatter(view, x="demand_percentile", y="profitability_percentile", color="category",
                 hover_name="item_name", hover_data={"profit_percentage": ":.1f",
                                                     "total_quantity_sold": ":,",
                                                     "demand_percentile": ":.0%",
                                                     "profitability_percentile": ":.0%"},
                 category_orders={"category": ui.CLASSES}, color_discrete_map=ui.CLASS_COLORS,
                 labels={"demand_percentile": "Demand percentile",
                         "profitability_percentile": "Profitability percentile",
                         "profit_percentage": "Margin %", "total_quantity_sold": "Units sold",
                         "category": ""})
fig.update_traces(marker=dict(size=9, line=dict(width=1, color="#FFFFFF")))
fig.update_xaxes(tickformat=".0%")
fig.update_yaxes(tickformat=".0%")
ui.chart(fig, source_path=src)

low = view[view.category == "Low Performer"]
if len(low):
    drivers = {"low demand": (low.demand_percentile < 0.33).mean(),
               "low profitability": (low.profitability_percentile < 0.33).mean(),
               "high wastage": (low.wastage_percentile > 0.67).mean()}
    main = max(drivers, key=drivers.get)
    ui.takeaway(f"{len(low)} of {len(view)} rows are Low Performers, driven mainly by {main} "
                f"({drivers[main]:.0%} of them). " + "; ".join(
                    f"{v:.0%} have {k}" for k, v in drivers.items() if k != main) + ".")
pd_ = view[view.category == "Profit Driver"].sort_values("contribution_margin", ascending=False)
if len(pd_):
    top = pd_.iloc[0]
    ui.takeaway(f"{len(pd_)} Profit Drivers. The largest is {top.item_name}, with a "
                f"{top.profit_percentage:.1f}% margin and {ui.money(top.contribution_margin)} "
                "contribution.")

st.subheader("Item performance")
cols = ["item_name", "category_name", "category", "profit_percentage", "contribution_margin",
        "total_quantity_sold", "average_rating", "repeat_purchase_rate", "wastage_percentage"]
if "location_id" in view.columns:
    cols.insert(0, "location_id")
table = view[[c for c in cols if c in view.columns]].sort_values("contribution_margin",
                                                                  ascending=False)
st.dataframe(table, hide_index=True, width="stretch",
             column_config={"wastage_percentage": st.column_config.NumberColumn(format="percent"),
                            "repeat_purchase_rate": st.column_config.NumberColumn(format="percent"),
                            "contribution_margin": st.column_config.NumberColumn(format="%,.0f")})
ui.source(src, dl.as_of())
ui.download_df(table, "menu_performance")

st.subheader("Margins by menu category")
by_cat = (view.groupby("category_name", as_index=False)
          .agg(margin=("contribution_margin", "sum"), items=("item_id", "count")))
ui.chart(ui.hbar(by_cat, "margin", "category_name", "Contribution margin (PKR)"), source_path=src)
best, worst = by_cat.loc[by_cat.margin.idxmax()], by_cat.loc[by_cat.margin.idxmin()]
ui.takeaway(f"{best.category_name} contributes the most margin ({ui.money(best.margin)} from "
            f"{best['items']} rows); {worst.category_name} the least ({ui.money(worst.margin)}).")

st.subheader("Slow-moving items")
with ui.guard("Could not load slow-moving items."):
    slow = dl.slow_moving()
slow = ui.filter_df(slow[slow.slow_moving].merge(dl.classification()[["item_id", "category_name"]],
                                                 on="item_id"), f, cat_col="category_name",
                    cls_col="category")
st.dataframe(slow[["item_name", "category_name", "category", "total_quantity_sold",
                   "demand_percentile", "repeat_purchase_rate", "relative_trend", "average_rating"]],
             hide_index=True, width="stretch",
             column_config={c: st.column_config.NumberColumn(format="percent")
                            for c in ["repeat_purchase_rate", "relative_trend"]})
if len(slow):
    ui.takeaway(f"{len(slow)} items are slow-moving: low demand, low repeat purchase, and a flat or "
                f"falling trend. The weakest is {slow.sort_values('demand_percentile').iloc[0].item_name}. "
                "These are the first candidates to replace.")
else:
    st.caption("No slow-moving items match the filters.")
ui.source("parquet_data/slow_moving")

st.subheader("Ratings and wastage")
worst_rated = view.dropna(subset=["average_rating"]).sort_values("average_rating").head(5)
st.dataframe(worst_rated[["item_name", "average_rating", "total_quantity_sold", "wastage_percentage"]],
             hide_index=True,
             column_config={"wastage_percentage": st.column_config.NumberColumn(format="percent"),
                            "average_rating": st.column_config.NumberColumn(format="%.2f")})
ui.takeaway(f"Lowest rated: {', '.join(f'{r.item_name} ({r.average_rating:.2f})' for r in worst_rated.itertuples())}. "
            f"The highest wastage share is {view.wastage_percentage.max():.1%} "
            f"({view.loc[view.wastage_percentage.idxmax(), 'item_name']}).")
ui.source(src, dl.as_of())

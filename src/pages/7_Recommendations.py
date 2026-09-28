import streamlit as st

import data_loader as dl
import ui

PRIORITIES = ui.PRIORITIES

user, f = ui.page("Recommendations")
with ui.guard("Could not load recommendations."):
    recs = dl.recommendations()
    item_info = dl.classification().set_index("item_id")[["category_name", "category"]]
st.caption(f"{len(recs)} recommendations as of {recs.as_of_date.iloc[0]}, from "
           "python_pipeline/recommendation_engine.py. Priority is the rank of each "
           "recommendation's estimated impact across all of them.")

c1, c2, c3 = st.columns([1, 2, 2])
pri = c1.multiselect("Priority", PRIORITIES, default=["Critical", "High"])
types = c2.multiselect("Type", sorted(recs.type.unique()))
text = c3.text_input("Search (item, segment, location, promotion)")

view = recs[recs.priority.isin(pri or PRIORITIES)]
if types:
    view = view[view.type.isin(types)]
if text:
    t = text.lower()
    view = view[view.target_name.str.lower().str.contains(t) | view.target_id.str.lower().str.contains(t)
                | view.action.str.lower().str.contains(t)]
items = view.target_type == "item"
cat = view.target_id.map(item_info.category_name)
cls = view.target_id.map(item_info.category)
if f["categories"]:
    view = view[~items | cat.isin(f["categories"])]
if f["classes"]:
    view = view[~items | cls.reindex(view.index).isin(f["classes"])]
if f["locations"]:
    view = view[(view.target_type != "location") | view.target_id.isin(f["locations"])]
ui.applied(f, ["location", "category", "class"])
st.caption("Menu category and performance class filters apply to item recommendations, and the "
           "location filter to location recommendations.")

counts = view.priority.value_counts()
ui.kpis([(p, int(counts.get(p, 0)), None, "accent" if p == "Critical" else None)
         for p in PRIORITIES])
ui.source("parquet_data/recommendations", recs.as_of_date.iloc[0])
if view.empty:
    st.warning("No recommendations match the current filters.")
    st.stop()
top_type = view.type.value_counts()
ui.takeaway(f"{len(view)} recommendations shown. The most common type is "
            f"{top_type.index[0].lower()} ({top_type.iloc[0]}). The highest-impact one is "
            f"\"{view.iloc[0].action}\" ({view.iloc[0].impact_basis}: {view.iloc[0].estimated_impact_value:,.0f}).")
export = view.assign(evidence=view.evidence.map(lambda e: " | ".join(e)))
ui.download_df(export, "recommendations")

limit = st.number_input("Show", min_value=1, max_value=len(view), value=min(12, len(view)), step=10)
for r in view.head(int(limit)).itertuples():
    ui.card(r.priority, r.action,
            f"{r.target_type.title()}: {r.target_name} · estimated impact "
            f"{r.estimated_impact_value:,.0f} ({r.impact_basis})",
            r.evidence, f"{r.recommendation_id} · {r.type}")

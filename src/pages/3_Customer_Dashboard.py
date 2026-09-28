import plotly.express as px
import streamlit as st

import data_loader as dl
import ui

user, f = ui.page("Customer Intelligence")
st.caption(f"Customer segments as of {dl.as_of()} (Step 6 KMeans, module 07). The location "
           "filter uses each customer's home location; the date filter does not apply.")

with ui.guard("Could not load customer segments."):
    seg = dl.customer_segments()
if f["locations"]:
    seg = seg[seg.home_location_id.isin(f["locations"])]
ui.applied(f, ["location"])
if seg.empty:
    st.warning("No customers match the current filters.")
    st.stop()

ui.kpis([("Customers", f"{len(seg):,}"),
         ("Total spend", ui.money_short(seg.customer_monetary_value.sum()), None, "accent"),
         ("Churn-risk flagged", f"{seg.churn_risk.fillna(False).sum():,}",
          f"{seg.churn_risk.fillna(False).mean():.1%} of customers"),
         ("Loyalty members", f"{seg.loyalty_member.mean():.0%}")])
SRC = "parquet_data/customer_segments"
ui.source(SRC)

st.subheader("Segment breakdown")
summary = (seg.groupby("segment", as_index=False)
           .agg(customers=("customer_id", "count"), total_spend=("customer_monetary_value", "sum"),
                avg_recency_days=("customer_recency", "mean"),
                avg_orders=("customer_frequency", "mean"),
                avg_order_value=("average_order_value", "mean"),
                promo_share=("promo_order_share", "mean"),
                churn_risk_share=("churn_risk", lambda s: s.fillna(False).mean()))
           .sort_values("total_spend", ascending=False))
summary["spend_share"] = summary.total_spend / summary.total_spend.sum()
left, right = st.columns(2)
left.markdown("**Customers per segment**")
ui.chart(ui.hbar(summary, "customers", "segment", "Customers"), container=left)
right.markdown("**Share of total spend**")
ui.chart(ui.hbar(summary, "spend_share", "segment", "Share of spend", color=ui.INK, fmt=".1%")
         .update_xaxes(tickformat=".0%"), container=right)
st.dataframe(summary, hide_index=True, width="stretch",
             column_config={c: st.column_config.NumberColumn(format="percent")
                            for c in ["promo_share", "churn_risk_share", "spend_share"]})
ui.source(SRC)
ui.download_df(summary, "segment_summary")
top = summary.iloc[0]
cust_share = top.customers / summary.customers.sum()
ui.takeaway(f"{top.segment} customers are {cust_share:.0%} of the base but bring "
            f"{top.spend_share:.0%} of spend ({ui.money(top.total_spend)}), averaging "
            f"{top.avg_orders:.1f} orders. The highest churn-risk share is in "
            f"{summary.loc[summary.churn_risk_share.idxmax(), 'segment']} "
            f"({summary.churn_risk_share.max():.0%}).")

st.subheader("RFM score distribution")
fig = px.histogram(seg, x="rfm_score", color="segment", nbins=13,
                   category_orders={"segment": summary.segment.tolist()},
                   labels={"rfm_score": "RFM score (3 = weakest, 15 = strongest)", "segment": ""})
fig.update_layout(yaxis_title="Customers", bargap=0.08)
ui.chart(fig, source_path=SRC)
ui.takeaway(f"Median RFM score {seg.rfm_score.median():.0f}; {(seg.rfm_score >= 13).mean():.0%} "
            f"of customers score 13 or more, and {(seg.rfm_score <= 5).mean():.0%} score 5 or less.")

cols = ["customer_id", "segment", "rfm_code", "customer_recency", "customer_frequency",
        "customer_monetary_value", "average_order_value", "promo_order_share", "home_location_id"]
tabs = st.tabs(["High-value", "At-risk", "Promotion-sensitive"])
with tabs[0]:
    hv = seg[seg.segment == "High-Value Loyal"].sort_values("customer_monetary_value",
                                                            ascending=False)
    st.dataframe(hv[cols].head(200), hide_index=True, width="stretch")
    ui.download_df(hv[cols], "high_value_customers")
    if len(hv):
        ui.takeaway(f"{len(hv):,} High-Value Loyal customers; the top 10% of them spend "
                    f"{ui.money(hv.head(max(1, len(hv) // 10)).customer_monetary_value.mean())} each "
                    "on average.")
with tabs[1]:
    ar = seg[seg.churn_risk.fillna(False)].sort_values("customer_monetary_value", ascending=False)
    st.dataframe(ar[cols + ["orders_prev_quarter", "orders_last_quarter"]].head(200),
                 hide_index=True, width="stretch")
    ui.download_df(ar[cols], "at_risk_customers")
    if len(ar):
        ui.takeaway(f"{len(ar):,} customers are flagged by the churn rule (long gap and fewer "
                    f"orders last quarter). Together they spent {ui.money(ar.customer_monetary_value.sum())}. "
                    f"The largest group is {ar.segment.value_counts().index[0]}.")
with tabs[2]:
    ps = seg[(seg.promo_order_share >= 0.5) & (seg.customer_frequency >= 2)].sort_values(
        "customer_monetary_value", ascending=False)
    st.dataframe(ps[cols].head(200), hide_index=True, width="stretch")
    ui.download_df(ps[cols], "promotion_sensitive_customers")
    ui.takeaway(f"{len(ps):,} repeat customers used a promotion on at least half their orders. "
                "Target them with offers on high-margin items rather than blanket discounts.")

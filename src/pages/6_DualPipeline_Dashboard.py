import streamlit as st

import auth
import data_loader as dl
import ui

user, f = ui.page("Model Comparison", auth.MANAGERS_AND_ANALYSTS)
st.caption("Spark (PySpark + MLlib) against the independent Python pipeline (pandas + "
           "scikit-learn) on unseen records. Source: reports/dual_pipeline_comparison.csv.")
ui.applied(f, [])

with ui.guard("Could not load the comparison results."):
    d = dl.dual_comparison()
task = st.radio("Task", ["All", "customer", "menu_item"], horizontal=True,
                format_func=lambda t: {"All": "Both tasks", "customer": "Customer segmentation",
                                       "menu_item": "Menu classification"}[t])
if task != "All":
    d = d[d.record_type == task]

match = (d.spark_result == d.python_result).mean()
SRC = "reports/dual_pipeline_comparison.csv"
ui.kpis([("Records compared", f"{len(d):,}"),
         ("Spark vs Python agreement", f"{match:.1%}", None, "accent"),
         ("Spark vs Actual", f"{(d.spark_result == d.actual).mean():.1%}", "accuracy"),
         ("Python vs Actual", f"{(d.python_result == d.actual).mean():.1%}", "accuracy")])
ui.source(SRC)
mism = d[d.spark_python_match == "Mismatch"]
gap = abs((d.spark_result == d.actual).mean() - (d.python_result == d.actual).mean()) * 100
ui.takeaway(f"The two pipelines agree on {match:.1%} of {len(d):,} records; {len(mism):,} "
            f"disagree. Their accuracy against Actual differs by {gap:.1f} points"
            + (", so the two implementations reach effectively the same results." if match >= 0.95
               else "."))

left, right = st.columns(2)
with left:
    st.subheader("Consistency status")
    status = d.final_consistency_status.value_counts().rename_axis("status").reset_index(name="records")
    ui.chart(ui.hbar(status, "records", "status", "Records"))
with right:
    st.subheader("Agreement by actual class")
    by = (d.assign(agree=d.spark_result == d.python_result,
                   spark_ok=d.spark_result == d.actual, python_ok=d.python_result == d.actual)
          .groupby("actual", as_index=False)
          .agg(records=("record_id", "count"), spark_vs_python=("agree", "mean"),
               spark_correct=("spark_ok", "mean"), python_correct=("python_ok", "mean")))
    st.dataframe(by, hide_index=True, width="stretch",
                 column_config={c: st.column_config.NumberColumn(format="percent")
                                for c in ["spark_vs_python", "spark_correct", "python_correct"]})
worst = by.sort_values("spark_vs_python").iloc[0]
ui.takeaway(f"The lowest agreement is on {worst.actual} ({worst.spark_vs_python:.1%} of "
            f"{worst.records:,} records). reports/dual_pipeline_comparison_report.md explains each "
            "disagreement pattern, with the numbers behind it.")

st.subheader("Disagreements")
st.dataframe(mism, hide_index=True, width="stretch")
ui.source(SRC)
ui.download_df(mism, "pipeline_disagreements")

st.subheader("All records")
status_pick = st.multiselect("Consistency status", sorted(d.final_consistency_status.unique()))
shown = d[d.final_consistency_status.isin(status_pick)] if status_pick else d
st.dataframe(shown.head(2000), hide_index=True, width="stretch")
st.caption(f"Showing up to 2,000 of {len(shown):,} rows; the download has all of them.")
ui.download_df(shown, "dual_pipeline_comparison")

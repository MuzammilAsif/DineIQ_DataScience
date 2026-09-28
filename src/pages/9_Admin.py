import pandas as pd
import streamlit as st
from werkzeug.security import generate_password_hash

import auth
import data_loader as dl
import db
import model_registry
import ui

user, f = ui.page("Admin", ["Administrator"])
ui.applied(f, [])

tabs = st.tabs(["Job monitoring", "Model registry", "Audit log", "Users", "Master data"])

with tabs[0]:
    with ui.guard("Could not read the job logs."):
        jobs = dl.job_runs().sort_values("finished", ascending=False)
    st.dataframe(jobs, hide_index=True, width="stretch",
                 column_config={"duration_s": st.column_config.NumberColumn("duration (s)",
                                                                             format="%.0f")})
    done = (jobs.status == "Completed").sum()
    longest = jobs.loc[jobs.duration_s.idxmax()]
    ui.takeaway(f"{done} of {len(jobs)} Spark jobs completed on their last run. The longest is "
                f"{longest.job} ({longest.duration_s / 60:.1f} min). Each row is read from "
                "reports/spark_execution_log_<job>.txt; status is Completed when every START "
                "stage has a matching END.")
    pick = st.selectbox("Show log", jobs.job)
    st.code((dl.REPORTS / f"spark_execution_log_{pick}.txt").read_text()[-6000:], language=None)

with tabs[1]:
    if st.button("Refresh registry from saved models"):
        with ui.guard("Could not rebuild the registry."):
            model_registry.write()
        db.log(user["id"], "admin_refresh_registry")
    with ui.guard("Could not read models/model_registry.json."):
        reg = model_registry.read()
    st.caption(f"Generated {reg['generated_at']} from each model's model_version.txt.")
    st.dataframe(pd.DataFrame(reg["models"]), hide_index=True, width="stretch")

with tabs[2]:
    audit = pd.DataFrame(db.recent_audit(500))
    if not audit.empty:
        kinds = st.multiselect("Action", sorted(audit.action.unique()))
        if kinds:
            audit = audit[audit.action.isin(kinds)]
    st.dataframe(audit, hide_index=True, width="stretch")
    ui.download_df(audit, "audit_log")

with tabs[3]:
    users = pd.DataFrame(db.list_users())
    st.dataframe(users, hide_index=True, width="stretch")
    with st.form("add_user", clear_on_submit=True):
        st.markdown("**Add user**")
        c = st.columns(3)
        username = c[0].text_input("Username")
        password = c[1].text_input("Password", type="password")
        role = c[2].selectbox("Role", auth.ROLES)
        c = st.columns(2)
        full_name = c[0].text_input("Full name")
        loc = c[1].selectbox("Assigned location (Restaurant Manager)", [""] + list(f["location_names"]))
        if st.form_submit_button("Create user"):
            if not username or len(password) < 8:
                st.error("Username is required and the password needs at least 8 characters.")
            elif db.get_user(username):
                st.error(f"User {username} already exists.")
            else:
                db.add_user(username, generate_password_hash(password), role, full_name, loc or None)
                db.log(user["id"], "admin_add_user", f"{username} ({role})")
                st.success(f"Created {username}.")
                st.rerun()
    others = users[users.id != user["id"]]
    if len(others):
        c = st.columns([3, 1])
        victim = c[0].selectbox("Delete user", others.id,
                                format_func=lambda i: others.set_index("id").username[i])
        if c[1].button("Delete"):
            db.delete_user(int(victim))
            db.log(user["id"], "admin_delete_user", str(victim))
            st.rerun()

with tabs[4]:
    st.caption("Add, edit or deactivate the small reference tables. Edits are stored in the app "
               "database (database/app.db) and audited. The analytics pipeline reads "
               "processed_data/, so edits reach the dashboards only on the next pipeline run fed "
               "from them. Transaction tables (orders, ratings, inventory, wastage) are managed by "
               "the pipeline and are not editable here.")
    table = st.radio("Table", ["restaurants", "menu_items"], horizontal=True,
                     format_func=lambda t: {"restaurants": "Restaurants", "menu_items": "Menu items"}[t])
    key, cols = db.MASTER[table]
    rows = pd.DataFrame(db.master_rows(table))
    st.dataframe(rows, hide_index=True, width="stretch")
    choice = st.selectbox("Edit record", ["(new)"] + rows[key].tolist())
    current = rows.set_index(key).loc[choice].to_dict() if choice != "(new)" else {}
    with st.form(f"edit_{table}"):
        values = {key: choice if choice != "(new)" else st.text_input(key)}
        for col in cols:
            if col in (key, "is_active"):
                continue
            v = current.get(col)
            if col in ("base_price", "base_cost"):
                values[col] = st.number_input(col, value=float(v or 0.0), min_value=0.0)
            elif col == "seating_capacity":
                values[col] = st.number_input(col, value=int(v or 0), min_value=0, step=1)
            elif col == "is_seasonal":
                values[col] = int(st.checkbox(col, value=bool(v)))
            else:
                values[col] = st.text_input(col, value="" if v is None else str(v))
        values["is_active"] = int(st.checkbox("Active", value=bool(current.get("is_active", 1))))
        if st.form_submit_button("Save"):
            if not values[key]:
                st.error(f"{key} is required.")
            else:
                db.upsert_master(table, values)
                db.log(user["id"], f"admin_save_{table}", values[key])
                st.success(f"Saved {values[key]}.")
                st.rerun()
    if choice != "(new)":
        active = bool(current.get("is_active", 1))
        if st.button("Deactivate" if active else "Reactivate"):
            db.set_active(table, choice, not active)
            db.log(user["id"], f"admin_{'deactivate' if active else 'reactivate'}_{table}", choice)
            st.rerun()

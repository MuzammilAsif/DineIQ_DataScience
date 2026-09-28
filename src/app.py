"""DineIQ Analytics web app. Run from the project root: .venv/bin/streamlit run src/app.py"""
import sys
from pathlib import Path

import streamlit as st

import auth
import ui

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "database"))
import init_db  # noqa: E402

st.set_page_config(page_title="DineIQ Analytics", layout="wide")


@st.cache_resource(show_spinner="Setting up the user database...")
def ensure_database():
    """Creates and seeds database/app.db on a fresh clone (e.g. Streamlit Cloud)."""
    init_db.ensure()


ensure_database()


def login():
    ui.css()
    st.title("DineIQ Analytics")
    st.caption("Restaurant intelligence for the DineIQ chain. Please sign in.")
    left, _ = st.columns([1, 2])
    with left.form("login"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in", type="primary")
    if submitted:
        try:
            ok = auth.login(st.session_state, username, password)
        except Exception as e:  # noqa: BLE001
            st.error(f"Login is unavailable: the user database could not be read ({type(e).__name__}). "
                     "Run database/init_db.py.")
            st.stop()
        if ok:
            st.rerun()
        st.error("Incorrect username or password.")


if "user" not in st.session_state:
    st.navigation([st.Page(login, title="Sign in")], position="hidden").run()
    st.stop()

ALL, MGR, ADMIN = auth.ALL_ROLES, auth.MANAGERS_AND_ANALYSTS, ["Administrator"]
SECTIONS = {
    "Overview": [
        ("1_Executive_Dashboard", "Executive Dashboard", "dashboard", ALL),
    ],
    "Intelligence": [
        ("2_Menu_Dashboard", "Menu Intelligence", "restaurant_menu", ALL),
        ("3_Customer_Dashboard", "Customer Intelligence", "group", ALL),
        ("5_Forecast_Dashboard", "Demand Forecast", "trending_up", ALL),
        ("4_Wastage_Dashboard", "Wastage", "delete", ALL),
    ],
    "Decisions & Actions": [
        ("10_Anomalies", "Anomalies", "warning", ALL),
        ("7_Recommendations", "Recommendations", "task_alt", ALL),
        ("8_WhatIf_Simulator", "What-If Simulator", "tune", ALL),
    ],
    "Platform": [
        ("6_DualPipeline_Dashboard", "Model Comparison", "compare_arrows", MGR),
        ("9_Admin", "Admin", "admin_panel_settings", ADMIN),
    ],
}
role = st.session_state["user"]["role"]
nav = {}
for section, pages in SECTIONS.items():
    allowed = [st.Page(f"pages/{f}.py", title=t, icon=f":material/{i}:",
                       default=f.startswith("1_"))
               for f, t, i, roles in pages if role in roles]
    if allowed:
        nav[section] = allowed
st.navigation(nav).run()

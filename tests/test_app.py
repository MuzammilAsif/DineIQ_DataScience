"""Tests for the Streamlit app: login, role gating, and the cached loaders behind each page."""
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "database"))

import auth  # noqa: E402
import data_loader as dl  # noqa: E402
import db  # noqa: E402
import init_db  # noqa: E402


@pytest.fixture()
def tmp_db(tmp_path):
    return init_db.init(tmp_path / "app.db", seed_master=False)


def test_passwords_are_hashed(tmp_db):
    rows = sqlite3.connect(tmp_db).execute("SELECT username, password_hash FROM users").fetchall()
    plain = {u: p for u, p, *_ in init_db.DEMO_USERS}
    assert rows and all(h != plain[u] and h.startswith(("scrypt:", "pbkdf2:")) for u, h in rows)


def test_login_success_sets_session_role(tmp_db):
    session = {}
    assert auth.login(session, "analyst", "Analyst@123", tmp_db)
    assert session["role"] == "Analyst" and session["user"]["username"] == "analyst"
    session = {}
    assert auth.login(session, "manager", "Manager@123", tmp_db)
    assert session["role"] == "Restaurant Manager"
    assert session["user"]["assigned_location_id"] == "LOC0001"


def test_login_wrong_password_fails_and_is_audited(tmp_db):
    session = {}
    assert not auth.login(session, "admin", "wrong", tmp_db)
    assert not auth.login(session, "nobody", "Admin@123", tmp_db)
    assert "role" not in session
    actions = [r["action"] for r in db.recent_audit(10, tmp_db)]
    assert actions[:2] == ["login_failed", "login_failed"]


def _run_page(name, username):
    from streamlit.testing.v1 import AppTest
    u = db.get_user(username)
    at = AppTest.from_file(str(ROOT / "src" / "pages" / name), default_timeout=120)
    at.session_state["user"] = {k: u[k] for k in ("id", "username", "full_name", "role",
                                                  "assigned_location_id")}
    at.session_state["role"] = u["role"]
    return at.run()


@pytest.mark.parametrize("username", ["analyst", "regional", "manager"])
def test_admin_page_stops_non_administrators(username):
    at = _run_page("9_Admin.py", username)
    assert any("does not have access" in e.value for e in at.error)
    assert len(at.tabs) == 0 and not at.exception


def test_admin_page_opens_for_administrator():
    at = _run_page("9_Admin.py", "admin")
    assert not at.error and not at.exception and len(at.tabs) == 5


def test_page_without_login_is_stopped():
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(ROOT / "src" / "pages" / "2_Menu_Dashboard.py")).run()
    assert any("log in" in w.value for w in at.warning) and not at.dataframe


LOADERS = [
    ("Executive", dl.orders, ["order_id", "location_id", "date", "total_amount", "margin"]),
    ("Executive", dl.anomalies, ["anomaly_type", "entity_id", "period_start", "score"]),
    ("Menu", dl.classification, ["item_id", "category", "profit_percentage", "average_rating"]),
    ("Menu", dl.slow_moving, ["item_id", "slow_moving", "demand_percentile"]),
    ("Customer", dl.customer_segments, ["customer_id", "segment", "rfm_score", "churn_risk"]),
    ("Wastage", dl.wastage, ["item_id", "location_id", "date", "cost_of_waste"]),
    ("Wastage", dl.wastage_risk, ["item_id", "date", "predicted_risk", "risk_probability"]),
    ("Forecast", lambda: dl.forecast("location_day"), ["location_id", "target_date", "actual",
                                                        "forecast", "baseline", "recent_average"]),
    ("Dual-Pipeline", dl.dual_comparison, ["record_id", "actual", "spark_result", "python_result",
                                           "spark_python_match", "final_consistency_status"]),
    ("Recommendations", dl.recommendations, ["recommendation_id", "action", "evidence", "priority"]),
    ("Admin", dl.job_runs, ["job", "finished", "duration_s", "status"]),
]


@pytest.mark.parametrize("page,loader,columns", LOADERS, ids=[f"{p}-{i}" for i, (p, *_) in enumerate(LOADERS)])
def test_loaders_return_expected_columns(page, loader, columns):
    df = loader()
    assert len(df) > 0
    assert set(columns) <= set(df.columns)


def test_anomaly_severity_follows_z_score():
    import ui
    assert [ui.severity("item_sales_z", z) for z in (3.1, -4.6, 6.0)] == ["Medium", "High", "Critical"]
    assert ui.severity("high_order_value", 9.0) == "Review"


def test_anomalies_page_renders_cards():
    at = _run_page("10_Anomalies.py", "analyst")
    assert not at.exception and not at.error
    assert any("dq-card" in m.value for m in at.markdown)


def test_ensure_creates_missing_database(tmp_path):
    path = tmp_path / "app.db"
    assert not init_db.is_ready(path)
    assert init_db.ensure(path) is True
    assert init_db.is_ready(path)
    assert auth.login({}, "admin", "Admin@123", path)
    with sqlite3.connect(path) as c:
        assert c.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0] > 0
    assert init_db.ensure(path) is False


def test_ensure_repairs_empty_database_file(tmp_path):
    path = tmp_path / "app.db"
    path.touch()
    assert not init_db.is_ready(path)
    assert init_db.ensure(path) is True
    assert init_db.is_ready(path)

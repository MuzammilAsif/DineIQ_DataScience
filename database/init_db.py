"""Creates database/app.db from schema.sql, seeds one demo user per role, and copies the
Restaurants and Menu_Items master data from processed_data/.

Run: .venv/bin/python database/init_db.py
"""
import csv
import sqlite3
import sys
from pathlib import Path

from werkzeug.security import generate_password_hash

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import db  # noqa: E402

DEMO_USERS = [
    # username, password, role, full name, assigned location
    ("admin", "Admin@123", "Administrator", "System Administrator", None),
    ("regional", "Regional@123", "Regional Manager", "Sindh Regional Manager", None),
    ("manager", "Manager@123", "Restaurant Manager", "Clifton Restaurant Manager", "LOC0001"),
    ("analyst", "Analyst@123", "Analyst", "Data Analyst", None),
]


def _bool(v):
    return 1 if str(v).strip().lower() == "true" else 0


def init(path=None, seed_master=True):
    path = Path(path or db.DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with db.connect(path) as c:
        c.executescript((ROOT / "database" / "schema.sql").read_text())
    for username, pw, role, name, loc in DEMO_USERS:
        if not db.get_user(username, path):
            db.add_user(username, generate_password_hash(pw), role, name, loc, path)
    if seed_master:
        with db.connect(path) as c:
            has = c.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0]
        if not has:
            with open(ROOT / "processed_data" / "Restaurants.csv") as fh:
                for r in csv.DictReader(fh):
                    db.upsert_master("restaurants", {**r, "is_active": 1}, path)
            with open(ROOT / "processed_data" / "Menu_Items.csv") as fh:
                for r in csv.DictReader(fh):
                    db.upsert_master("menu_items", {**r, "is_active": _bool(r["is_active"]),
                                                    "is_seasonal": _bool(r["is_seasonal"])}, path)
    return path


REQUIRED_TABLES = {"users", "audit_log", "restaurants", "menu_items"}


def is_ready(path=None):
    """True if the database exists, has every required table and at least one user."""
    path = Path(path or db.DB_PATH)
    if not path.exists():
        return False
    try:
        with db.connect(path) as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not REQUIRED_TABLES <= tables:
                return False
            return c.execute("SELECT COUNT(*) FROM users").fetchone()[0] > 0
    except sqlite3.DatabaseError:
        return False


def ensure(path=None):
    """Initialises the database if it is missing or incomplete. Returns True if it ran init."""
    if is_ready(path):
        return False
    init(path)
    return True


if __name__ == "__main__":
    p = init()
    print(f"initialised {p}")
    for u in DEMO_USERS:
        print(f"  {u[2]:20s} {u[0]} / {u[1]}")

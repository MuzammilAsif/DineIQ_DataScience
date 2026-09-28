"""SQLite access for users, the audit log and master-data copies."""
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "database" / "app.db"

RESTAURANT_COLS = ["location_id", "name", "city", "region", "address", "location_type",
                   "seating_capacity", "opening_date", "is_active"]
MENU_COLS = ["item_id", "item_name", "category_id", "base_price", "base_cost", "description",
             "is_active", "launch_date", "is_seasonal", "season"]
MASTER = {"restaurants": ("location_id", RESTAURANT_COLS), "menu_items": ("item_id", MENU_COLS)}


def connect(path=None):
    conn = sqlite3.connect(path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_user(username, path=None):
    with connect(path) as c:
        row = c.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    return dict(row) if row else None


def list_users(path=None):
    with connect(path) as c:
        return [dict(r) for r in c.execute(
            "SELECT id, username, role, full_name, assigned_location_id, created_at "
            "FROM users ORDER BY id")]


def add_user(username, password_hash, role, full_name=None, location_id=None, path=None):
    with connect(path) as c:
        c.execute("INSERT INTO users (username, password_hash, role, full_name, "
                  "assigned_location_id) VALUES (?, ?, ?, ?, ?)",
                  (username, password_hash, role, full_name, location_id))


def delete_user(user_id, path=None):
    with connect(path) as c:
        c.execute("DELETE FROM users WHERE id = ?", (user_id,))


def log(user_id, action, details="", path=None):
    with connect(path) as c:
        c.execute("INSERT INTO audit_log (user_id, action, details) VALUES (?, ?, ?)",
                  (user_id, action, details))


def recent_audit(limit=200, path=None):
    with connect(path) as c:
        return [dict(r) for r in c.execute(
            "SELECT a.timestamp, u.username, a.action, a.details FROM audit_log a "
            "LEFT JOIN users u ON u.id = a.user_id ORDER BY a.id DESC LIMIT ?", (limit,))]


def master_rows(table, path=None):
    key, cols = MASTER[table]
    with connect(path) as c:
        return [dict(r) for r in c.execute(f"SELECT {', '.join(cols)} FROM {table} ORDER BY {key}")]


def upsert_master(table, row, path=None):
    key, cols = MASTER[table]
    values = [row.get(c) for c in cols]
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c != key)
    with connect(path) as c:
        c.execute(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
                  f"ON CONFLICT({key}) DO UPDATE SET {updates}, updated_at = CURRENT_TIMESTAMP",
                  values)


def set_active(table, key_value, active, path=None):
    key, _ = MASTER[table]
    with connect(path) as c:
        c.execute(f"UPDATE {table} SET is_active = ?, updated_at = CURRENT_TIMESTAMP "
                  f"WHERE {key} = ?", (int(active), key_value))

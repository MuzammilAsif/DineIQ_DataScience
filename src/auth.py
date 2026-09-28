"""Login, session state and role checks."""
from werkzeug.security import check_password_hash

import db

ROLES = ["Administrator", "Regional Manager", "Restaurant Manager", "Analyst"]
ALL_ROLES = ROLES
MANAGERS_AND_ANALYSTS = ["Administrator", "Regional Manager", "Analyst"]


def authenticate(username, password, path=None):
    """Returns the user row on a correct password, else None. Every attempt is audited."""
    user = db.get_user(username.strip(), path) if username else None
    ok = user is not None and check_password_hash(user["password_hash"], password or "")
    db.log(user["id"] if user else None, "login_success" if ok else "login_failed",
           f"username={username}", path)
    return user if ok else None


def login(session, username, password, path=None):
    user = authenticate(username, password, path)
    if user:
        session["user"] = {k: user[k] for k in ("id", "username", "full_name", "role",
                                                "assigned_location_id")}
        session["role"] = user["role"]
    return user is not None


def logout(session):
    for k in ("user", "role"):
        session.pop(k, None)


def is_allowed(role, allowed):
    return role in allowed


def require(allowed=ALL_ROLES):
    """Top-of-page gate. Stops the page if nobody is logged in or the role is not allowed."""
    import streamlit as st
    role = st.session_state.get("role")
    if role is None:
        st.warning("Please log in on the home page first.")
        st.stop()
    if not is_allowed(role, allowed):
        st.error(f"Your role ({role}) does not have access to this page.")
        st.stop()
    return st.session_state["user"]

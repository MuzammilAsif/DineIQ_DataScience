"""Shared page setup: access check, global filters, KPI tiles, cards, charts, source footers."""
import datetime as dt
import html
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import auth
import data_loader as dl
import db

CLASSES = ["Profit Driver", "Volume Driver", "Hidden Opportunity", "Low Performer",
           "Insufficient History"]
DATA_START, DATA_END = dt.date(2025, 1, 1), dt.date(2025, 12, 31)
RED, INK, GRAY, LIGHT_GRAY = "#C8102E", "#1A1A1A", "#8C8C8C", "#BDBDBD"
# Fixed categorical order, checked for colour-blind separation. Gray is kept for baselines.
PALETTE = [RED, "#2F6FC0", "#C77C00", "#12876F", "#7C4DBF", "#D1478C"]
CLASS_COLORS = dict(zip(CLASSES, PALETTE[:4] + [GRAY]))
PRIORITIES = ["Critical", "High", "Medium", "Low"]
CSS = Path(__file__).with_name("style.css")

px.defaults.color_discrete_sequence = PALETTE
px.defaults.color_continuous_scale = ["#FBE7EA", RED]


def css():
    st.html(f"<style>{CSS.read_text()}</style>")


def page(title, allowed=auth.ALL_ROLES):
    st.set_page_config(page_title=f"DineIQ · {title}", layout="wide")
    css()
    user = auth.require(allowed)
    st.title(title)
    user_panel(user)
    return user, filter_bar(user)


def _persisted(key, default):
    return st.session_state.get(f"flt_{key}", default)


def _keep(key):
    st.session_state[f"flt_{key}"] = st.session_state[f"w_{key}"]


def user_panel(user):
    with st.sidebar:
        st.divider()
        st.markdown(f"**{user['full_name'] or user['username']}**  \n{user['role']}")
        st.caption(f"Data 1 Jan to 31 Dec 2025 · snapshots as of {dl.as_of()}")
        if st.button("Log out"):
            db.log(user["id"], "logout")
            auth.logout(st.session_state)
            st.rerun()


def filter_bar(user):
    with guard("Could not load filter options."):
        locs = dl.locations()[["location_id", "location_name"]].sort_values("location_id")
        names = dict(zip(locs.location_id, locs.location_name))
        cats = sorted(dl.classification().category_name.dropna().unique())
    with st.container(border=True):
        c = st.columns([1.1, 1.5, 1.2, 1.2])
        dates = c[0].date_input("Date range", _persisted("dates", (DATA_START, DATA_END)),
                                min_value=DATA_START, max_value=DATA_END, key="w_dates",
                                on_change=_keep, args=("dates",))
        if user["role"] == "Restaurant Manager" and user.get("assigned_location_id"):
            loc = [user["assigned_location_id"]]
            c[1].text_input("Location", f"{names.get(loc[0], loc[0])} (your restaurant)",
                            disabled=True)
        else:
            loc = c[1].multiselect("Location", list(names), _persisted("loc", []),
                                   format_func=lambda x: f"{x} · {names[x]}", key="w_loc",
                                   on_change=_keep, args=("loc",), placeholder="All locations")
        cat = c[2].multiselect("Menu category", cats, _persisted("cat", []), key="w_cat",
                               on_change=_keep, args=("cat",), placeholder="All categories")
        cls = c[3].multiselect("Performance class", CLASSES, _persisted("cls", []), key="w_cls",
                               on_change=_keep, args=("cls",), placeholder="All classes")
    start, end = (dates if isinstance(dates, (tuple, list)) and len(dates) == 2
                  else (DATA_START, DATA_END))
    return dict(start=pd.Timestamp(start), end=pd.Timestamp(end), locations=loc,
                categories=cat, classes=cls, location_names=names)


def filter_df(df, f, date_col=None, loc_col=None, cat_col=None, cls_col=None):
    if date_col:
        df = df[(df[date_col] >= f["start"]) & (df[date_col] <= f["end"])]
    if loc_col and f["locations"]:
        df = df[df[loc_col].isin(f["locations"])]
    if cat_col and f["categories"]:
        df = df[df[cat_col].isin(f["categories"])]
    if cls_col and f["classes"]:
        df = df[df[cls_col].isin(f["classes"])]
    return df


def applied(f, used):
    """Caption listing which of the global filters this section applies."""
    parts = []
    if "date" in used:
        parts.append(f"{f['start']:%d %b %Y} to {f['end']:%d %b %Y}")
    if "location" in used:
        parts.append(f"{len(f['locations'])} location(s)" if f["locations"] else "all locations")
    if "category" in used:
        parts.append(", ".join(f["categories"]) if f["categories"] else "all categories")
    if "class" in used:
        parts.append(", ".join(f["classes"]) if f["classes"] else "all classes")
    st.caption("Filters applied: " + "; ".join(parts) if parts else "Global filters do not apply here.")


def chart(fig, container=None, source_path=None, **kw):
    """Plotly figure in a white card: light grid, visible legend, exact values on hover."""
    fig.update_layout(template="plotly_white", plot_bgcolor="#FFFFFF", paper_bgcolor="#FFFFFF",
                      colorway=PALETTE, font=dict(family="sans-serif", color=INK, size=13),
                      margin=dict(l=10, r=10, t=36, b=10), hoverlabel=dict(bgcolor="#FFFFFF"),
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text=""),
                      separators=".,")
    fig.update_xaxes(gridcolor="#EEEEEE", linecolor="#D0D0D0", zeroline=False)
    fig.update_yaxes(gridcolor="#EEEEEE", linecolor="#D0D0D0", zeroline=False)
    kw.setdefault("width", "stretch")
    box = (container or st).container(border=True)
    box.plotly_chart(fig, theme=None, **kw)
    if source_path:
        source(source_path, container=container)


def hbar(df, x, y, x_title, color=RED, top=None, fmt=",.0f"):
    """Horizontal ranking bar, largest at the top."""
    d = df.sort_values(x, ascending=False).head(top) if top else df.sort_values(x, ascending=False)
    fig = px.bar(d, x=x, y=y, orientation="h", labels={x: x_title, y: ""})
    fig.update_traces(marker_color=color, hovertemplate=f"%{{y}}<br>{x_title}: %{{x:{fmt}}}<extra></extra>")
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(height=max(260, 28 * len(d) + 80), showlegend=False)
    return fig


def kpis(tiles):
    """Row of KPI tiles. Each tile: (label, value[, delta[, tone]]), tone in muted/pos/neg/accent."""
    cols = st.columns(len(tiles))
    for col, t in zip(cols, tiles):
        label, value, delta, tone = list(t) + [None] * (4 - len(t))
        tone = tone or "muted"
        accent = " accent" if tone == "accent" else ""
        d = (f'<div class="dq-kpi-delta {tone}">{html.escape(str(delta))}</div>'
             if delta is not None else '<div class="dq-kpi-delta">&nbsp;</div>')
        col.markdown(f'<div class="dq-kpi"><div class="dq-kpi-label">{html.escape(label)}</div>'
                     f'<div class="dq-kpi-value{accent}">{html.escape(str(value))}</div>{d}</div>',
                     unsafe_allow_html=True)


def signed(v, text):
    """Delta tone from the sign: green up, red down."""
    return text, "pos" if v > 0 else "neg" if v < 0 else "muted"


def badge(level):
    css_class = level.lower() if level in PRIORITIES else "neutral"
    return f'<span class="dq-badge {css_class}">{html.escape(level)}</span>'


def card(level, title, summary="", evidence=(), meta="", container=None):
    ev = "".join(f"<li>{html.escape(str(e))}</li>" for e in evidence)
    crit = " critical" if level == "Critical" else ""
    (container or st).markdown(
        f'<div class="dq-card{crit}"><div class="dq-card-head">{badge(level)}'
        f'<span class="dq-card-meta">{html.escape(meta)}</span></div>'
        f'<div class="dq-card-title">{html.escape(title)}</div>'
        f'<div class="dq-card-summary">{html.escape(summary)}</div>'
        + (f"<ul>{ev}</ul>" if ev else "") + "</div>", unsafe_allow_html=True)


@st.cache_data(show_spinner=False, ttl=300)
def _generated(rel):
    p = dl.ROOT / rel
    files = [p] if p.is_file() else [f for f in p.rglob("*") if f.is_file() and not f.name.startswith(".")]
    return max(f.stat().st_mtime for f in files) if files else None


def source(rel, as_of=None, container=None):
    """Muted footer naming the file a section is read from and when that file was written."""
    ts = _generated(rel)
    parts = [f"Source: {rel}"]
    if as_of:
        parts.append(f"data as of {as_of}")
    if ts:
        parts.append(f"generated {dt.datetime.fromtimestamp(ts):%Y-%m-%d %H:%M}")
    (container or st).markdown(f'<div class="dq-source">{html.escape(" · ".join(parts))}</div>',
                               unsafe_allow_html=True)


def takeaway(text):
    st.info(text)


def download_df(df, name, label="Download CSV"):
    user = st.session_state.get("user", {})
    st.download_button(label, df.to_csv(index=False).encode(), file_name=f"{name}.csv",
                       mime="text/csv", key=f"dl_{name}",
                       on_click=db.log, args=(user.get("id"), "export_csv", name))


@contextmanager
def guard(message):
    """Shows a plain error instead of a stack trace and stops the page."""
    try:
        yield
    except Exception as e:  # noqa: BLE001
        st.error(f"{message} ({type(e).__name__})")
        st.stop()


def money(v):
    return f"PKR {v:,.0f}"


def money_short(v):
    """For KPI tiles, where long numbers get cut off."""
    for div, unit in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(v) >= div:
            return f"PKR {v / div:,.1f}{unit}"
    return f"PKR {v:,.0f}"


def pct(v):
    return f"{v:.1%}"


ANOMALY_LABELS = {"item_sales_z": "Item sales", "location_sales_z": "Location sales",
                  "rating_z": "Ratings", "high_order_value": "High-value order",
                  "identical_ratings": "Identical ratings"}
Z_TYPES = {"item_sales_z", "location_sales_z", "rating_z"}
SEVERITY_RULE = ("Severity applies to z-score anomalies (all flagged at |z| of 3 or more): "
                 "Critical at |z| 6+, High at 4.5+, Medium below that. High-value orders and "
                 "identical-rating weeks have no z-score and are marked Review.")


def severity(anomaly_type, score):
    if anomaly_type not in Z_TYPES:
        return "Review"
    z = abs(score)
    return "Critical" if z >= 6 else "High" if z >= 4.5 else "Medium"


@st.cache_data(show_spinner=False)
def _item_names():
    return dl.classification().set_index("item_id").item_name.to_dict()


def anomaly_card(r, location_names, container=None):
    names = {**_item_names(), **location_names}
    who = names.get(r.entity_id, r.entity_id)
    where = f" at {location_names.get(r.location_id, r.location_id)}" if isinstance(r.location_id, str) \
        and r.location_id != r.entity_id else ""
    metric = r.metric.replace("_", " ")
    if r.anomaly_type in Z_TYPES:
        direction = "above" if r.score > 0 else "below"
        title = f"{ANOMALY_LABELS[r.anomaly_type]} {direction} normal: {who}{where}"
        evidence = [f"{metric}: {r.value:,.2f} against a baseline of {r.baseline_mean:,.2f} "
                    f"(std {r.baseline_std:,.2f})", f"z-score {r.score:+.1f}"]
    elif r.anomaly_type == "high_order_value":
        title = f"High-value order {r.entity_id}{where}"
        evidence = [f"Order total PKR {r.value:,.0f}", f"{r.score:.2f}x the IQR outlier cutoff"]
    else:
        title = f"Identical ratings: {who}"
        evidence = [f"{r.value:.0%} of the week's ratings share one value ({metric})",
                    f"{r.score:.0f} ratings that week"]
    card(severity(r.anomaly_type, r.score), title, f"{r.period_start:%d %b %Y}", evidence,
         ANOMALY_LABELS.get(r.anomaly_type, r.anomaly_type), container)

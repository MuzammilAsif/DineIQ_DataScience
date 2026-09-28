"""Core-formula tests for the Step 6 analytics jobs (spark_jobs/07-14)."""
import datetime as dt
import importlib.util
import random
from pathlib import Path

import pytest

JOBS = Path(__file__).resolve().parent.parent / "spark_jobs"


def load(name):
    spec = importlib.util.spec_from_file_location(name.split("_", 1)[1], JOBS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


seg = load("07_customer_segmentation")
mb = load("08_market_basket")
wa = load("09_wastage_analysis")
pi = load("10_price_intelligence")
pa = load("11_promotion_analysis")
an = load("12_anomaly_detection")
sm = load("13_slow_moving_and_location")
cc = load("14_channel_and_churn")
import analytics_common as ac  # noqa: E402

CFG = ac.load_config()
D = dt.date


# --- 07 segmentation ---

ARCHETYPES = {
    # recency, frequency, monetary, aov, promo share, tenure
    "loyal": (10, 35, 70000, 2000, 0.1, 300),
    "frequent": (15, 16, 15000, 900, 0.1, 300),
    "promo": (30, 7, 8000, 1100, 0.8, 250),
    "at_risk": (200, 3, 4000, 1400, 0.1, 300),
    "new": (15, 1, 1500, 1000, 0.1, 40),
    "occasional": (60, 2, 3000, 1300, 0.1, 250),
}
AS_OF = D(2025, 12, 31)


def customer_frames(spark):
    rng = random.Random(1)
    feats, orders = [], []

    def add(cid, rec, freq, mon, aov, promo, tenure):
        feats.append((cid, int(rec), int(freq), float(mon), float(aov)))
        for i in range(10):
            orders.append((cid, dt.datetime.combine(AS_OF - dt.timedelta(days=tenure - i), dt.time(12)),
                           "P" if i < round(promo * 10) else None))

    for name, (rec, freq, mon, aov, promo, tenure) in ARCHETYPES.items():
        for i in range(30):
            j = lambda v, s=0.1: v * rng.uniform(1 - s, 1 + s)  # noqa: E731
            add(f"{name}{i}", j(rec), max(1, round(j(freq))), j(mon), j(aov), promo, int(j(tenure)))
    add("TARGET", 3, 40, 85000, 2100, 0.05, 330)
    cf = spark.createDataFrame(feats, "customer_id string, customer_recency int, "
                               "customer_frequency long, customer_monetary_value double, "
                               "average_order_value double")
    od = spark.createDataFrame(orders, "customer_id string, order_datetime timestamp, "
                               "promotion_id string")
    return cf, od


def test_high_value_customer_lands_in_high_value_cluster(spark):
    cf, od = customer_frames(spark)
    df, _, profiles = seg.segment(cf, od, AS_OF, CFG["segmentation"])
    labels = {r.customer_id: r.segment for r in df.select("customer_id", "segment").collect()}
    assert labels["TARGET"] == "High-Value Loyal"
    assert labels["loyal0"] == "High-Value Loyal"
    assert labels["at_risk0"] == "At-Risk"
    assert labels["new0"] == "New"
    assert labels["promo0"] == "Promotion-Driven"


def test_rfm_quintiles(spark):
    rows = [(f"C{i}", 100 - 10 * i, i + 1, 100.0 * (i + 1)) for i in range(10)]
    df = spark.createDataFrame(rows, "customer_id string, customer_recency int, "
                               "customer_frequency long, customer_monetary_value double")
    out = {r.customer_id: r for r in seg.rfm_scores(df).collect()}
    # C9: most recent, most frequent, highest spend
    assert (out["C9"].r_score, out["C9"].f_score, out["C9"].m_score) == (5, 5, 5)
    assert (out["C0"].r_score, out["C0"].f_score, out["C0"].m_score) == (1, 1, 1)
    assert out["C9"].rfm_code == "555" and out["C9"].rfm_score == 15


# --- 08 market basket ---

def test_fpgrowth_rule_metrics(spark):
    baskets = [["A", "B"]] * 4 + [["A"]] * 2 + [["B", "C"]] * 2 + [["C"]] * 2
    df = spark.createDataFrame([(str(i), b) for i, b in enumerate(baskets)], "order_id string, items array<string>")
    _, rules = mb.mine(df, {"min_support": 0.2, "min_confidence": 0.5})
    r = {(tuple(x.antecedent), tuple(x.consequent)): x for x in rules.collect()}[(("A",), ("B",))]
    # P(A)=0.6, P(B)=0.6, P(A,B)=0.4
    assert r.support == pytest.approx(0.4)
    assert r.confidence == pytest.approx(0.4 / 0.6)
    assert r.lift == pytest.approx((0.4 / 0.6) / 0.6)


# --- 09 wastage risk ---

def test_wastage_features_exclude_leakage():
    assert not set(wa.FEATURES) & set(wa.LEAKAGE_COLUMNS)
    (assembler,) = [s for s in wa.pipeline(CFG["wastage"]).getStages()
                    if type(s).__name__ == "VectorAssembler"]
    assert assembler.getInputCols() == wa.FEATURES
    assert not set(assembler.getInputCols()) & set(wa.LEAKAGE_COLUMNS)


def test_recent_demand_uses_prior_days_only(spark):
    inv = spark.createDataFrame(
        [("I1", "L1", D(2025, 1, d), 40.0, float(c)) for d, c in [(1, 10), (2, 20), (3, 30)]],
        "item_id string, location_id string, date date, prepared_quantity double, consumed_stock double")
    waste = spark.createDataFrame([("I1", "L1", D(2025, 1, 3), 8.0)],
                                  "item_id string, location_id string, date date, quantity_wasted double")
    cov = spark.createDataFrame([], "item_id string, location_id string, date date")
    menu = spark.createDataFrame([("I1", "x", "Mains")], "item_id string, item_name string, category_name string")
    pop = spark.createDataFrame([("I1", 0.5)], "item_id string, item_popularity double")
    out = {r.date.day: r for r in
           wa.risk_frame(inv, waste, cov, menu, pop, CFG["wastage"]).collect()}
    assert out[1].recent_demand == -1.0
    assert out[3].recent_demand == pytest.approx(15.0)
    assert out[3].prep_to_demand == pytest.approx(40 / 15)
    assert out[3].wastage_percentage == pytest.approx(0.2)
    assert out[3].label == float(0.2 > CFG["wastage"]["high_risk_above"])
    assert out[2].label == 0.0


# --- 10 price ---

def test_elasticity_before_after_with_category_control(spark):
    changes = spark.createDataFrame([("I1", D(2025, 6, 1), 100.0, 120.0)],
                                    "item_id string, change_date date, old_price double, new_price double")
    days = [D(2025, 6, 1) + dt.timedelta(days=o) for o in range(-30, 30)]
    after = lambda d: d >= D(2025, 6, 1)  # noqa: E731
    # item 10 -> 9 a day; the rest of its category 20 -> 25 (+25% seasonal lift)
    rows = [("I1", "Mains", d, 9 if after(d) else 10) for d in days]
    rows += [("I2", "Mains", d, 25 if after(d) else 20) for d in days]
    daily = spark.createDataFrame(rows, "item_id string, category_name string, date date, qty long")
    cfg = dict(CFG["price"], window_days=30)
    r = [x for x in pi.event_impact(changes, daily, D(2025, 1, 1), D(2025, 12, 31), cfg).collect()
         if x.item_id == "I1"][0]
    assert r.evaluable
    assert r.demand_change_pct == pytest.approx(-0.1)
    assert r.control_change_pct == pytest.approx(0.25)
    assert r.adjusted_demand_change_pct == pytest.approx(0.9 / 1.25 - 1)
    assert r.elasticity == pytest.approx((0.9 / 1.25 - 1) / 0.2)
    edge = pi.event_impact(changes, daily, D(2025, 5, 15), D(2025, 12, 31), cfg).first()
    assert not edge.evaluable and edge.elasticity is None
    small = changes.withColumn("new_price", changes.old_price * (1 + cfg["min_price_change"] / 2))
    assert not pi.event_impact(small, daily, D(2025, 1, 1), D(2025, 12, 31), cfg).first().evaluable



def test_price_sensitive_item_classes(spark):
    """Most negative elasticity is Highly sensitive, positive is Inconclusive, no change is Not Evaluated."""
    rows = [(f"I{i}", 0.2, -0.2 * i, True) for i in range(1, 10)]
    rows += [("UP", 0.2, 0.5, True), ("SMALL", 0.02, None, False)]
    events = spark.createDataFrame(rows, "item_id string, price_change_pct double, "
                                   "elasticity double, evaluable boolean")
    items = spark.createDataFrame([(r[0],) for r in rows] + [("NONE",)], "item_id string")
    out = {r.item_id: r.price_sensitivity
           for r in pi.classify_items(events, items, CFG["price"]).collect()}
    assert out["I9"] == "Highly Price Sensitive"
    assert out["I1"] == "Low Price Sensitivity"
    assert out["I5"] == "Moderately Price Sensitive"
    assert out["UP"] == "Inconclusive"
    assert out["SMALL"] == out["NONE"] == "Not Evaluated"


# --- 11 promotions ---

def test_promotion_trap_flags(spark):
    cols = ["orders", "quantity", "revenue", "margin", "customers", "wastage_cost",
            "revenue_per_order", "margin_per_order", "margin_pct"]
    pre = dict(orders=100, quantity=200, revenue=10000.0, margin=5000.0, customers=80,
               wastage_cost=100.0)
    during = dict(orders=150, quantity=320, revenue=12000.0, margin=4500.0, customers=120,
                  wastage_cost=300.0)
    post = dict(orders=80, quantity=150, revenue=8000.0, margin=4000.0, customers=60,
                wastage_cost=100.0)
    rows = []
    for period, m in [("pre", pre), ("during", during), ("post", post)]:
        m = dict(m, revenue_per_order=m["revenue"] / m["orders"],
                 margin_per_order=m["margin"] / m["orders"], margin_pct=m["margin"] / m["revenue"])
        rows.append(("P1", period, *[float(m[c]) for c in cols]))
    metrics = spark.createDataFrame(rows, "promotion_id string, period string, "
                                    + ", ".join(f"{c} double" for c in cols))
    promos = spark.createDataFrame([("P1", "x", "BOGO", D(2025, 8, 1), D(2025, 8, 31))],
                                   "promotion_id string, promotion_name string, "
                                   "promotion_type string, start_date date, end_date date")
    r = pa.compare(metrics, promos, D(2025, 1, 1), D(2025, 12, 31), CFG["promotion"]).first()
    assert r.volume_up_margin_down and r.customers_up_margin_per_order_down
    assert r.wastage_up and r.post_promo_drop
    assert r.trap_flags == 4 and r.is_promotion_trap
    early = pa.compare(metrics, promos, D(2025, 7, 15), D(2025, 12, 31), CFG["promotion"]).first()
    assert early.volume_up_margin_down is None


# --- 12 anomalies ---

def test_rolling_z_flags_spike_only(spark):
    vals = [10, 12, 11, 9, 10, 11, 12, 10, 60, 11]
    df = spark.createDataFrame([("L1", D(2025, 1, 1) + dt.timedelta(days=i), i, float(v))
                                for i, v in enumerate(vals)],
                               "location_id string, period_start date, d int, daily_orders double")
    out = an.rolling_z(df, ["location_id"], "d", ["daily_orders"], -7, -1, 4, 3.0, "t").collect()
    assert [r.period_start.day for r in out] == [9]
    assert out[0].score > 3



def test_rating_anomaly_flags_rating_drop_and_identical_week(spark):
    avg = [4.0, 4.1, 3.9, 4.0, 4.1, 3.9, 4.0, 1.5]
    rows = [("I1", D(2025, 1, 6) + dt.timedelta(weeks=i), 12, a, 0.4, 4, i) for i, a in enumerate(avg)]
    rows.append(("I2", D(2025, 1, 6), 12, 4.0, 0.9, 4, 0))
    weeks = spark.createDataFrame(rows, "item_id string, period_start date, rating_count long, "
                                  "avg_rating double, identical_share double, top_value int, "
                                  "week_num int")
    out = an.rating_anomalies(weeks, CFG["anomaly"]).collect()
    drops = [r for r in out if r.anomaly_type == "rating_z" and r.metric == "avg_rating"]
    assert [(r.entity_id, r.period_start) for r in drops] == [("I1", D(2025, 2, 24))]
    assert drops[0].score < -3
    same = [r for r in out if r.anomaly_type == "identical_ratings"]
    assert [(r.entity_id, r.metric) for r in same] == [("I2", "share_of_rating_4")]


# --- 13 slow-moving ---

def test_slow_moving_rule(spark):
    rows = [("S", 0.1, 0.01, -2.0, 120, 365), ("UP", 0.1, 0.01, 5.0, 120, 365),
            ("POP", 0.9, 0.01, -2.0, 1200, 365), ("NEW", None, 0.01, -2.0, 50, 30)]
    rows += [(f"F{i}", 0.2 + 0.07 * i, 0.05 + 0.01 * i, 0.0, 500, 365) for i in range(10)]
    df = spark.createDataFrame(rows, "item_id string, demand_percentile double, "
                               "repeat_purchase_rate double, sales_trend double, "
                               "total_quantity_sold long, days_since_launch int")
    out = {r.item_id: r.slow_moving for r in sm.slow_moving(df, CFG["slow_moving"]).collect()}
    assert out["S"] and not out["UP"] and not out["POP"] and not out["NEW"]


# --- 14 churn ---

def test_churn_rule(spark):
    def ts(days_ago):
        return dt.datetime.combine(AS_OF - dt.timedelta(days=days_ago), dt.time(12))
    orders = spark.createDataFrame(
        [("A", ts(d)) for d in (170, 150, 130, 100)]      # declining, lapsed 100 days
        + [("B", ts(d)) for d in (170, 140, 60, 30, 10)]  # active
        + [("C", ts(d)) for d in (300, 280)],             # lapsed before both quarters
        "customer_id string, order_datetime timestamp")
    cf = spark.createDataFrame([("A", 100, 4, 1.0), ("B", 10, 5, 1.0), ("C", 280, 2, 1.0)],
                               "customer_id string, customer_recency int, customer_frequency long, "
                               "customer_monetary_value double")
    df, gap, threshold = cc.churn_risk(orders, cf, AS_OF, {"gap_multiplier": 1.0})
    assert gap == pytest.approx(((170 - 100) / 3 + (170 - 10) / 4 + 20 / 1) / 3)
    out = {r.customer_id: r.churn_risk for r in df.collect()}
    assert out == {"A": True, "B": False, "C": False}


# --- shared report ---

def test_write_section_replaces_in_place(tmp_path):
    p = tmp_path / "r.md"
    ac.write_section("09", "## B\nFIRST_VERSION", p)
    ac.write_section("07", "## A", p)
    ac.write_section("09", "## B\nSECOND_VERSION", p)
    text = p.read_text()
    assert text.count("## B") == 1 and "SECOND_VERSION" in text and "FIRST_VERSION" not in text
    assert text.index("## A") < text.index("## B")

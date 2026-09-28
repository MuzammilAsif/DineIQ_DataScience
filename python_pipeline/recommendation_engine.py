"""Evidence-based recommendations built from the outputs of Steps 3-8 (SRS Steps 37-39).

Each recommendation type reads one already-computed table, and every evidence line carries values
from it. Priority is not set per type: every recommendation gets an estimated_impact_value, all
of them are ranked together, and the rank percentile decides the bucket (config:
recommendations.priority).

Output: parquet_data/recommendations/, reports/recommendations_report.md
Run: .venv/bin/python -m python_pipeline.recommendation_engine
"""
import pandas as pd

from python_pipeline import segment_rules as sr

ROOT = sr.CONFIG_PATH.parent.parent
PQ = ROOT / "parquet_data"
OUT_DIR = PQ / "recommendations"
REPORT = ROOT / "reports" / "recommendations_report.md"
PRIORITIES = ["Critical", "High", "Medium", "Low"]

CAMPAIGNS = {
    "High-Value Loyal": "Reward the High-Value Loyal segment with a loyalty tier or early access, "
                        "not blanket discounts",
    "Frequent": "Upsell the Frequent segment to higher-margin add-ons and combos",
    "Promotion-Driven": "Target the Promotion-Driven segment with offers on high-margin items only",
    "At-Risk": "Re-engage the At-Risk segment with a time-limited win-back offer",
    "New": "Convert the New segment with a second-order incentive",
    "Occasional": "Reactivate the Occasional segment with reminders around weekends and events",
}


def money(v):
    return f"PKR {v:,.0f}"


def _latest(path):
    return max(path.glob("as_of_date=*")).name.split("=", 1)[1]


def load_sources():
    as_of = _latest(PQ / "menu_classification")
    return dict(
        as_of=as_of,
        classification=pd.read_parquet(PQ / "menu_classification" / f"as_of_date={as_of}"),
        item_features=pd.read_parquet(PQ / "features" / "item_features" / f"as_of_date={as_of}"),
        wastage_risk=pd.read_parquet(PQ / "wastage_risk", columns=["item_id", "predicted_risk"]),
        price=pd.read_parquet(PQ / "price_sensitivity" / "items"),
        rules=pd.read_parquet(PQ / "market_basket" / "rules"),
        forecast=pd.read_parquet(PQ / "demand_forecast" / "item_day"),
        segments=pd.read_parquet(PQ / "customer_segments"),
        churn=pd.read_parquet(PQ / "churn_risk", columns=["customer_id", "churn_risk"]),
        promotions=pd.read_parquet(PQ / "promotion_effectiveness"),
        anomalies=pd.read_parquet(PQ / "anomalies"),
        locations=pd.read_parquet(PQ / "features" / "location_features" / f"as_of_date={as_of}",
                                  columns=["location_id", "location_name"]),
    )


def rec(type_, target_type, target_id, name, action, evidence, impact, basis):
    return dict(type=type_, target_type=target_type, target_id=target_id, target_name=name,
                action=action, evidence=evidence, estimated_impact_value=float(impact),
                impact_basis=basis)


def hidden_opportunity(cls):
    d = cls[(cls.category == "Hidden Opportunity") & (cls.profitability_tier == "High")]
    return [rec("Promote Hidden Opportunity", "item", r.item_id, r.item_name,
                f"Promote Item {r.item_id} ({r.item_name})",
                [f"High contribution margin ({money(r.contribution_margin)}, "
                 f"{r.profit_percentage:.1f}% of revenue, profitability percentile "
                 f"{r.profitability_percentile:.2f})",
                 f"{r.average_rating:.2f} average rating",
                 f"{'Low' if r.wastage_tier != 'High' else 'High'} wastage ({r.wastage_percentage:.1%})",
                 f"Low current order volume ({r.total_quantity_sold:,} units, demand percentile "
                 f"{r.demand_percentile:.2f})",
                 f"Repeat purchase among existing buyers: {r.repeat_purchase_rate:.1%}"],
                r.contribution_margin, "current contribution margin")
            for r in d.itertuples()]


def high_wastage(cls, feats, risk, cfg):
    share = risk.groupby("item_id").predicted_risk.mean().rename("risk_share")
    d = cls.merge(feats[["item_id", "wastage_cost"]], on="item_id").merge(
        share, on="item_id", how="left")
    d = d[d.excessive_wastage | (d.risk_share >= cfg["wastage_risk_share_min"])]
    out = []
    for r in d.itertuples():
        ev = [f"Wastage cost {money(r.wastage_cost)} over the period",
              f"Wastage {r.wastage_percentage:.1%} of portions (wastage percentile "
              f"{r.wastage_percentile:.2f})",
              f"Demand: {r.total_quantity_sold:,} units sold (demand percentile "
              f"{r.demand_percentile:.2f})"]
        if pd.notna(r.risk_share):
            ev.append(f"Predicted high wastage risk on {r.risk_share:.0%} of its item-location "
                      "forecast days (Step 6 model)")
        if r.excessive_wastage:
            ev.append("Step 4 excessive_wastage flag raised")
        out.append(rec("Reduce prep of high-wastage dish", "item", r.item_id, r.item_name,
                       f"Reduce preparation quantity of {r.item_name} ({r.item_id})", ev,
                       r.wastage_cost, "wastage cost"))
    return out


def price_sensitive(price, cls):
    d = price[price.price_sensitivity == "Highly Price Sensitive"].merge(
        cls[["item_id", "item_revenue"]], on="item_id")
    out = []
    for r in d.itertuples():
        rose = r.price_change_pct > 0
        action = (f"Review the {r.change_date} price increase on {r.item_name} ({r.item_id}); "
                  "hold further increases or test a partial rollback" if rose else
                  f"Keep the lower price on {r.item_name} ({r.item_id}); demand responded to the "
                  f"{r.change_date} cut")
        out.append(rec("Review pricing of price-sensitive dish", "item", r.item_id, r.item_name,
                       action,
                       [f"Price changed {money(r.old_price)} to {money(r.new_price)} "
                        f"({r.price_change_pct:+.1%}) on {r.change_date}",
                        f"Daily demand {r.demand_before:.1f} to {r.demand_after:.1f} units "
                        f"({r.demand_change_pct:+.1%}; rest of category {r.control_change_pct:+.1%})",
                        f"Category-adjusted elasticity {r.elasticity:+.2f}",
                        f"Current item revenue {money(r.item_revenue)}"],
                       r.item_revenue, "current item revenue"))
    return out


def bundles(rules, cls, min_lift):
    names = cls.set_index("item_id").item_name.to_dict()
    revenue = cls.set_index("item_id").item_revenue.to_dict()
    d = rules[rules.lift >= min_lift].copy()
    d["items"] = [tuple(sorted(set(a) | set(c))) for a, c in zip(d.antecedent, d.consequent)]
    d = d.sort_values("confidence", ascending=False).drop_duplicates("items")
    out = []
    for r in d.itertuples():
        combined = sum(revenue.get(i, 0.0) for i in r.items)
        label = " + ".join(names.get(i, i) for i in r.items)
        lhs = " + ".join(names.get(i, i) for i in r.antecedent)
        rhs = " + ".join(names.get(i, i) for i in r.consequent)
        out.append(rec("Bundle frequently bought items", "item_set", "+".join(r.items), label,
                       f"Create a bundle: {label}",
                       [f"Bought together {r.lift:.2f}x as often as chance (lift)",
                        f"{r.confidence:.0%} of orders with {lhs} also include {rhs}",
                        f"Together in {r.support:.1%} of orders",
                        f"Combined item revenue {money(combined)}"],
                       (r.lift - 1) * combined, "(lift - 1) x combined item revenue"))
    return out


def low_performers(cls, feats, cfg):
    d = cls[cls.category == "Low Performer"].merge(feats[["item_id", "wastage_cost"]], on="item_id")
    out = []
    for r in d.itertuples():
        cats = dict(r.location_categories) if r.location_categories is not None else {}
        if not cats:
            continue
        low = sum(v == "Low Performer" for v in cats.values())
        if low / len(cats) < cfg["persistent_location_share"]:
            continue
        impact = r.wastage_cost + (abs(r.contribution_margin) if r.contribution_margin < 0 else 0)
        out.append(rec("Remove or redesign Low Performer", "item", r.item_id, r.item_name,
                       f"Remove or redesign {r.item_name} ({r.item_id})",
                       [f"Low Performer at {low} of {len(cats)} locations",
                        f"Demand percentile {r.demand_percentile:.2f} "
                        f"({r.total_quantity_sold:,} units)",
                        f"Profitability percentile {r.profitability_percentile:.2f} "
                        f"({r.profit_percentage:.1f}% margin, {money(r.contribution_margin)})",
                        f"{r.average_rating:.2f} average rating",
                        f"Wastage cost {money(r.wastage_cost)}"],
                       impact, "wastage cost (+ |margin| if loss-making)"))
    return out


def stock_before_peak(forecast, cls, cfg):
    unit_margin = (cls.contribution_margin / cls.total_quantity_sold).where(cls.total_quantity_sold > 0)
    info = cls.assign(unit_margin=unit_margin).set_index("item_id")
    f = forecast.assign(extra=forecast.forecast - forecast.recent_average)
    peaks = f[(f.forecast >= f.recent_average * (1 + cfg["peak_increase_min"]))
              & (f.extra >= cfg["peak_min_extra_units"])]
    out = []
    for item, g in peaks.groupby("item_id"):
        if item not in info.index or pd.isna(info.loc[item, "unit_margin"]):
            continue
        top = g.loc[g.extra.idxmax()]
        i = info.loc[item]
        out.append(rec("Increase stock before predicted peak", "item", item, i.item_name,
                       f"Increase stock of {i.item_name} ({item}) before {top.target_date}",
                       [f"Forecast {top.forecast:,.0f} units on {top.target_date} against a recent "
                        f"daily level of {top.recent_average:,.0f} ({top.extra / top.recent_average:+.0%})",
                        f"{len(g)} of {forecast[forecast.item_id == item].target_date.nunique()} "
                        "forecast days are peaks",
                        f"Contribution margin {money(i.unit_margin)} per unit"],
                       top.extra * i.unit_margin,
                       "forecast extra units x contribution margin per unit"))
    return out


def segment_campaigns(segments, churn):
    s = segments.merge(churn, on="customer_id", how="left")
    out = []
    for seg, g in s.groupby("segment"):
        out.append(rec("Target customer segment", "segment", seg, seg, CAMPAIGNS.get(seg, seg),
                       [f"{len(g):,} customers, total spend {money(g.customer_monetary_value.sum())}",
                        f"Average recency {g.customer_recency.mean():.0f} days, "
                        f"{g.customer_frequency.mean():.1f} orders per customer",
                        f"Average order value {money(g.average_order_value.mean())}",
                        f"Promotion order share {g.promo_order_share.mean():.0%}",
                        f"{g.churn_risk.fillna(False).mean():.1%} flagged churn risk"],
                       g.customer_monetary_value.sum(), "segment total spend"))
    return out


def ineffective_promotions(promos):
    flags = ["volume_up_margin_down", "customers_up_margin_per_order_down", "wastage_up",
             "post_promo_drop"]
    d = promos[promos.trap_flags > 0]
    out = []
    for r in d.itertuples():
        lost = max(r.pre_margin - r.during_margin,
                   (r.pre_margin_per_order - r.during_margin_per_order) * r.during_orders, 0.0)
        waste = max(r.during_wastage_cost - r.pre_wastage_cost, 0.0)
        raised = [f for f in flags if getattr(r, f) is True]
        ev = [f"Trap flags: {', '.join(raised)}",
              f"Orders {r.pre_orders:,.0f} to {r.during_orders:,.0f}; margin rate "
              f"{r.pre_margin_pct:.1%} to {r.during_margin_pct:.1%}",
              f"Margin per order {money(r.pre_margin_per_order)} to {money(r.during_margin_per_order)}",
              f"Wastage cost {money(r.pre_wastage_cost)} to {money(r.during_wastage_cost)}"]
        if pd.notna(r.post_quantity):
            ev.append(f"Post-promotion quantity {r.post_quantity / r.pre_quantity - 1:+.0%} vs before")
        out.append(rec("Review ineffective promotion", "promotion", r.promotion_id,
                       r.promotion_name,
                       f"Review promotion {r.promotion_id} ({r.promotion_name}) before running it again",
                       ev, lost + waste, "margin given up + extra wastage in the promotion window"))
    return out


def anomalous_locations(anomalies, locations, cfg, z):
    loc = anomalies[anomalies.anomaly_type == "location_sales_z"]
    per_date = loc.groupby("period_start").location_id.nunique()
    specific = loc[loc.period_start.map(per_date) < cfg["chain_event_min_locations"]]
    names = locations.set_index("location_id").location_name.to_dict()
    out = []
    for lid, g in specific.groupby("location_id"):
        if len(g) < cfg["location_anomaly_min_flags"]:
            continue
        top = g.loc[g.score.abs().idxmax()]
        rev = g[g.metric == "daily_revenue"]
        deviation = (rev.value - rev.baseline_mean).abs().sum()
        out.append(rec("Investigate anomalous location", "location", lid, names.get(lid, lid),
                       f"Investigate sales anomalies at {names.get(lid, lid)} ({lid})",
                       [f"{len(g)} location-specific sales anomalies (|z| > {z}) on "
                        f"{g.period_start.nunique()} days, out of {len(loc[loc.location_id == lid])} "
                        "flags in total",
                        f"Largest: {top.metric} {top.value:,.0f} vs baseline {top.baseline_mean:,.0f} "
                        f"on {top.period_start} (z {top.score:+.1f})",
                        f"Total daily-revenue deviation from baseline {money(deviation)}",
                        f"{(g.score > 0).sum()} spikes, {(g.score < 0).sum()} drops"],
                       deviation, "total |revenue - baseline| on flagged days"))
    return out


def assign_priority(impacts, cfg):
    """Rank descending; the top critical_top share is Critical, and so on down."""
    p = cfg["priority"]
    rank = pd.Series(impacts).rank(ascending=False, method="min") / len(impacts)
    cuts = [p["critical_top"], p["critical_top"] + p["high_next"],
            p["critical_top"] + p["high_next"] + p["medium_next"]]
    return [PRIORITIES[next((i for i, c in enumerate(cuts) if r <= c + 1e-12), 3)] for r in rank]


def build(src, cfg):
    rc = cfg["recommendations"]
    cls, feats = src["classification"], src["item_features"]
    recs = (hidden_opportunity(cls) + high_wastage(cls, feats, src["wastage_risk"], rc)
            + price_sensitive(src["price"], cls)
            + bundles(src["rules"], cls, cfg["market_basket"]["bundle_min_lift"])
            + low_performers(cls, feats, rc) + stock_before_peak(src["forecast"], cls, rc)
            + segment_campaigns(src["segments"], src["churn"])
            + ineffective_promotions(src["promotions"])
            + anomalous_locations(src["anomalies"], src["locations"], rc,
                                  cfg["anomaly"]["z_threshold"]))
    df = pd.DataFrame(recs).sort_values("estimated_impact_value", ascending=False,
                                        ignore_index=True)
    df["priority"] = assign_priority(df.estimated_impact_value, rc)
    df.insert(0, "recommendation_id", [f"REC-{i:03d}" for i in range(1, len(df) + 1)])
    df["as_of_date"] = src["as_of"]
    return df


def write_report(df, cfg):
    p = cfg["recommendations"]["priority"]
    counts = pd.crosstab(df.type, df.priority).reindex(columns=PRIORITIES, fill_value=0)
    lines = [
        "# Recommendations Report",
        "",
        f"Generated by `python_pipeline/recommendation_engine.py`, as of {df.as_of_date.iloc[0]}. "
        f"{len(df)} recommendations. Each one comes from an existing output (Steps 4-8), and "
        "every reason line is a value computed there.",
        "",
        "**Priority.** Every recommendation gets an `estimated_impact_value`, a money or "
        "magnitude proxy for what is at stake (the basis is shown on each one). All "
        f"recommendations are ranked together: the top {p['critical_top']:.0%} are Critical, the "
        f"next {p['high_next']:.0%} High, the next {p['medium_next']:.0%} Medium, and the rest "
        "Low. The proxies aren't all in the same unit. Segment spend and bundle revenue are much "
        "larger numbers than a single item's wastage, so compare priorities within a type as "
        "well as across types.",
        "",
        "| Type | " + " | ".join(PRIORITIES) + " | Total |",
        "|---|" + "---|" * (len(PRIORITIES) + 1),
    ]
    for t, row in counts.iterrows():
        lines.append(f"| {t} | " + " | ".join(str(int(v)) for v in row) + f" | {int(row.sum())} |")
    for pr in PRIORITIES:
        g = df[df.priority == pr]
        if g.empty:
            continue
        lines += ["", f"## {pr} ({len(g)})"]
        for r in g.itertuples():
            lines += ["", f"### {r.recommendation_id}: {r.action}", "",
                      f"Recommended Action: {r.action}", "", "Reason:"]
            lines += [f"  - {e}" for e in r.evidence]
            lines += ["", f"Priority: {r.priority}. Estimated impact: "
                          f"{r.estimated_impact_value:,.0f} ({r.impact_basis}). Type: {r.type}."]
    REPORT.write_text("\n".join(lines) + "\n")


def main():
    cfg = sr.load_config()
    df = build(load_sources(), cfg)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_DIR / "recommendations.parquet", index=False)
    write_report(df, cfg)
    print(pd.crosstab(df.type, df.priority).reindex(columns=PRIORITIES, fill_value=0))
    print("wrote", OUT_DIR, REPORT)


if __name__ == "__main__":
    main()

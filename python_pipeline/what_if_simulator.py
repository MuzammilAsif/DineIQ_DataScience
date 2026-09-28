"""Formula-based what-if simulator for one menu item (SRS Steps 40-41).

Each scenario starts from the item's current baseline (average price, unit cost, quantity sold,
wastage share, promotion dependency, elasticity) and applies a formula. There is no model
re-run, so a call returns in well under a second once the sources are loaded. Every result
is labelled as a simulated estimate.

Usage:
    from python_pipeline.what_if_simulator import simulate
    simulate("ITEM0042", "price_change", pct=0.10)
"""
from dataclasses import asdict, dataclass
from functools import lru_cache

import pandas as pd

from python_pipeline import segment_rules as sr

PQ = sr.CONFIG_PATH.parent.parent / "parquet_data"
NOTE = "Simulated estimate, not an actual result"


@dataclass
class Baseline:
    item_id: str
    item_name: str
    price: float                  # average realised price per unit (net of discounts)
    unit_cost: float
    quantity: float               # units sold over period_days
    wastage_pct: float            # wasted / (wasted + sold), in portions
    discount_pct: float           # discount / (revenue + discount)
    promotion_dependency: float   # share of revenue from promoted orders
    elasticity: float
    elasticity_source: str
    promo_margin_effect: float    # typical change in margin rate during a promotion
    period_days: int
    forecast_daily_demand: float | None = None


def metrics(price, unit_cost, quantity, waste_units):
    revenue = price * quantity
    margin = (price - unit_cost) * quantity
    waste_cost = waste_units * unit_cost
    return dict(revenue=revenue, contribution_margin=margin, demand=quantity,
                wastage_units=waste_units, wastage_cost=waste_cost,
                wastage_pct=waste_units / (waste_units + quantity) if waste_units + quantity else 0.0,
                profitability=margin / revenue if revenue else 0.0,
                net_margin_after_wastage=margin - waste_cost)


def waste_units(b):
    return b.quantity * b.wastage_pct / (1 - b.wastage_pct) if b.wastage_pct < 1 else 0.0


def baseline_metrics(b):
    return metrics(b.price, b.unit_cost, b.quantity, waste_units(b))


def result(b, scenario, inputs, sim, flags=(), limitations=(), assumptions=None):
    base = baseline_metrics(b)
    return dict(scenario=scenario, item_id=b.item_id, item_name=b.item_name, inputs=inputs,
                baseline=base, simulated=sim,
                change={k: sim[k] - base[k] for k in base},
                assumptions=assumptions or {}, flags=list(flags), limitations=list(limitations),
                note=NOTE)


def price_change(b, pct):
    """new_demand = quantity * (1 + elasticity * pct); wastage keeps its share of portions."""
    q = max(0.0, b.quantity * (1 + b.elasticity * pct))
    scale = q / b.quantity if b.quantity else 0.0
    sim = metrics(b.price * (1 + pct), b.unit_cost, q, waste_units(b) * scale)
    flags = ["demand falls to zero at this price change"] if q == 0 else []
    return result(b, "price_change", {"pct": pct}, sim, flags,
                  assumptions={"elasticity": b.elasticity, "elasticity_source": b.elasticity_source})


def discount_change(b, new_discount_pct):
    """A discount change is a price change on the effective (discounted) price."""
    list_price = b.price / (1 - b.discount_pct)
    pct = list_price * (1 - new_discount_pct) / b.price - 1
    r = price_change(b, pct)
    r.update(scenario="discount_change",
             inputs={"new_discount_pct": new_discount_pct, "current_discount_pct": b.discount_pct,
                     "effective_price_change_pct": pct})
    return r


def promotion_frequency(b, factor):
    """Promoted revenue scales by factor; the extra promoted revenue earns the typical promotion
    margin rate (current margin rate + promo_margin_effect). factor 1 returns the baseline."""
    base = baseline_metrics(b)
    extra_share = b.promotion_dependency * (factor - 1)
    q = b.quantity * (1 + extra_share)
    rate = base["profitability"] + b.promo_margin_effect
    revenue = base["revenue"] * (1 + extra_share)
    margin = base["contribution_margin"] + base["revenue"] * extra_share * rate
    sim = metrics(revenue / q if q else 0.0, b.unit_cost, q, waste_units(b) * (q / b.quantity))
    sim.update(revenue=revenue, contribution_margin=margin,
               profitability=margin / revenue if revenue else 0.0,
               net_margin_after_wastage=margin - sim["wastage_cost"])
    flags = ["item has no promoted revenue, so promotion frequency has no effect"] \
        if b.promotion_dependency == 0 else []
    return result(b, "promotion_frequency", {"factor": factor}, sim, flags,
                  limitations=["Promotion-driven wastage increases are not modelled; wastage "
                               "keeps its current share of portions."],
                  assumptions={"promotion_dependency": b.promotion_dependency,
                               "promo_margin_effect": b.promo_margin_effect})


def remove_item(b):
    sim = metrics(b.price, b.unit_cost, 0.0, 0.0)
    sim["profitability"] = 0.0
    return result(b, "remove_item", {}, sim,
                  limitations=["Substitution and cannibalisation are not modelled: customers "
                               "who bought this item may buy other items instead, so the real "
                               "revenue loss is likely smaller."])


def reduce_prep(b, pct):
    """Prepared quantity falls by pct. The cut comes out of waste first; any cut beyond current
    waste comes out of sales. Stockout risk is flagged if that happens or if daily prep falls
    below the forecast daily demand."""
    w = waste_units(b)
    prepared = b.quantity + w
    cut = prepared * pct
    new_w = max(0.0, w - cut)
    lost_sales = max(0.0, cut - w)
    q = b.quantity - lost_sales
    sim = metrics(b.price, b.unit_cost, q, new_w)
    flags = []
    if lost_sales > 0:
        flags.append(f"stockout: the cut exceeds current waste by {lost_sales:,.0f} units, "
                     "which comes out of sales")
    new_daily_prep = prepared * (1 - pct) / b.period_days
    if b.forecast_daily_demand is not None and new_daily_prep < b.forecast_daily_demand:
        flags.append(f"stockout risk: new daily prep {new_daily_prep:,.1f} units is below the "
                     f"forecast daily demand of {b.forecast_daily_demand:,.1f}")
    return result(b, "reduce_prep", {"pct": pct}, sim, flags,
                  assumptions={"prepared_units": prepared, "new_daily_prep": new_daily_prep,
                               "forecast_daily_demand": b.forecast_daily_demand})


def demand_change(b, pct):
    scale = max(0.0, 1 + pct)
    sim = metrics(b.price, b.unit_cost, b.quantity * scale, waste_units(b) * scale)
    return result(b, "demand_change", {"pct": pct}, sim)


def wastage_assumption(b, new_wastage_pct):
    new_w = b.quantity * new_wastage_pct / (1 - new_wastage_pct)
    sim = metrics(b.price, b.unit_cost, b.quantity, new_w)
    r = result(b, "wastage_assumption", {"new_wastage_pct": new_wastage_pct}, sim)
    r["net_margin_improvement"] = r["change"]["net_margin_after_wastage"]
    return r


SCENARIOS = {"price_change": price_change, "discount_change": discount_change,
             "promotion_frequency": promotion_frequency, "remove_item": remove_item,
             "reduce_prep": reduce_prep, "demand_change": demand_change,
             "wastage_assumption": wastage_assumption}


@lru_cache(maxsize=1)
def _sources():
    as_of = max((PQ / "menu_classification").glob("as_of_date=*")).name.split("=", 1)[1]
    cls = pd.read_parquet(PQ / "menu_classification" / f"as_of_date={as_of}")
    feats = pd.read_parquet(PQ / "features" / "item_features" / f"as_of_date={as_of}",
                            columns=["item_id", "discount_percentage"])
    price = pd.read_parquet(PQ / "price_sensitivity" / "items",
                            columns=["item_id", "elasticity", "price_sensitivity"])
    promos = pd.read_parquet(PQ / "promotion_effectiveness")
    promos = promos[promos.pre_window_in_data]
    effect = float((promos.during_margin_pct - promos.pre_margin_pct).median())
    fc = pd.read_parquet(PQ / "demand_forecast" / "item_day").groupby("item_id").forecast.mean()
    items = cls.merge(feats, on="item_id", how="left").merge(price, on="item_id", how="left")
    return as_of, items.set_index("item_id"), effect, fc


def load_baseline(item_id, cfg=None):
    cfg = cfg or sr.load_config()
    wi = cfg["what_if"]
    as_of, items, effect, fc = _sources()
    r = items.loc[item_id]
    measured = r.price_sensitivity in wi["elasticity_defaults"] and pd.notna(r.elasticity) \
        and r.elasticity < 0
    if measured:
        cap = wi["max_abs_elasticity"]
        e, src = max(-cap, float(r.elasticity)), f"measured ({r.price_sensitivity})"
        if r.elasticity < -cap:
            src += f", capped at -{cap}"
    else:
        e = wi["elasticity_defaults"]["unclassified"]
        src = f"default for {r.price_sensitivity or 'unclassified'} items"
    start = max(pd.Timestamp(r.launch_date), pd.Timestamp(f"{as_of[:4]}-01-01"))
    q = float(r.total_quantity_sold)
    return Baseline(
        item_id=item_id, item_name=r.item_name,
        price=r.item_revenue / q if q else 0.0, unit_cost=r.item_cost / q if q else 0.0,
        quantity=q, wastage_pct=float(r.wastage_percentage or 0.0),
        discount_pct=float(r.discount_percentage or 0.0),
        promotion_dependency=float(r.promotion_dependency or 0.0),
        elasticity=e, elasticity_source=src, promo_margin_effect=effect,
        period_days=(pd.Timestamp(as_of) - start).days + 1,
        forecast_daily_demand=float(fc[item_id]) if item_id in fc.index else None)


def simulate(item_id, scenario, **params):
    b = load_baseline(item_id)
    out = SCENARIOS[scenario](b, **params)
    out["baseline_inputs"] = asdict(b)
    return out


if __name__ == "__main__":
    import json
    import sys
    item = sys.argv[1] if len(sys.argv) > 1 else "ITEM0001"
    print(json.dumps(simulate(item, "price_change", pct=0.10), indent=2, default=str))

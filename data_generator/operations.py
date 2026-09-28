import numpy as np
import pandas as pd

from common import (
    BASE_VOLUMES, N_DAYS, build_cdf, day_to_date_str, make_ids, sample_from_cdf_rows,
    weighted_sample_without_replacement,
)
import reference_data as ref

RATING_PROPENSITY = np.array([2.0, 1.3, 1.0, 1.0, 1.0, 0.8])
DELIVERY_CHANNELS = [2, 3, 4]
WASTAGE_REASONS = ["Overproduction", "Expired", "Spoilage", "CustomerReturn", "PrepError"]
REASON_P = {
    "default": [0.45, 0.20, 0.15, 0.08, 0.12],
    "high_waste": [0.50, 0.15, 0.25, 0.02, 0.08],
    "poor_rating": [0.30, 0.15, 0.10, 0.35, 0.10],
}


def _trap_coverage(ctx):
    p = ctx.promotions
    trap_days = p["active"][p["trap"]].any(axis=1)
    return trap_days, p["items_mask"][p["trap"]]


def build_ratings(ctx):
    rng = ctx.rng
    m, r, o, oi = ctx.menu, ctx.restaurants, ctx.orders, ctx.order_items
    target = int(round(BASE_VOLUMES["ratings"] * ctx.scale))
    n_item = int(target * 0.85)
    n_overall = target - n_item

    completed_line = ~o["cancelled"][oi["order"]]
    line_w = RATING_PROPENSITY[o["arch"][oi["order"]]] * np.where(m["poor_rating"][oi["item"]], 1.5, 1.0)
    line_pick = weighted_sample_without_replacement(line_w * completed_line, n_item, rng)
    order_w = RATING_PROPENSITY[o["arch"]] * ~o["cancelled"]
    order_pick = weighted_sample_without_replacement(order_w, n_overall, rng)

    rated_order = np.concatenate([oi["order"][line_pick], order_pick])
    rated_item = np.concatenate([oi["item"][line_pick], np.full(n_overall, -1)])
    loc = o["loc"][rated_order]
    day = o["day"][rated_order]
    delivery = np.isin(o["channel"][rated_order], DELIVERY_CHANNELS)

    trap_days, trap_items = _trap_coverage(ctx)
    item_part = rated_item >= 0
    mean = np.where(item_part, m["rating_mean"][np.maximum(rated_item, 0)], 3.8)
    mean += r["quality"][loc] * np.where(item_part, 1.0, 1.5)
    mean -= 0.3 * delivery
    rushed = item_part & trap_days[day] & trap_items[np.maximum(rated_item, 0)] & (o["promo"][rated_order] >= 0)
    mean -= 0.4 * rushed
    value = np.clip(np.round(mean + rng.normal(0, 0.75, target)), 1, 5).astype(int)

    rating_day = np.minimum(day + rng.geometric(0.5, target) - 1, N_DAYS - 1)
    has_text = rng.random(target) < 0.35
    text_choice = rng.integers(0, 5, target)
    review = np.array([ref.REVIEW_TEXT[v][t] for v, t in zip(value, text_choice)], dtype=object)
    review[~has_text] = None

    order = np.lexsort((rng.random(target), rating_day))
    df = pd.DataFrame({
        "rating_id": make_ids("RAT", target, 7),
        "customer_id": ctx.customers["ids"][o["cust"][rated_order]][order],
        "item_id": np.where(item_part, m["ids"][np.maximum(rated_item, 0)], None)[order],
        "location_id": r["ids"][loc][order],
        "order_id": o["ids"][rated_order][order],
        "rating_value": value[order],
        "rating_date": day_to_date_str(rating_day[order]),
        "review_text": review[order],
    })
    return df


def _moving_average(values, window):
    padded = np.pad(values, ((0, 0), (window // 2, window // 2)), mode="edge")
    cs = np.cumsum(padded, axis=1)
    cs = np.pad(cs, ((0, 0), (1, 0)))
    return (cs[:, window:] - cs[:, :-window]) / window


def build_inventory_and_wastage(ctx):
    rng = ctx.rng
    m, r, o, oi, p = ctx.menu, ctx.restaurants, ctx.orders, ctx.order_items, ctx.promotions
    n_loc, n_items = r["n"], m["n"]

    completed = ~o["cancelled"][oi["order"]]
    flat = ((o["loc"][oi["order"]] * n_items + oi["item"]) * N_DAYS + o["day"][oi["order"]])[completed]
    consumed_all = np.bincount(flat, weights=oi["qty"][completed], minlength=n_loc * n_items * N_DAYS)
    consumed_all = consumed_all.reshape(n_loc * n_items, N_DAYS)

    pairs = np.where(oi["carried"].ravel())[0]
    pair_loc, pair_item = pairs // n_items, pairs % n_items
    consumed = consumed_all[pairs]
    days = np.arange(N_DAYS)
    avail = (days[None, :] >= m["first_day"][pair_item][:, None]) & (days[None, :] <= m["last_day"][pair_item][:, None])
    n_pairs = len(pairs)

    overprep = rng.uniform(1.08, 1.15, n_pairs)
    high_waste = m["high_waste"][pair_item]
    overprep[high_waste] = rng.uniform(1.4, 1.6, high_waste.sum())
    covered = np.tensordot(p["active"].astype(np.float32), p["items_mask"].astype(np.float32), axes=([0], [0])) > 0
    promo_cover = covered[:, pair_loc, pair_item].T
    trap_days, trap_items = _trap_coverage(ctx)
    trap_cover = trap_items[pair_item][:, None] & trap_days[None, :]
    # kitchens over-prepare for promotions, and far more for the August BOGO campaign
    prep_factor = overprep[:, None] * np.where(trap_cover, 1.3, np.where(promo_cover, 1.15, 1.0))

    forecast = (0.6 * consumed + 0.4 * _moving_average(consumed, 7)) * rng.lognormal(0, 0.12, consumed.shape)
    avail_days = np.maximum(avail.sum(axis=1), 1)
    min_prep = 0.5 * (consumed * avail).sum(axis=1) / avail_days
    prepared = np.maximum(consumed, np.maximum(forecast * prep_factor, min_prep[:, None]))
    is_pcs = (m["units"][pair_item] == "pcs")[:, None]
    portion = m["portion"][pair_item][:, None]
    prepared = np.where(is_pcs, np.ceil(prepared), prepared) * avail

    unsold = prepared - consumed
    waste_p = np.full(n_pairs, 0.035)
    waste_p[m["promo_dependent"][pair_item]] = 0.08
    waste_p[m["poor_rating"][pair_item]] = 0.06
    waste_p[high_waste] = 0.45
    waste_p = np.minimum(waste_p[:, None] * np.where(trap_cover, 1.4, 1.0), 0.9)
    wasted_event = avail & (unsold > 0) & (rng.random(consumed.shape) < waste_p)
    frac = np.where(high_waste[:, None], rng.beta(5, 2, consumed.shape), rng.beta(2, 3, consumed.shape))
    wasted = np.where(wasted_event, unsold * frac, 0.0)
    wasted = np.where(is_pcs, np.minimum(np.maximum(np.ceil(wasted), wasted_event), unsold), wasted)

    consumed_u = np.round(consumed * portion, 2)
    prepared_u = np.round(prepared * portion, 2)
    wasted_u = np.minimum(np.round(wasted * portion, 2), prepared_u - consumed_u)

    opening = np.zeros((n_pairs, N_DAYS))
    received = np.zeros((n_pairs, N_DAYS))
    closing = np.zeros((n_pairs, N_DAYS))
    stock = np.round(prepared_u[:, 0] * rng.uniform(1.0, 2.0, n_pairs), 2)
    stock = np.where(is_pcs[:, 0], np.ceil(stock), stock)
    for d in range(N_DAYS):
        a = avail[:, d]
        opening[:, d] = np.where(a, stock, 0.0)
        need = a & (stock < prepared_u[:, d] * 1.2)
        target = prepared_u[:, d] * rng.uniform(2.0, 3.0, n_pairs)
        recv = np.where(need, np.maximum(target - stock, 0), 0.0)
        recv = np.where(is_pcs[:, 0], np.ceil(recv), np.ceil(recv * 100) / 100)
        received[:, d] = recv
        new_stock = np.round(stock + recv - consumed_u[:, d] - wasted_u[:, d], 2)
        closing[:, d] = np.where(a, new_stock, 0.0)
        stock = np.where(a, new_stock, stock)

    day_idx, pair_idx = np.nonzero(avail.T)
    units = m["units"][pair_item]
    inventory = pd.DataFrame({
        "inventory_id": make_ids("INV", len(day_idx), 8),
        "item_id": m["ids"][pair_item[pair_idx]],
        "location_id": r["ids"][pair_loc[pair_idx]],
        "date": day_to_date_str(day_idx),
        "opening_stock": opening[pair_idx, day_idx],
        "received_stock": received[pair_idx, day_idx],
        "prepared_quantity": prepared_u[pair_idx, day_idx],
        "consumed_stock": consumed_u[pair_idx, day_idx],
        "closing_stock": closing[pair_idx, day_idx],
        "unit": units[pair_idx],
    })

    w_day, w_pair = np.nonzero((wasted_u > 0).T)
    w_item, w_loc = pair_item[w_pair], pair_loc[w_pair]
    qty_u = wasted_u[w_pair, w_day]
    cost = np.round(qty_u / m["portion"][w_item] * ctx.pricing["cost_mat"][w_day, w_loc, w_item], 2)
    reason_group = np.where(m["high_waste"][w_item], 1, np.where(m["poor_rating"][w_item], 2, 0))
    reason_cdf = build_cdf(np.array([REASON_P["default"], REASON_P["high_waste"], REASON_P["poor_rating"]]))
    reason = sample_from_cdf_rows(reason_cdf, reason_group, rng)
    wastage = pd.DataFrame({
        "wastage_id": make_ids("WST", len(w_day), 7),
        "item_id": m["ids"][w_item],
        "location_id": r["ids"][w_loc],
        "date": day_to_date_str(w_day),
        "quantity_wasted": qty_u,
        "unit": m["units"][w_item],
        "cost_of_waste": cost,
        "reason": np.array(WASTAGE_REASONS)[reason],
    })
    ctx.inventory = {"wastage_prepared": prepared_u[w_pair, w_day]}
    return inventory, wastage

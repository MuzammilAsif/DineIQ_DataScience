import numpy as np
import pandas as pd

from common import (
    BASE_VOLUMES, N_DAYS, START_DATE, build_cdf, make_ids, sample_from_cdf_rows, to_day_index,
)
from dimensions import CHANNELS
import reference_data as ref

# Monday..Sunday, per location type in reference_data.LOCATION_TYPES order
DOW_FACTORS = np.array([
    [0.85, 0.82, 0.85, 0.92, 1.25, 1.35, 1.30],  # Urban
    [0.80, 0.78, 0.80, 0.88, 1.25, 1.45, 1.40],  # Suburban
    [0.78, 0.76, 0.80, 0.86, 1.30, 1.50, 1.45],  # Mall
    [0.90, 0.88, 0.90, 0.95, 1.25, 1.20, 1.35],  # Highway
    [1.00, 1.00, 1.00, 1.02, 1.15, 1.12, 0.90],  # Downtown
])
MONTH_FACTORS = np.array([0.92, 0.90, 0.88, 1.00, 1.00, 0.94, 0.97, 1.02, 0.98, 1.03, 1.08, 1.20])

HOUR_BASE = np.zeros(24)
HOUR_BASE[[10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1]] = \
    [1, 3, 9, 11, 7, 3, 3, 4, 6, 10, 13, 12, 8, 4, 2, 1]
HOUR_RAMADAN = np.zeros(24)
HOUR_RAMADAN[[11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4]] = \
    [0.3, 0.8, 0.9, 0.8, 0.6, 0.8, 2, 18, 16, 12, 10, 9, 7, 5, 4, 3, 3, 1]

TYPE_CHANNEL_P = np.array([
    [0.35, 0.15, 0.08, 0.24, 0.18],
    [0.30, 0.15, 0.08, 0.25, 0.22],
    [0.60, 0.20, 0.04, 0.10, 0.06],
    [0.60, 0.35, 0.01, 0.03, 0.01],
    [0.40, 0.20, 0.08, 0.18, 0.14],
])
CANCEL_RATE_BY_CHANNEL = np.array([0.02, 0.03, 0.06, 0.06, 0.08])
PAYMENT_METHODS = ["Cash", "Card", "Wallet", "Online"]
PAYMENT_P_BY_CHANNEL = np.array([
    [0.45, 0.40, 0.15, 0.00],
    [0.55, 0.30, 0.15, 0.00],
    [0.00, 0.25, 0.15, 0.60],
    [0.10, 0.20, 0.20, 0.50],
    [0.50, 0.00, 0.00, 0.50],
])

# indexed by archetype: loyal, frequent, promo, churned, new, occasional
ANNUAL_ORDER_RATE = np.array([26.0, 12.0, 5.0, 14.0, 10.0, 1.6])
HOME_LOCATION_P = np.array([0.88, 0.78, 0.60, 0.65, 0.60, 0.55])
PROMO_USE_P = np.array([0.12, 0.20, 0.90, 0.15, 0.35, 0.25])
EXTRA_LINES = np.array([1.5, 0.3, 0.8, 0.0, 0.0, 0.0])
FAVORITE_P = np.array([0.40, 0.35, 0.15, 0.25, 0.10, 0.15])
PROMO_ARCHETYPE = 2

LINES_BY_CHANNEL = np.array([5.2, 3.6, 4.0, 4.0, 3.6])


def _day_of_week():
    first_monday = np.datetime64("1970-01-05")
    return (np.arange(N_DAYS) + (START_DATE - first_monday).astype(int)) % 7


def _month_of_day():
    dates = START_DATE + np.arange(N_DAYS).astype("timedelta64[D]")
    return dates.astype("datetime64[M]").astype(int) % 12


def _location_day_weights(ctx):
    r, p = ctx.restaurants, ctx.promotions
    rng = ctx.rng
    dow = _day_of_week()
    month = _month_of_day()
    base = DOW_FACTORS[r["type_idx"]][:, dow] * MONTH_FACTORS[month][None, :]
    base *= (1 + 0.08 * np.arange(N_DAYS) / N_DAYS)[None, :]
    for d, mult in ref.HOLIDAY_MULTIPLIERS.items():
        base[:, to_day_index(d)] *= mult
    ramadan = np.zeros(N_DAYS, dtype=bool)
    ramadan[to_day_index(ref.RAMADAN[0]):to_day_index(ref.RAMADAN[1]) + 1] = True
    base[:, ramadan] *= 0.9
    promo_lift = np.exp(np.tensordot(np.log1p(p["lift"]), p["active"].astype(float), axes=1)).T
    base *= promo_lift
    base *= rng.lognormal(0, 0.08, base.shape)
    any_promo = p["active"].any(axis=0).T
    promo_seeker = base * np.where(any_promo, 15.0, 0.08)
    return base, promo_seeker, ramadan


def build_orders(ctx):
    rng = ctx.rng
    r, c, p = ctx.restaurants, ctx.customers, ctx.promotions
    target_orders = BASE_VOLUMES["orders"] * ctx.scale

    arch = c["archetype"]
    window_len = (c["window_end"] - c["window_start"] + 1) / N_DAYS
    expected = ANNUAL_ORDER_RATE[arch] * window_len
    lam = expected * target_orders / expected.sum()
    counts = rng.poisson(lam)
    # keep never-ordered sign-ups to a realistic minority
    counts[(counts == 0) & (rng.random(c["n"]) < 0.75)] = 1
    cust = np.repeat(np.arange(c["n"]), counts)
    n = len(cust)
    order_arch = arch[cust]

    use_home = c["has_home"][cust] & (rng.random(n) < HOME_LOCATION_P[order_arch])
    loc = np.where(use_home, c["home"][cust], rng.choice(r["n"], n, p=r["popularity"]))

    weights, promo_weights, ramadan = _location_day_weights(ctx)
    cdf = build_cdf(np.vstack([weights, promo_weights]))
    row = np.where(order_arch == PROMO_ARCHETYPE, loc + r["n"], loc)
    day = sample_from_cdf_rows(cdf, row, rng, c["window_start"][cust], c["window_end"][cust])

    hour_weights = []
    for t in range(len(ref.LOCATION_TYPES)):
        base = HOUR_BASE.copy()
        if ref.LOCATION_TYPES[t] == "Downtown":
            base[12:15] *= 1.5
        if ref.LOCATION_TYPES[t] == "Highway":
            base[10:24] += 2
            base[[0, 1, 2, 6, 7, 8, 9]] += 1
        hour_weights += [base, HOUR_RAMADAN]
    hour_row = r["type_idx"][loc] * 2 + ramadan[day]
    hour = sample_from_cdf_rows(build_cdf(np.array(hour_weights)), hour_row, rng)
    seconds = day * 86400 + hour * 3600 + rng.integers(0, 3600, n)
    order_dt = START_DATE.astype("datetime64[s]") + seconds.astype("timedelta64[s]")

    loc_type = r["type_idx"][loc]
    use_pref_p = np.where(r["types"][loc] == "Highway", 0.1, 0.6)
    pref_idx = pd.Categorical(c["preferred_channel"], categories=CHANNELS).codes[cust]
    by_type = sample_from_cdf_rows(build_cdf(TYPE_CHANNEL_P), loc_type, rng)
    channel = np.where(rng.random(n) < use_pref_p, pref_idx, by_type)

    active = p["active"][:, day, loc].T
    score = rng.random(active.shape, dtype=np.float32) * active
    score[:, p["trap"]] *= 3
    chosen = score.argmax(axis=1)
    apply = active.any(axis=1) & (rng.random(n) < PROMO_USE_P[order_arch])
    promo = np.where(apply, chosen, -1)

    cancelled = rng.random(n) < CANCEL_RATE_BY_CHANNEL[channel]
    payment = sample_from_cdf_rows(build_cdf(PAYMENT_P_BY_CHANNEL), channel, rng)

    order = np.argsort(seconds, kind="stable")
    ctx.orders = {
        "n": n,
        "cust": cust[order],
        "arch": order_arch[order],
        "loc": loc[order],
        "day": day[order],
        "datetime": order_dt[order],
        "channel": channel[order],
        "promo": promo[order],
        "cancelled": cancelled[order],
        "payment": payment[order],
        "ids": make_ids("ORD", n, 8),
    }


def _season_multiplier(ctx):
    m = ctx.menu
    month = np.arange(1, 13)
    mult = np.ones((12, m["n"]))
    for season, months in ref.IN_SEASON_MONTHS.items():
        cols = m["season"] == season
        in_season = np.isin(month, list(months))
        shoulder = np.isin(month, list(ref.SHOULDER_MONTHS[season]))
        mult[:, cols] = np.where(in_season, 2.3, np.where(shoulder, 1.0, 0.3))[:, None]
    cat = np.array(m["cat_names"])[m["cat_idx"]]
    summer, winter = np.isin(month, [5, 6, 7, 8]), np.isin(month, [11, 12, 1, 2])
    cold = (cat == "Cold Beverages & Shakes") & (m["season"] == "AllYear")
    warm = np.isin(cat, ["Hot Beverages", "Soups"]) & (m["season"] == "AllYear")
    mult[:, cold] *= np.where(summer, 1.3, np.where(winter, 0.8, 1.0))[:, None]
    mult[:, warm] *= np.where(winter, 1.3, np.where(summer, 0.8, 1.0))[:, None]
    return mult


def build_order_items(ctx):
    rng = ctx.rng
    m, r, p, o = ctx.menu, ctx.restaurants, ctx.promotions, ctx.orders
    n_items = m["n"]

    must_carry = m["high_volume"] | m["high_waste"] | m["promo_dependent"]
    carried = (rng.random((r["n"], n_items)) < 0.9) | must_carry[None, :]
    loc_mult = rng.lognormal(0, 0.35, (r["n"], n_items)) * carried

    lam = LINES_BY_CHANNEL[o["channel"]] + EXTRA_LINES[o["arch"]]
    target_lines = BASE_VOLUMES["order_lines"] * ctx.scale
    lam *= (target_lines - o["n"]) / lam.sum()
    n_lines = 1 + rng.poisson(lam)
    line_order = np.repeat(np.arange(o["n"]), n_lines)

    n_codes = p["n"] + 1
    order_key = (o["loc"] * N_DAYS + o["day"]) * n_codes + (o["promo"] + 1)
    ctx_keys, order_ctx = np.unique(order_key, return_inverse=True)
    ctx_code = ctx_keys % n_codes
    ctx_day = (ctx_keys // n_codes) % N_DAYS
    ctx_loc = ctx_keys // n_codes // N_DAYS

    season = _season_multiplier(ctx)
    month = _month_of_day()
    weights = (m["popularity"][None, :] * loc_mult[ctx_loc] * season[month[ctx_day]]
               * ctx.pricing["demand_effect"][ctx_day])
    has_promo = ctx_code > 0
    covered = np.zeros_like(weights, dtype=bool)
    covered[has_promo] = p["items_mask"][ctx_code[has_promo] - 1]
    boost = np.where(m["promo_dependent"], 6.0, 2.5)[None, :] * np.where(ctx_code == p["trap"] + 1, 1.4, 1.0)[:, None]
    weights = np.where(covered, weights * boost, weights)
    weights = np.where(~covered & m["promo_dependent"][None, :], weights * 0.1, weights)

    line_ctx = order_ctx[line_order]
    line_item = sample_from_cdf_rows(build_cdf(weights), line_ctx, rng)

    everyday = ~m["promo_dependent"] & ~m["late_launch"] & (m["last_day"] == N_DAYS - 1)
    fav_pool = np.where(everyday)[0]
    fav_p = m["popularity"][fav_pool] / m["popularity"][fav_pool].sum()
    favorites = rng.choice(fav_pool, (ctx.customers["n"], 3), p=fav_p)
    line_cust = o["cust"][line_order]
    use_fav = rng.random(len(line_order)) < FAVORITE_P[o["arch"][line_order]]
    fav_item = favorites[line_cust, rng.integers(0, 3, len(line_order))]
    fav_ok = weights[line_ctx, fav_item] > 0
    line_item = np.where(use_fav & fav_ok, fav_item, line_item)

    cat = np.array(m["cat_names"])[m["cat_idx"]]
    qty_lam = np.where(cat == "Sides & Breads", 1.0,
                       np.where(np.isin(cat, ["Hot Beverages", "Cold Beverages & Shakes"]), 0.6, 0.2))
    qty = 1 + rng.poisson(qty_lam[line_item] + 0.2 * (o["channel"][line_order] == 0))

    key = line_order.astype(np.int64) * n_items + line_item
    uniq, inverse = np.unique(key, return_inverse=True)
    qty = np.bincount(inverse, weights=qty).astype(np.int64)
    line_order = uniq // n_items
    line_item = uniq % n_items

    line_promo = o["promo"][line_order]
    has_promo = line_promo >= 0
    eligible = np.zeros(len(line_order), dtype=bool)
    eligible[has_promo] = p["items_mask"][line_promo[has_promo], line_item[has_promo]]
    promo_type = np.where(has_promo, p["type_idx"][np.maximum(line_promo, 0)], -1)
    promo_value = np.where(has_promo, p["value"][np.maximum(line_promo, 0)], 0.0)
    bogo = eligible & (promo_type == 2)
    qty = np.where(bogo, np.maximum(qty, 2), qty)

    line_day = o["day"][line_order]
    line_loc = o["loc"][line_order]
    unit_price = ctx.pricing["price_mat"][line_day, line_loc, line_item]
    unit_cost = ctx.pricing["cost_mat"][line_day, line_loc, line_item]
    assert not np.isnan(unit_price).any(), "sampled an item with no effective price"
    gross = qty * unit_price

    n_orders = o["n"]
    eligible_count = np.bincount(line_order, weights=eligible, minlength=n_orders)
    eligible_gross = np.bincount(line_order, weights=gross * eligible, minlength=n_orders)
    discount = np.zeros(len(line_order))
    pct = eligible & (promo_type == 0)
    discount[pct] = gross[pct] * promo_value[pct] / 100
    discount[bogo] = (qty[bogo] // 2) * unit_price[bogo]
    combo = eligible & (promo_type == 3) & (eligible_count[line_order] >= 2)
    discount[combo] = gross[combo] * promo_value[combo] / 100
    fixed = eligible & (promo_type == 1)
    order_fixed = np.minimum(promo_value[fixed], eligible_gross[line_order[fixed]])
    discount[fixed] = gross[fixed] * order_fixed / eligible_gross[line_order[fixed]]
    discount = np.round(discount, 2)

    subtotal = np.bincount(line_order, weights=gross, minlength=n_orders)
    below_min = (o["promo"] >= 0) & (subtotal < p["min_order"][np.maximum(o["promo"], 0)])
    discount[below_min[line_order]] = 0.0
    order_discount = np.bincount(line_order, weights=discount, minlength=n_orders)
    o["promo"] = np.where(order_discount > 0, o["promo"], -1)

    subtotal = np.round(subtotal, 2)
    order_discount = np.round(order_discount, 2)
    tax = np.round(ctx.restaurants["tax_rate"][o["loc"]] * (subtotal - order_discount), 2)
    o["subtotal"] = subtotal
    o["discount"] = order_discount
    o["tax"] = tax
    o["total"] = np.round(subtotal - order_discount + tax, 2)

    ctx.order_items = {
        "order": line_order,
        "item": line_item,
        "qty": qty,
        "unit_price": unit_price,
        "unit_cost": unit_cost,
        "discount": discount,
        "line_total": np.round(gross - discount, 2),
        "carried": carried,
    }

    orders_df = pd.DataFrame({
        "order_id": o["ids"],
        "customer_id": ctx.customers["ids"][o["cust"]],
        "location_id": r["ids"][o["loc"]],
        "order_datetime": np.char.replace(np.datetime_as_string(o["datetime"], unit="s"), "T", " "),
        "order_channel": np.array(CHANNELS)[o["channel"]],
        "order_status": np.where(o["cancelled"], "Cancelled", "Completed"),
        "promotion_id": np.where(o["promo"] >= 0, p["ids"][np.maximum(o["promo"], 0)], None),
        "subtotal": subtotal,
        "discount_amount": order_discount,
        "tax_amount": tax,
        "total_amount": o["total"],
        "payment_method": np.array(PAYMENT_METHODS)[o["payment"]],
    })
    items_df = pd.DataFrame({
        "order_item_id": make_ids("OI", len(line_order), 9),
        "order_id": o["ids"][line_order],
        "item_id": m["ids"][line_item],
        "quantity": qty,
        "unit_price": unit_price,
        "unit_cost": unit_cost,
        "discount_amount": discount,
        "line_total": ctx.order_items["line_total"],
    })
    return orders_df, items_df

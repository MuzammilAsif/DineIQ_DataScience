import numpy as np
import pandas as pd
from faker import Faker

from common import (
    BASE_VOLUMES, END_DATE, N_DAYS, PRICE_HISTORY_START, SEED, START_DATE,
    dates_to_str, make_ids, to_day_index,
)
import reference_data as ref

TYPE_POPULARITY = {"Mall": 1.3, "Downtown": 1.2, "Urban": 1.1, "Suburban": 0.9, "Highway": 0.6}
TYPE_SEATING = {"Mall": (80, 140), "Urban": (90, 160), "Downtown": (70, 120), "Suburban": (100, 180),
                "Highway": (120, 220)}
BIG_CITIES = {"Karachi", "Lahore"}


def build_restaurants(ctx):
    rng = ctx.rng
    faker = Faker("en_PK")
    faker.seed_instance(SEED)
    n = len(ref.LOCATIONS)
    cities, regions, areas, types, lats, lons = map(np.array, zip(*ref.LOCATIONS))
    lats = lats.astype(float) + rng.uniform(-0.004, 0.004, n)
    lons = lons.astype(float) + rng.uniform(-0.004, 0.004, n)

    names = np.array([f"DineIQ {area}" if city in area or types[i] == "Highway" else f"DineIQ {city} {area}"
                      for i, (city, area) in enumerate(zip(cities, areas))])
    addresses = [f"Plot {rng.integers(1, 400)}, {faker.street_name()}, {area}, {city}"
                 for area, city in zip(areas, cities)]
    seating = np.array([rng.integers(*TYPE_SEATING[t]) for t in types]) // 5 * 5
    opening_days = rng.integers(0, (np.datetime64("2024-10-01") - np.datetime64("2012-01-01")).astype(int), n)
    opening_dates = np.datetime64("2012-01-01") + opening_days.astype("timedelta64[D]")

    popularity = np.array([TYPE_POPULARITY[t] for t in types]) * rng.lognormal(0, 0.25, n)
    popularity *= np.where(np.isin(cities, list(BIG_CITIES)), 1.15, 1.0)

    location_ids = make_ids("LOC", n, 4)
    df = pd.DataFrame({
        "location_id": location_ids,
        "name": names,
        "city": cities,
        "region": regions,
        "address": addresses,
        "location_type": types,
        "seating_capacity": seating,
        "opening_date": dates_to_str(opening_dates),
        "latitude": np.round(lats, 6),
        "longitude": np.round(lons, 6),
    })
    ctx.restaurants = {
        "ids": location_ids,
        "n": n,
        "types": types,
        "type_idx": np.array([ref.LOCATION_TYPES.index(t) for t in types]),
        "cities": cities,
        "popularity": popularity / popularity.sum(),
        "tax_rate": np.array([ref.TAX_RATE_BY_REGION[r] for r in regions]),
        # service quality differences show up in ratings and in how the same item sells per location
        "quality": rng.normal(0, 0.25, n),
    }
    return df


def build_menu(ctx):
    rng = ctx.rng
    cat_names = [c[0] for c in ref.CATEGORIES]
    categories = pd.DataFrame({
        "category_id": make_ids("CAT", len(cat_names), 2),
        "category_name": cat_names,
        "description": [c[1] for c in ref.CATEGORIES],
    })

    n = len(ref.MENU)
    item_cat = np.array([cat_names.index(c) for c, _, _ in ref.MENU])
    names = np.array([name for _, name, _ in ref.MENU])
    list_price = np.array([p for _, _, p in ref.MENU], dtype=float)
    cat_weight = np.array([c[2] for c in ref.CATEGORIES])
    units = np.array([ref.CATEGORIES[c][3] for c in item_cat])
    portion = np.array([ref.CATEGORIES[c][4] for c in item_cat])

    def flag(names_list):
        return np.isin(names, names_list)

    high_volume = flag(ref.HIGH_VOLUME_LOW_MARGIN)
    high_margin = flag(ref.HIGH_MARGIN_LOW_VOLUME)
    high_waste = flag(ref.HIGH_WASTAGE_POPULAR)
    poor_rating = flag(ref.POORLY_RATED)
    promo_dependent = flag(ref.PROMOTION_DEPENDENT)

    popularity = cat_weight[item_cat] * rng.lognormal(0, 0.45, n)
    popularity[high_volume] *= 4.0
    popularity[high_margin] *= 0.3
    popularity[high_waste] *= 2.5
    popularity[promo_dependent] *= 1.5

    cost_ratio = rng.uniform(0.32, 0.45, n)
    cost_ratio[high_volume] = rng.uniform(0.64, 0.74, high_volume.sum())
    cost_ratio[high_margin] = rng.uniform(0.12, 0.20, high_margin.sum())
    cost_ratio[promo_dependent] = rng.uniform(0.40, 0.48, promo_dependent.sum())

    season = np.full(n, "AllYear", dtype=object)
    for season_name, members in ref.SEASONAL_ITEMS.items():
        season[np.isin(names, members)] = season_name
    is_seasonal = season != "AllYear"

    launch_offsets = rng.integers(0, (np.datetime64("2024-12-01") - np.datetime64("2016-01-01")).astype(int), n)
    launch_dates = np.datetime64("2016-01-01") + launch_offsets.astype("timedelta64[D]")
    for name, d in ref.LAUNCH_DATES_2025.items():
        launch_dates[names == name] = np.datetime64(d)
    late_launch = launch_dates >= np.datetime64(ref.LATE_LAUNCH_CUTOFF)

    discontinue_dates = np.full(n, END_DATE)
    for name, d in ref.DISCONTINUED.items():
        discontinue_dates[names == name] = np.datetime64(d)
    is_active = discontinue_dates == END_DATE

    first_day = np.clip((launch_dates - START_DATE).astype(int), 0, N_DAYS - 1)
    last_day = (discontinue_dates - START_DATE).astype(int)

    rating_mean = rng.normal(3.95, 0.2, n)
    rating_mean[high_margin] = rng.normal(4.3, 0.1, high_margin.sum())
    rating_mean[poor_rating] = rng.normal(2.2, 0.15, poor_rating.sum())

    item_ids = make_ids("ITEM", n, 4)
    descriptions = [f"{name} from our {cat_names[c].lower()} menu" for name, c in zip(names, item_cat)]
    menu_items = pd.DataFrame({
        "item_id": item_ids,
        "item_name": names,
        "category_id": categories["category_id"].to_numpy()[item_cat],
        "base_price": 0.0,
        "base_cost": 0.0,
        "description": descriptions,
        "is_active": is_active,
        "launch_date": dates_to_str(launch_dates),
        "is_seasonal": is_seasonal,
        "season": season,
    })
    ctx.menu = {
        "ids": item_ids,
        "n": n,
        "names": names,
        "cat_idx": item_cat,
        "cat_names": cat_names,
        "list_price": list_price,
        "cost_ratio": cost_ratio,
        "units": units,
        "portion": portion,
        "popularity": popularity,
        "season": season,
        "launch_dates": launch_dates,
        "first_day": first_day,
        "last_day": last_day,
        "late_launch": late_launch,
        "high_volume": high_volume,
        "high_margin": high_margin,
        "high_waste": high_waste,
        "poor_rating": poor_rating,
        "promo_dependent": promo_dependent,
        "rating_mean": rating_mean,
    }
    return categories, menu_items


def _round_price(p):
    step = np.where(p < 300, 5.0, 10.0)
    return np.round(p / step) * step


def build_pricing_history(ctx, menu_items):
    rng = ctx.rng
    m = ctx.menu
    n = m["n"]
    shock_items = set(ref.PRICE_SHOCKS)
    winter_start, winter_end = np.datetime64("2025-11-01"), np.datetime64("2025-03-01")
    first_inflation_dates = [np.datetime64("2025-02-01"), np.datetime64("2025-03-15")]
    rows = []

    for i in range(n):
        name = m["names"][i]
        launch = m["launch_dates"][i]
        disc = m["last_day"][i]
        events = []  # (date, price multiplier, cost multiplier, reason)
        if launch >= START_DATE:
            events.append((launch, 0.9, 1.0, "introductory price"))
            events.append((launch + np.timedelta64(21, "D"), 1 / 0.9, 1.0, "introductory offer ended"))
        else:
            start = max(PRICE_HISTORY_START, launch)
            if m["season"][i] == "Winter":
                events.append((max(start, np.datetime64("2024-11-01")), 1.05, 1.0, "seasonal"))
            else:
                events.append((start, 1.0, 1.0, "menu revision"))

        inflation_date = first_inflation_dates[m["cat_idx"][i] % 2]
        if launch < START_DATE:
            events.append((inflation_date, 1 + rng.uniform(0.03, 0.07), 1 + rng.uniform(0.04, 0.08), "inflation"))
        if rng.random() < 0.5 and launch < np.datetime64("2025-09-01"):
            events.append((np.datetime64("2025-09-01"), 1 + rng.uniform(0.02, 0.05),
                           1 + rng.uniform(0.02, 0.05), "inflation"))

        season = m["season"][i]
        if season == "Winter" and launch < START_DATE:
            events.append((winter_end, 1 / 1.05, 1.0, "seasonal"))
            events.append((winter_start, 1.05, 1.0, "seasonal"))
        elif season == "Summer":
            events.append((np.datetime64("2025-04-01"), 1.05, 1.0, "seasonal"))
            events.append((np.datetime64("2025-09-01"), 1 / 1.05, 1.0, "seasonal"))
        elif season == "Monsoon":
            events.append((np.datetime64("2025-07-01"), 1.05, 1.0, "seasonal"))
            events.append((np.datetime64("2025-10-01"), 1 / 1.05, 1.0, "seasonal"))

        if name in shock_items:
            d, pct = ref.PRICE_SHOCKS[name]
            events.append((np.datetime64(d), 1 + pct, 1.0, "menu repricing" if pct > 0 else "competitive pricing"))
        if m["promo_dependent"][i]:
            events.append((np.datetime64("2025-05-01"), 0.92, 1.0, "promotion-adjustment"))

        events.sort(key=lambda e: e[0])
        merged = []
        for e in events:
            if merged and merged[-1][0] == e[0]:
                d0, pm, cm, r0 = merged[-1]
                merged[-1] = (d0, pm * e[1], cm * e[2], r0 if r0 != "menu revision" else e[3])
            else:
                merged.append(e)

        disc_date = START_DATE + np.timedelta64(int(disc), "D")
        merged = [e for e in merged if e[0] <= disc_date]
        price_mult, cost_mult = 1.0, 1.0
        base_price = m["list_price"][i]
        base_cost = base_price * m["cost_ratio"][i]
        for k, (d, pm, cm, reason) in enumerate(merged):
            price_mult *= pm
            cost_mult *= cm
            if k + 1 < len(merged):
                end = merged[k + 1][0] - np.timedelta64(1, "D")
            elif disc_date < END_DATE:
                end = disc_date
            else:
                end = None
            rows.append((i, -1, float(_round_price(base_price * price_mult)),
                         round(base_cost * cost_mult, 2), d, end, reason))

    r = ctx.restaurants
    premium_locs = np.where(np.isin(r["types"], ["Mall", "Highway"]))[0]
    eligible_items = np.where(~m["late_launch"] & (m["last_day"] == N_DAYS - 1))[0]
    premium_items = rng.choice(eligible_items, 35, replace=False)
    premium_start = np.datetime64("2025-03-01")
    premium_set = set(premium_items.tolist())
    global_rows = [row for row in rows if row[0] in premium_set]
    for loc in premium_locs:
        for item, _, price, cost, start, end, _ in global_rows:
            if end is not None and end < premium_start:
                continue
            rows.append((item, loc, float(_round_price(price * 1.08)), cost, max(start, premium_start), end,
                         "location premium"))

    rows.sort(key=lambda row: (row[0], row[1], row[4]))
    item_idx = np.array([row[0] for row in rows])
    loc_idx = np.array([row[1] for row in rows])
    starts = np.array([row[4] for row in rows], dtype="datetime64[D]")
    ends = np.array([row[5] if row[5] is not None else np.datetime64("NaT") for row in rows], dtype="datetime64[D]")
    prices = np.array([row[2] for row in rows])
    costs = np.array([row[3] for row in rows])

    n_loc = r["n"]
    price_mat = np.full((N_DAYS, n_loc, n), np.nan)
    cost_mat = np.full((N_DAYS, n_loc, n), np.nan)
    start_day = np.clip((starts - START_DATE).astype(int), 0, N_DAYS)
    end_day = np.where(np.isnat(ends), N_DAYS - 1, np.clip((ends - START_DATE).astype(int), -1, N_DAYS - 1))
    for pass_locs in (False, True):
        for k in np.where((loc_idx >= 0) == pass_locs)[0]:
            s, e = start_day[k], end_day[k] + 1
            if e <= s:
                continue
            target = slice(None) if loc_idx[k] < 0 else loc_idx[k]
            price_mat[s:e, target, item_idx[k]] = prices[k]
            cost_mat[s:e, target, item_idx[k]] = costs[k]

    global_mask = loc_idx < 0
    last_global = pd.DataFrame({"item": item_idx[global_mask], "price": prices[global_mask],
                                "cost": costs[global_mask]}).groupby("item").last()
    menu_items["base_price"] = last_global["price"].reindex(range(n)).to_numpy()
    menu_items["base_cost"] = last_global["cost"].reindex(range(n)).to_numpy()

    global_price = price_mat[:, 0, :].copy()
    for k in np.where(global_mask)[0]:
        s, e = start_day[k], end_day[k] + 1
        if e > s:
            global_price[s:e, item_idx[k]] = prices[k]
    ref_price = global_price[m["first_day"], np.arange(n)]
    elasticity = np.full(n, 0.4)
    elasticity[np.isin(m["names"], list(shock_items))] = 2.2
    demand_effect = np.nan_to_num((global_price / ref_price) ** (-elasticity), nan=0.0)

    df = pd.DataFrame({
        "price_id": make_ids("PRC", len(rows), 5),
        "item_id": m["ids"][item_idx],
        "location_id": np.where(loc_idx >= 0, r["ids"][np.maximum(loc_idx, 0)], None),
        "price": prices,
        "cost": costs,
        "effective_start_date": dates_to_str(starts),
        "effective_end_date": np.where(np.isnat(ends), None, dates_to_str(np.where(np.isnat(ends), starts, ends))),
        "change_reason": [row[6] for row in rows],
    })
    ctx.pricing = {"price_mat": price_mat, "cost_mat": cost_mat, "demand_effect": demand_effect}
    return df


ARCHETYPES = ["loyal", "frequent", "promo", "churned", "new", "occasional"]
ARCHETYPE_SHARE = [0.06, 0.14, 0.12, 0.14, 0.10, 0.44]
ANNUAL_ORDER_RATE = np.array([26.0, 12.0, 5.0, 14.0, 10.0, 1.6])
LOYALTY_RATE = np.array([0.9, 0.55, 0.35, 0.3, 0.15, 0.12])
CHANNELS = ["Dine-in", "Takeaway", "Website", "App", "ThirdPartyDelivery"]


def build_customers(ctx):
    rng = ctx.rng
    n = int(round(BASE_VOLUMES["customers"] * ctx.scale))
    archetype = rng.choice(len(ARCHETYPES), n, p=ARCHETYPE_SHARE)

    def random_dates(lo, hi, size):
        span = (np.datetime64(hi) - np.datetime64(lo)).astype(int)
        return np.datetime64(lo) + rng.integers(0, span + 1, size).astype("timedelta64[D]")

    signup = random_dates("2022-01-01", "2024-12-31", n)
    recent = (rng.random(n) < 0.3) & np.isin(archetype, [1, 2, 5])
    signup[recent] = random_dates("2025-01-01", "2025-09-15", recent.sum())
    loyal = archetype == 0
    signup[loyal] = random_dates("2021-01-01", "2024-06-30", loyal.sum())
    new = archetype == 4
    signup[new] = random_dates("2025-10-01", "2025-12-20", new.sum())

    window_start = np.maximum((signup - START_DATE).astype(int), 0)
    window_end = np.full(n, N_DAYS - 1)
    churned = archetype == 3
    window_end[churned] = rng.integers(60, 240, churned.sum())

    r = ctx.restaurants
    has_home = rng.random(n) < 0.92
    home = rng.choice(r["n"], n, p=r["popularity"])

    age_group = rng.choice(["18-24", "25-34", "35-44", "45-54", "55+"], n, p=[0.22, 0.33, 0.22, 0.13, 0.10])
    young = np.isin(age_group, ["18-24", "25-34"])
    channel_p_young = [0.25, 0.12, 0.08, 0.32, 0.23]
    channel_p_older = [0.48, 0.20, 0.08, 0.12, 0.12]
    preferred = np.where(young, rng.choice(CHANNELS, n, p=channel_p_young),
                         rng.choice(CHANNELS, n, p=channel_p_older))

    ids = make_ids("CUST", n, 7)
    df = pd.DataFrame({
        "customer_id": ids,
        "signup_date": dates_to_str(signup),
        "age_group": age_group,
        "gender": rng.choice(["Male", "Female", "Undisclosed"], n, p=[0.55, 0.38, 0.07]),
        "home_location_id": np.where(has_home, r["ids"][home], None),
        "preferred_channel": preferred,
        "loyalty_member": rng.random(n) < LOYALTY_RATE[archetype],
    })
    ctx.customers = {
        "ids": ids,
        "n": n,
        "archetype": archetype,
        "window_start": window_start,
        "window_end": window_end,
        "has_home": has_home,
        "home": home,
        "preferred_channel": preferred,
    }
    return df


PROMO_TYPES = ["PercentOff", "FixedAmountOff", "BOGO", "ComboDeal"]


def build_promotions(ctx):
    m = ctx.menu
    r = ctx.restaurants
    n_promo = len(ref.PROMOTIONS)
    ids = make_ids("PROMO", n_promo, 3)
    active = np.zeros((n_promo, N_DAYS, r["n"]), dtype=bool)
    items_mask = np.zeros((n_promo, m["n"]), dtype=bool)
    promo_item_rows, promo_loc_rows = [], []

    for p, (key, name, ptype, value, start, end, min_order, item_spec, loc_spec, lift) in enumerate(ref.PROMOTIONS):
        if item_spec.get("all"):
            items_mask[p] = True
        for cat in item_spec.get("categories", []):
            items_mask[p] |= m["cat_idx"] == m["cat_names"].index(cat)
        items_mask[p] |= np.isin(m["names"], item_spec.get("items", []))
        start_day, end_day = to_day_index(start), to_day_index(end)
        items_mask[p] &= (m["first_day"] <= end_day) & (m["last_day"] >= start_day)

        if loc_spec.get("all"):
            loc_mask = np.ones(r["n"], dtype=bool)
        elif "types" in loc_spec:
            loc_mask = np.isin(r["types"], loc_spec["types"])
        else:
            loc_mask = np.isin(r["cities"], loc_spec["cities"])
        active[p, start_day:end_day + 1, :] = loc_mask

        promo_item_rows += [(ids[p], m["ids"][i]) for i in np.where(items_mask[p])[0]]
        promo_loc_rows += [(ids[p], r["ids"][j]) for j in np.where(loc_mask)[0]]

    promos = ref.PROMOTIONS
    df = pd.DataFrame({
        "promotion_id": ids,
        "promotion_name": [p[1] for p in promos],
        "promotion_type": [p[2] for p in promos],
        "discount_value": [float(p[3]) for p in promos],
        "start_date": [p[4] for p in promos],
        "end_date": [p[5] for p in promos],
        "min_order_value": [float(p[6]) if p[6] is not None else None for p in promos],
    })
    promo_items = pd.DataFrame(promo_item_rows, columns=["promotion_id", "item_id"])
    promo_locations = pd.DataFrame(promo_loc_rows, columns=["promotion_id", "location_id"])
    ctx.promotions = {
        "ids": ids,
        "n": n_promo,
        "keys": [p[0] for p in promos],
        "type_idx": np.array([PROMO_TYPES.index(p[2]) for p in promos]),
        "value": np.array([float(p[3]) for p in promos]),
        "min_order": np.array([float(p[6]) if p[6] is not None else 0.0 for p in promos]),
        "lift": np.array([p[9] for p in promos]),
        "active": active,
        "items_mask": items_mask,
        "trap": [p[0] for p in promos].index(ref.TRAP_PROMOTION),
    }
    return df, promo_items, promo_locations

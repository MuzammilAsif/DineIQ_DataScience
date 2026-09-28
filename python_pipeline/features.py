"""Pandas feature engineering, independent of the Spark pipeline.

Reads only the Step 2 cleaned CSVs in processed_data/ and recomputes the Step 3 item and
customer features with the same formulas (documented in documentation/dev_log.md, Step 3).
"""
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = ROOT / "processed_data"
CANCELLED = "Cancelled"
PEAK_HOURS = [(12, 14), (19, 22)]
# pandas dayofweek: Monday=0 ... Sunday=6. Weekend is Fri-Sat-Sun.
WEEKEND_DAYS = [4, 5, 6]
MIN_TREND_MONTHS = 3

DATES = {
    "Orders": ["order_datetime"], "Menu_Items": ["launch_date"], "Ratings": ["rating_date"],
    "Wastage": ["date"], "Pricing_History": ["effective_start_date", "effective_end_date"],
    "Customers": ["signup_date"],
}


def load_tables(names, processed_dir=PROCESSED_DIR):
    return {n: pd.read_csv(processed_dir / f"{n}.csv", parse_dates=DATES.get(n, []),
                           low_memory=False) for n in names}


def _div(num, den):
    num, den = pd.Series(num, dtype="float64"), pd.Series(den, dtype="float64")
    return num.where(den.notna() & (den != 0)) / den.where(den != 0)


def completed_lines(order_items, orders, as_of):
    lines = order_items.merge(orders, on="order_id", how="left", suffixes=("", "_order"))
    end = pd.Timestamp(as_of) + pd.Timedelta(days=1)
    lines = lines[(lines.order_datetime < end) & lines.order_status.notna()
                  & (lines.order_status != CANCELLED)].copy()
    lines["line_cost"] = lines.quantity * lines.unit_cost
    return lines


def monthly_slope(df, key, month_col, value_col):
    """Least-squares slope of value over month index, NaN under MIN_TREND_MONTHS months."""
    if df.empty:
        return pd.Series(dtype="float64")
    m = df[month_col].dt.year * 12 + df[month_col].dt.month
    d = df.assign(m=m.astype("float64"), v=df[value_col].astype("float64"))

    def slope(g):
        if len(g) < MIN_TREND_MONTHS:
            return np.nan
        var = ((g.m - g.m.mean()) ** 2).mean()
        return ((g.m - g.m.mean()) * (g.v - g.v.mean())).mean() / var if var else np.nan
    return d.groupby(key)[["m", "v"]].apply(slope)


def item_features(t, as_of):
    as_of = pd.Timestamp(as_of)
    lines = completed_lines(t["Order_Items"], t["Orders"], as_of)
    lines = lines[lines.item_id.notna()]
    weekend = lines.order_datetime.dt.dayofweek.isin(WEEKEND_DAYS)
    lines = lines.assign(
        weekend_qty=lines.quantity.where(weekend, 0),
        promo_rev=lines.line_total.where(lines.promotion_id.notna(), 0.0))

    g = lines.groupby("item_id")
    sales = pd.DataFrame({
        "item_revenue": g.line_total.sum(), "item_cost": g.line_cost.sum(),
        "total_quantity_sold": g.quantity.sum(), "weekend_quantity": g.weekend_qty.sum(),
        "order_frequency": g.order_id.nunique(), "promo_revenue": g.promo_rev.sum(),
        "discount_total": g.discount_amount.sum()})

    per_cust = (lines[lines.customer_id.notna()].groupby(["item_id", "customer_id"])
                .order_id.nunique())
    repeat = (per_cust > 1).groupby("item_id").mean().rename("repeat_purchase_rate")

    r = t["Ratings"]
    r = r[(r.rating_date <= as_of) & r.item_id.notna() & r.rating_value.notna()]
    avg_rating = r.groupby("item_id").rating_value.mean().rename("average_rating")
    monthly_r = (r.assign(month=r.rating_date.dt.to_period("M").dt.to_timestamp())
                 .groupby(["item_id", "month"]).rating_value.mean().reset_index())
    rating_trend = monthly_slope(monthly_r, "item_id", "month", "rating_value").rename("rating_trend")

    monthly_q = (lines.assign(month=lines.order_datetime.dt.to_period("M").dt.to_timestamp())
                 .groupby(["item_id", "month"]).quantity.sum().reset_index())
    sales_trend = monthly_slope(monthly_q, "item_id", "month", "quantity").rename("sales_trend")

    w = t["Wastage"]
    waste = (w[(w.date <= as_of) & w.item_id.notna()].groupby("item_id").cost_of_waste.sum()
             .rename("wastage_cost"))

    p = t["Pricing_History"]
    p = p[(p.effective_start_date <= as_of) & p.location_id.isna()].sort_values(
        ["item_id", "effective_start_date"])
    first = p.groupby("item_id").price.first()
    latest = p.groupby("item_id").price.last()
    price_change = (_div(latest - first, first) * 100).rename("price_change_percentage")
    price_change.index = first.index

    mi = t["Menu_Items"]
    items = (mi[mi.launch_date <= as_of]
             .merge(t["Menu_Categories"][["category_id", "category_name"]], on="category_id",
                    how="left")[["item_id", "item_name", "category_id", "category_name", "base_cost"]]
             .set_index("item_id"))
    df = items.join([sales, repeat, avg_rating, rating_trend, sales_trend, waste, price_change])
    zero = ["item_revenue", "item_cost", "total_quantity_sold", "order_frequency",
            "weekend_quantity", "promo_revenue", "discount_total", "wastage_cost"]
    df[zero] = df[zero].fillna(0)

    # wastage is logged in kg/l/pcs; cost / average unit cost converts it to portions
    avg_unit_cost = _div(df.item_cost, df.total_quantity_sold).fillna(df.base_cost.astype(float))
    avg_unit_cost.index = df.index
    wasted = pd.Series(_div(df.wastage_cost, avg_unit_cost).values, index=df.index)
    qty = df.total_quantity_sold
    n = len(df)
    out = pd.DataFrame({
        "item_name": df.item_name, "category_name": df.category_name,
        "item_revenue": df.item_revenue, "item_cost": df.item_cost,
        "contribution_margin": df.item_revenue - df.item_cost,
        "profit_percentage": _div(df.item_revenue - df.item_cost, df.item_revenue).values * 100,
        "total_quantity_sold": qty.astype("int64"),
        "item_popularity": (qty.rank(method="min") - 1) / (n - 1),
        "order_frequency": df.order_frequency.astype("int64"),
        "repeat_purchase_rate": df.repeat_purchase_rate,
        "average_rating": df.average_rating, "rating_trend": df.rating_trend,
        "weekend_order_ratio": _div(df.weekend_quantity, qty).values,
        "sales_trend": df.sales_trend, "wastage_cost": df.wastage_cost,
        "total_quantity_wasted": wasted,
        "wastage_percentage": _div(wasted, wasted + qty).values,
        "promotion_dependency": _div(df.promo_revenue, df.item_revenue).values,
        "discount_percentage": _div(df.discount_total, df.item_revenue + df.discount_total).values,
        "price_change_percentage": df.price_change_percentage,
    }, index=df.index)
    out.index.name = "item_id"
    return out.reset_index()


def customer_features(t, as_of):
    """Step 3 customer features plus promo_order_share and signup tenure for segmentation."""
    as_of = pd.Timestamp(as_of)
    o = t["Orders"]
    end = as_of + pd.Timedelta(days=1)
    o = o[(o.order_datetime < end) & (o.order_status != CANCELLED) & o.customer_id.notna()]
    g = o.groupby("customer_id")
    freq = g.order_id.nunique()
    monetary = g.total_amount.sum()
    df = pd.DataFrame({
        "customer_recency": (as_of - g.order_datetime.max().dt.normalize()).dt.days,
        "customer_frequency": freq.astype("int64"),
        "customer_monetary_value": monetary,
        "average_order_value": _div(monetary, freq).values,
        "promo_order_share": g.promotion_id.apply(lambda s: s.notna().mean()),
        "first_order_days": (as_of - g.order_datetime.min().dt.normalize()).dt.days,
    })
    c = t["Customers"].set_index("customer_id")
    df = df.join((as_of - c.signup_date).dt.days.rename("signup_days"))
    df.index.name = "customer_id"
    return df.reset_index()


def default_as_of(t):
    return t["Orders"].order_datetime.max().normalize()


if __name__ == "__main__":
    tables = load_tables(["Orders", "Order_Items", "Menu_Items", "Menu_Categories", "Ratings",
                          "Wastage", "Pricing_History", "Customers"])
    as_of = default_as_of(tables)
    print(as_of.date())
    print(item_features(tables, as_of).describe().T)
    print(customer_features(tables, as_of).describe().T)

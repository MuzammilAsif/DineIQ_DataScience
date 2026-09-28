import numpy as np
import pandas as pd

from common import make_ids

UNIT_VARIANTS = {
    "kg": ["kgs", "KG", "Kg", "kilogram"],
    "liters": ["ltr", "L", "litres", "Liters"],
    "pcs": ["pieces", "pc", "PCS", "Pcs"],
}
OUT_OF_RANGE_DATES = ["2031-07-22", "1900-01-01", "2027-11-05", "2099-12-31", "1970-01-01"]
MALFORMED_DATES = ["2025-13-07", "2025-02-30", "07/15/2025", "15-07-2025", "2025/06/31", "not_a_date", "20250612"]
MALFORMED_DATETIMES = ["2025-06-31 19:45:00", "2025-13-02 12:10:00", "14-06-2025 7:30 PM", "2025/08/14 25:61:00",
                       "unknown", "2025-02-29 13:00:00", "20250901T201500"]
OUT_OF_RANGE_RATINGS = [0, 6, 7, 10, -1, 55]


class DirtyInjector:
    def __init__(self, rng):
        self.rng = rng
        self.log = []
        self.used = {}

    def pick(self, table, df, rate, eligible=None):
        used = self.used.setdefault(table, np.zeros(len(df), dtype=bool))
        candidates = ~used if eligible is None else (~used & np.asarray(eligible))
        pool = np.where(candidates)[0]
        k = min(len(pool), max(1, int(round(len(df) * rate))))
        chosen = np.sort(self.rng.choice(pool, k, replace=False))
        used[chosen] = True
        return chosen

    def record(self, table, rule, key_col, df, rows, column, before, after):
        keys = df[key_col].to_numpy()[rows]
        for key, b, a in zip(keys, before, after):
            self.log.append((table, rule, key, column, b, a))

    def set_values(self, table, rule, key_col, df, column, rows, new_values):
        before = df[column].to_numpy()[rows]
        col_pos = df.columns.get_loc(column)
        df.iloc[rows, col_pos] = new_values
        self.record(table, rule, key_col, df, rows, column, before, new_values)

    def null_values(self, table, key_col, df, column, rate):
        rows = self.pick(table, df, rate)
        self.set_values(table, "missing_value", key_col, df, column, rows, [None] * len(rows))

    def orphan_fk(self, table, key_col, df, column, prefix, width, parent_count, rate):
        rows = self.pick(table, df, rate)
        fake_numbers = parent_count + self.rng.integers(1, 10 ** width - parent_count, len(rows))
        orphan_ids = [f"{prefix}{str(n).zfill(width)}" for n in fake_numbers]
        self.set_values(table, "orphan_foreign_key", key_col, df, column, rows, orphan_ids)

    def invalid_dates(self, table, key_col, df, column, rate, with_time=False):
        rows = self.pick(table, df, rate)
        malformed = MALFORMED_DATETIMES if with_time else MALFORMED_DATES
        out_of_range = OUT_OF_RANGE_DATES
        values = []
        for is_malformed in self.rng.random(len(rows)) < 0.5:
            if is_malformed:
                values.append(str(self.rng.choice(malformed)))
            else:
                d = str(self.rng.choice(out_of_range))
                values.append(f"{d} {self.rng.integers(10, 23):02d}:{self.rng.integers(0, 60):02d}:00" if with_time else d)
        self.set_values(table, "invalid_date", key_col, df, column, rows, values)

    def invalid_prices(self, table, key_col, df, column, rate):
        rows = self.pick(table, df, rate)
        current = df[column].to_numpy()[rows].astype(float)
        new = np.where(self.rng.random(len(rows)) < 0.5, 0.0, -np.round(current, 2))
        self.set_values(table, "invalid_price", key_col, df, column, rows, new)

    def inconsistent_units(self, table, key_col, df, rate):
        rows = self.pick(table, df, rate)
        current = df["unit"].to_numpy()[rows]
        new = [str(self.rng.choice(UNIT_VARIANTS[u])) for u in current]
        self.set_values(table, "inconsistent_unit", key_col, df, "unit", rows, new)

    def duplicate_rows(self, table, key_col, df, rate):
        rows = self.pick(table, df, rate)
        self.record(table, "duplicate_row", key_col, df, rows, "", [""] * len(rows), [""] * len(rows))
        dupes = df.iloc[rows]
        combined = pd.concat([df, dupes], ignore_index=True)
        return combined.sort_values(key_col, kind="stable").reset_index(drop=True)

    def log_frame(self):
        return pd.DataFrame(self.log, columns=["table", "rule", "row_key", "column", "original_value",
                                               "injected_value"])


def inject(tables, ctx):
    rng = np.random.default_rng(ctx.rng.integers(0, 2**32))
    inj = DirtyInjector(rng)
    n_customers = ctx.customers["n"]
    n_loc = ctx.restaurants["n"]
    n_items = ctx.menu["n"]
    n_orders = ctx.orders["n"]

    customers = tables["Customers"]
    inj.invalid_dates("Customers", "customer_id", customers, "signup_date", 0.005)
    inj.orphan_fk("Customers", "customer_id", customers, "home_location_id", "LOC", 4, n_loc, 0.005)

    menu = tables["Menu_Items"]
    inj.invalid_prices("Menu_Items", "item_id", menu, "base_price", 0.02)

    pricing = tables["Pricing_History"]
    inj.invalid_prices("Pricing_History", "price_id", pricing, "price", 0.015)

    orders = tables["Orders"]
    orders["customer_id"] = orders["customer_id"].astype(object)
    orders["order_datetime"] = orders["order_datetime"].astype(object)
    inj.null_values("Orders", "order_id", orders, "customer_id", 0.01)
    inj.orphan_fk("Orders", "order_id", orders, "customer_id", "CUST", 7, n_customers, 0.005)
    inj.orphan_fk("Orders", "order_id", orders, "location_id", "LOC", 4, n_loc, 0.003)
    inj.invalid_dates("Orders", "order_id", orders, "order_datetime", 0.005, with_time=True)
    rows = inj.pick("Orders", orders, 0.005, eligible=orders["subtotal"].to_numpy() > 0)
    subtotal = orders["subtotal"].to_numpy()[rows]
    inj.set_values("Orders", "discount_exceeds_subtotal", "order_id", orders, "discount_amount", rows,
                   np.round(subtotal * rng.uniform(1.1, 2.5, len(rows)), 2))
    tables["Orders"] = inj.duplicate_rows("Orders", "order_id", orders, 0.01)

    items = tables["Order_Items"]
    items["item_id"] = items["item_id"].astype(object)
    inj.null_values("Order_Items", "order_item_id", items, "item_id", 0.005)
    rows = inj.pick("Order_Items", items, 0.005)
    qty = items["quantity"].to_numpy()[rows]
    inj.set_values("Order_Items", "negative_quantity", "order_item_id", items, "quantity", rows, -qty)
    inj.invalid_prices("Order_Items", "order_item_id", items, "unit_price", 0.003)
    inj.orphan_fk("Order_Items", "order_item_id", items, "order_id", "ORD", 8, n_orders, 0.003)
    inj.orphan_fk("Order_Items", "order_item_id", items, "item_id", "ITEM", 4, n_items, 0.003)
    tables["Order_Items"] = inj.duplicate_rows("Order_Items", "order_item_id", items, 0.01)

    ratings = tables["Ratings"]
    ratings["rating_value"] = ratings["rating_value"].astype("Int64")
    ratings["rating_date"] = ratings["rating_date"].astype(object)
    inj.null_values("Ratings", "rating_id", ratings, "rating_value", 0.015)
    rows = inj.pick("Ratings", ratings, 0.01)
    inj.set_values("Ratings", "rating_out_of_range", "rating_id", ratings, "rating_value", rows,
                   rng.choice(OUT_OF_RANGE_RATINGS, len(rows)))
    inj.invalid_dates("Ratings", "rating_id", ratings, "rating_date", 0.005)
    inj.orphan_fk("Ratings", "rating_id", ratings, "location_id", "LOC", 4, n_loc, 0.005)
    inj.orphan_fk("Ratings", "rating_id", ratings, "customer_id", "CUST", 7, n_customers, 0.003)

    inventory = tables["Inventory"]
    inj.inconsistent_units("Inventory", "inventory_id", inventory, 0.01)
    inj.orphan_fk("Inventory", "inventory_id", inventory, "item_id", "ITEM", 4, n_items, 0.003)

    wastage = tables["Wastage"]
    wastage["item_id"] = wastage["item_id"].astype(object)
    wastage["date"] = wastage["date"].astype(object)
    rows = inj.pick("Wastage", wastage, 0.01)
    prepared = ctx.inventory["wastage_prepared"][rows]
    is_pcs = wastage["unit"].to_numpy()[rows] == "pcs"
    impossible = prepared * rng.uniform(1.5, 4.0, len(rows)) + 1
    impossible = np.where(is_pcs, np.ceil(impossible), np.round(impossible, 2))
    inj.set_values("Wastage", "wastage_exceeds_prepared", "wastage_id", wastage, "quantity_wasted", rows, impossible)
    inj.inconsistent_units("Wastage", "wastage_id", wastage, 0.02)
    inj.invalid_dates("Wastage", "wastage_id", wastage, "date", 0.005)
    inj.null_values("Wastage", "wastage_id", wastage, "item_id", 0.005)
    inj.orphan_fk("Wastage", "wastage_id", wastage, "location_id", "LOC", 4, n_loc, 0.005)

    log = inj.log_frame()
    table_rows = {name: len(df) for name, df in tables.items()}
    summary = (log.groupby(["table", "rule", "column"], sort=False).size().rename("rows_affected").reset_index())
    summary["table_rows"] = summary["table"].map(table_rows)
    summary["pct_of_table"] = (100 * summary["rows_affected"] / summary["table_rows"]).round(3)
    return log, summary

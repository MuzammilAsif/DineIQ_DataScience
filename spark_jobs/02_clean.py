"""Step 2.4 — cleaning: apply the rules in documentation/data_quality_rules.md.

Writes cleaned tables to processed_data/<Table>.csv, quarantined rows to
processed_data/quarantine/<Table>.csv (with quarantine_reason), and every
correction/drop/quarantine decision to reports/cleaning_log.csv.

Orphan-FK checks use a left join against the distinct parent keys (not a collected
Python set fed into .isin()) — with parent tables up to ~195k rows, a giant IN-list
expression makes Catalyst's analyzer/driver choke; a join does not.

Run: .venv/bin/python spark_jobs/02_clean.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import DATE_FMT, SCHEMAS, TIMESTAMP_FMT, load_table  # noqa: E402
from spark_utils import (  # noqa: E402
    JobLogger,
    PROCESSED_DIR,
    QUARANTINE_DIR,
    RAW_DIR,
    REPORTS_DIR,
    ensure_dirs,
    get_spark,
)

VALID_RANGES = {
    "Customers": ("signup_date", "2021-01-01", "2025-12-20"),
    "Orders": ("order_datetime", "2025-01-01 00:00:00", "2025-12-31 23:59:59"),
    "Ratings": ("rating_date", "2025-01-01", "2026-01-10"),
    "Wastage": ("date", "2025-01-01", "2025-12-31"),
}

UNIT_MAP = {
    "pcs": "pcs", "pieces": "pcs", "piece": "pcs", "pc": "pcs", "Pcs": "pcs", "PCS": "pcs",
    "kg": "kg", "kgs": "kg", "KG": "kg", "Kg": "kg", "kilogram": "kg", "kilograms": "kg",
    "liters": "liters", "litre": "liters", "litres": "liters", "ltr": "liters",
    "L": "liters", "l": "liters", "Liters": "liters", "liter": "liters",
}

DIMENSION_TABLES = ["Restaurants", "Menu_Categories", "Promotions", "Promotion_Items", "Promotion_Locations"]

cleaning_log_rows = []  # dicts: table, row_id, id_column, issue, action, detail


def log_action(table, id_column, row_id, issue, action, detail=""):
    cleaning_log_rows.append(
        {"table": table, "id_column": id_column, "row_id": str(row_id), "issue": issue,
         "action": action, "detail": detail}
    )


def log_bulk(table, id_column, ids_df, issue, action, detail=""):
    """ids_df: single-column DataFrame of row ids (collected — quarantine/duplicate
    counts are small relative to table size, safe to collect for logging)."""
    for row in ids_df.collect():
        log_action(table, id_column, row[0], issue, action, detail)


def with_orphan_flag(df, child_col, parent_df, parent_col, flag_name):
    """Adds a boolean column flag_name: True where child_col is non-null and has no
    matching row in parent_df.parent_col. Implemented as a left join, not isin()."""
    parent_keys = parent_df.select(F.col(parent_col).alias("_pk")).distinct()
    joined = df.join(parent_keys, df[child_col] == parent_keys["_pk"], "left")
    return joined.withColumn(flag_name, F.col(child_col).isNotNull() & F.col("_pk").isNull()).drop("_pk")


def dedupe(df, keys, table, id_column, logger):
    w = Window.partitionBy(*keys).orderBy(F.monotonically_increasing_id())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    dropped = ranked.filter(F.col("_rn") > 1)
    n = dropped.count()
    if n:
        log_bulk(table, id_column, dropped.select(id_column).distinct().limit(5000), "duplicate_row",
                  "dropped_duplicate", f"kept first of {n} extra duplicate rows total")
        logger.log(f"{table}: dropped {n} duplicate rows (key={keys})")
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def quarantine_and_split(df, id_column, rule_conditions, table, logger):
    """rule_conditions: ordered list of (rule_name, condition_col). First match wins."""
    reason = F.lit(None).cast("string")
    for rule_name, cond in reversed(rule_conditions):
        reason = F.when(cond, F.lit(rule_name)).otherwise(reason)
    tagged = df.withColumn("quarantine_reason", reason)
    quarantined = tagged.filter(F.col("quarantine_reason").isNotNull())
    clean = tagged.filter(F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    n = quarantined.count()
    if n:
        counts = quarantined.groupBy("quarantine_reason").count().collect()
        for r in counts:
            logger.log(f"{table}: quarantined {r['count']} rows for '{r['quarantine_reason']}'")
        log_bulk(table, id_column, quarantined.select(id_column, "quarantine_reason").limit(20000),
                  "quarantine", "quarantined")
    return clean, quarantined


def _write_single_csv(df, out_path: Path):
    """Write a Spark DataFrame as a single named CSV file (not a part-file directory).

    Pins dateFormat/timestampFormat to match schemas.DATE_FMT/TIMESTAMP_FMT — Spark's
    CSV writer defaults to ISO-8601 with a 'T'/'Z' for TimestampType, which the loader's
    explicit timestampFormat ("yyyy-MM-dd HH:mm:ss") then fails to parse back on reload,
    silently nulling every order_datetime value (and any partitioning derived from it).
    """
    tmp_dir = out_path.with_suffix(".tmp_write_dir")
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("dateFormat", DATE_FMT)
        .option("timestampFormat", TIMESTAMP_FMT)
        .csv(str(tmp_dir))
    )
    part_file = next(tmp_dir.glob("part-*.csv"))
    if out_path.exists():
        out_path.unlink()
    part_file.rename(out_path)
    for leftover in tmp_dir.iterdir():
        leftover.unlink()
    tmp_dir.rmdir()


def write_clean(df, table, logger):
    out = PROCESSED_DIR / f"{table}.csv"
    _write_single_csv(df, out)
    logger.log(f"wrote cleaned {table} -> {out}")


def write_quarantine(df, table, logger):
    if df.rdd.isEmpty():
        return
    # Drop internal helper flag columns (all prefixed "_") added during rule
    # evaluation — quarantine files should carry only the original row + reason.
    keep = [c for c in df.columns if not c.startswith("_") or c == "quarantine_reason"]
    df = df.select(*keep)
    out = QUARANTINE_DIR / f"{table}.csv"
    _write_single_csv(df, out)
    logger.log(f"wrote quarantined {table} -> {out}")


def main():
    ensure_dirs(PROCESSED_DIR, QUARANTINE_DIR, REPORTS_DIR)
    logger = JobLogger("clean")
    spark = get_spark("dineiq-clean")
    spark.sparkContext.setLogLevel("WARN")

    raw = {}
    before_counts = {}
    with logger.timer("reload raw tables"):
        for name in SCHEMAS:
            raw[name] = load_table(spark, name, RAW_DIR).cache()
            before_counts[name] = raw[name].count()
            logger.log(f"{name}: {before_counts[name]} raw rows")

    cleaned = {}
    after_counts = {}

    # ---- dimension tables: pass through as-is ----
    for name in DIMENSION_TABLES:
        cleaned[name] = raw[name]

    # ---- Menu_Items: correct invalid base_price from latest chain-wide Pricing_History ----
    with logger.timer("clean Menu_Items"):
        chain_prices = raw["Pricing_History"].filter(F.col("location_id").isNull())
        w = Window.partitionBy("item_id").orderBy(F.col("effective_start_date").desc())
        latest_price = (
            chain_prices.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select("item_id", F.col("price").alias("_latest_price"))
        )
        mi = raw["Menu_Items"].join(latest_price, "item_id", "left")
        correctable = mi.filter((F.col("base_price") <= 0) & F.col("_latest_price").isNotNull() & (F.col("_latest_price") > 0))
        for row in correctable.select("item_id", "base_price", "_latest_price").collect():
            log_action("Menu_Items", "item_id", row["item_id"], "invalid_price",
                       "corrected", f"{row['base_price']} -> {row['_latest_price']} (latest chain-wide Pricing_History)")
        mi_fixed = mi.withColumn(
            "base_price",
            F.when((F.col("base_price") <= 0) & F.col("_latest_price").isNotNull() & (F.col("_latest_price") > 0),
                   F.col("_latest_price")).otherwise(F.col("base_price")),
        ).drop("_latest_price")
        mi_fixed = with_orphan_flag(mi_fixed, "category_id", raw["Menu_Categories"], "category_id", "_orphan_category")
        mi_clean, mi_quarantine = quarantine_and_split(
            mi_fixed, "item_id",
            [("invalid_price", F.col("base_price") <= 0),
             ("orphan_foreign_key", F.col("_orphan_category"))],
            "Menu_Items", logger,
        )
        cleaned["Menu_Items"] = mi_clean.drop("_orphan_category").cache()
        logger.log(f"Menu_Items cleaned: {cleaned['Menu_Items'].count()} rows")
        write_quarantine(mi_quarantine, "Menu_Items", logger)

    # ---- Pricing_History: invalid price -> quarantine; orphan item_id/location_id -> quarantine ----
    with logger.timer("clean Pricing_History"):
        ph = with_orphan_flag(raw["Pricing_History"], "item_id", cleaned["Menu_Items"], "item_id", "_orphan_item")
        ph = with_orphan_flag(ph, "location_id", raw["Restaurants"], "location_id", "_orphan_loc")
        ph_clean, ph_quarantine = quarantine_and_split(
            ph, "price_id",
            [("invalid_price", F.col("price") <= 0),
             ("orphan_foreign_key", F.col("_orphan_item")),
             ("orphan_foreign_key", F.col("_orphan_loc"))],
            "Pricing_History", logger,
        )
        cleaned["Pricing_History"] = ph_clean.drop("_orphan_item", "_orphan_loc").cache()
        logger.log(f"Pricing_History cleaned: {cleaned['Pricing_History'].count()} rows")
        write_quarantine(ph_quarantine, "Pricing_History", logger)

    # ---- Customers: invalid signup_date -> quarantine; orphan home_location_id -> quarantine ----
    with logger.timer("clean Customers"):
        col, lo, hi = VALID_RANGES["Customers"]
        cust = with_orphan_flag(raw["Customers"], "home_location_id", raw["Restaurants"], "location_id", "_orphan_home")
        cust_clean, cust_quarantine = quarantine_and_split(
            cust, "customer_id",
            [("invalid_date", F.col(col).isNull() | (F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))),
             ("invalid_location_reference", F.col("_orphan_home"))],
            "Customers", logger,
        )
        cleaned["Customers"] = cust_clean.drop("_orphan_home").cache()
        logger.log(f"Customers cleaned: {cleaned['Customers'].count()} rows")
        write_quarantine(cust_quarantine, "Customers", logger)

    # ---- Orders: dedupe, then missing/invalid-date/discount/orphan -> quarantine ----
    with logger.timer("clean Orders"):
        o_dd = dedupe(raw["Orders"], ["order_id"], "Orders", "order_id", logger)
        col, lo, hi = VALID_RANGES["Orders"]
        o_dd = with_orphan_flag(o_dd, "customer_id", cleaned["Customers"], "customer_id", "_orphan_cust")
        o_dd = with_orphan_flag(o_dd, "location_id", raw["Restaurants"], "location_id", "_orphan_loc")
        o_clean, o_quarantine = quarantine_and_split(
            o_dd, "order_id",
            [("missing_value", F.col("customer_id").isNull()),
             ("invalid_date", F.col(col).isNull() | (F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))),
             ("discount_exceeds_subtotal", F.col("discount_amount") > F.col("subtotal")),
             ("orphan_foreign_key", F.col("_orphan_cust")),
             ("invalid_restaurant_id", F.col("_orphan_loc"))],
            "Orders", logger,
        )
        cleaned["Orders"] = o_clean.drop("_orphan_cust", "_orphan_loc").cache()
        logger.log(f"Orders cleaned: {cleaned['Orders'].count()} rows")
        write_quarantine(o_quarantine, "Orders", logger)

    # ---- Order_Items: dedupe, correct/quarantine price, quarantine missing/negative/orphan ----
    with logger.timer("clean Order_Items"):
        oi_dd = dedupe(raw["Order_Items"], ["order_id", "item_id"], "Order_Items", "order_item_id", logger)

        orders_ctx = cleaned["Orders"].select("order_id", "location_id", "order_datetime")
        oi_ctx = oi_dd.join(orders_ctx, "order_id", "left")

        loc_prices = cleaned["Pricing_History"].filter(F.col("location_id").isNotNull()).select(
            F.col("item_id").alias("_p_item"), F.col("location_id").alias("_p_loc"),
            F.col("price").alias("_p_price"), F.col("effective_start_date").alias("_p_start"),
            F.col("effective_end_date").alias("_p_end"),
        )
        chain_prices2 = cleaned["Pricing_History"].filter(F.col("location_id").isNull()).select(
            F.col("item_id").alias("_c_item"), F.col("price").alias("_c_price"),
            F.col("effective_start_date").alias("_c_start"), F.col("effective_end_date").alias("_c_end"),
        )

        bad_price = oi_ctx.filter(F.col("unit_price") <= 0)
        order_date = F.to_date(F.col("order_datetime"))

        with_loc = bad_price.join(
            loc_prices,
            (bad_price.item_id == loc_prices._p_item) & (bad_price.location_id == loc_prices._p_loc)
            & (order_date >= loc_prices._p_start) & (order_date <= F.coalesce(loc_prices._p_end, F.lit("9999-12-31"))),
            "left",
        )
        w2 = Window.partitionBy("order_item_id").orderBy(F.col("_p_price").isNull().asc())
        with_loc = with_loc.withColumn("_rn", F.row_number().over(w2)).filter(F.col("_rn") == 1).drop("_rn")

        with_chain = with_loc.join(
            chain_prices2,
            (with_loc.item_id == chain_prices2._c_item)
            & (order_date >= chain_prices2._c_start) & (order_date <= F.coalesce(chain_prices2._c_end, F.lit("9999-12-31"))),
            "left",
        )
        w3 = Window.partitionBy("order_item_id").orderBy(F.col("_c_price").isNull().asc())
        with_chain = with_chain.withColumn("_rn", F.row_number().over(w3)).filter(F.col("_rn") == 1).drop("_rn")

        corrected = with_chain.withColumn("_corrected_price", F.coalesce(F.col("_p_price"), F.col("_c_price")))
        for row in corrected.filter(F.col("_corrected_price").isNotNull()).select(
            "order_item_id", "unit_price", "_corrected_price"
        ).collect():
            log_action("Order_Items", "order_item_id", row["order_item_id"], "invalid_price",
                       "corrected", f"{row['unit_price']} -> {row['_corrected_price']} (Pricing_History lookup)")

        price_fix = corrected.select("order_item_id", "_corrected_price")
        oi_ctx = oi_ctx.join(price_fix, "order_item_id", "left")
        oi_ctx = oi_ctx.withColumn(
            "unit_price",
            F.when(F.col("unit_price") <= 0, F.col("_corrected_price")).otherwise(F.col("unit_price")),
        ).withColumn(
            "line_total",
            F.when(F.col("_corrected_price").isNotNull(),
                   F.col("quantity") * F.col("unit_price") - F.col("discount_amount")).otherwise(F.col("line_total")),
        ).drop("_corrected_price")

        oi_ctx = with_orphan_flag(oi_ctx, "order_id", cleaned["Orders"], "order_id", "_orphan_order")
        oi_ctx = with_orphan_flag(oi_ctx, "item_id", cleaned["Menu_Items"], "item_id", "_orphan_item")

        oi_clean, oi_quarantine = quarantine_and_split(
            oi_ctx, "order_item_id",
            [("missing_value", F.col("item_id").isNull()),
             ("negative_quantity", F.col("quantity") < 0),
             ("invalid_price", F.col("unit_price").isNull() | (F.col("unit_price") <= 0)),
             ("orphan_foreign_key", F.col("_orphan_order")),
             ("orphan_foreign_key", F.col("_orphan_item"))],
            "Order_Items", logger,
        )
        cleaned["Order_Items"] = oi_clean.drop("location_id", "order_datetime", "_orphan_order", "_orphan_item").cache()
        logger.log(f"Order_Items cleaned: {cleaned['Order_Items'].count()} rows")
        write_quarantine(oi_quarantine.drop("location_id", "order_datetime"), "Order_Items", logger)

    # ---- Ratings: missing rating_value/invalid rating/invalid date/orphan -> quarantine ----
    with logger.timer("clean Ratings"):
        col, lo, hi = VALID_RANGES["Ratings"]
        rt = with_orphan_flag(raw["Ratings"], "location_id", raw["Restaurants"], "location_id", "_orphan_loc")
        rt = with_orphan_flag(rt, "customer_id", cleaned["Customers"], "customer_id", "_orphan_cust")
        rt = with_orphan_flag(rt, "order_id", cleaned["Orders"], "order_id", "_orphan_order")
        rt = with_orphan_flag(rt, "item_id", cleaned["Menu_Items"], "item_id", "_orphan_item")
        r_clean, r_quarantine = quarantine_and_split(
            rt, "rating_id",
            [("missing_value", F.col("rating_value").isNull()),
             ("invalid_rating", (F.col("rating_value") < 1) | (F.col("rating_value") > 5)),
             ("invalid_date", F.col(col).isNull() | (F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))),
             ("invalid_restaurant_id", F.col("_orphan_loc")),
             ("orphan_foreign_key", F.col("_orphan_cust")),
             ("orphan_foreign_key", F.col("_orphan_order")),
             ("orphan_foreign_key", F.col("_orphan_item"))],
            "Ratings", logger,
        )
        cleaned["Ratings"] = r_clean.drop("_orphan_loc", "_orphan_cust", "_orphan_order", "_orphan_item").cache()
        logger.log(f"Ratings cleaned: {cleaned['Ratings'].count()} rows")
        write_quarantine(r_quarantine, "Ratings", logger)

    # ---- Inventory: normalize unit, quarantine orphan item_id/location_id ----
    with logger.timer("clean Inventory"):
        map_expr = F.create_map([F.lit(x) for pair in UNIT_MAP.items() for x in pair])
        inv = raw["Inventory"]
        normalized = inv.withColumn("_norm_unit", map_expr[F.col("unit")])
        changed = normalized.filter(F.col("_norm_unit").isNotNull() & (F.col("_norm_unit") != F.col("unit")))
        for row in changed.select("inventory_id", "unit", "_norm_unit").collect():
            log_action("Inventory", "inventory_id", row["inventory_id"], "inconsistent_unit",
                       "normalized", f"{row['unit']} -> {row['_norm_unit']}")
        inv_fixed = normalized.withColumn(
            "unit", F.coalesce(F.col("_norm_unit"), F.col("unit"))
        ).drop("_norm_unit")

        inv_fixed = with_orphan_flag(inv_fixed, "item_id", cleaned["Menu_Items"], "item_id", "_orphan_item")
        inv_fixed = with_orphan_flag(inv_fixed, "location_id", raw["Restaurants"], "location_id", "_orphan_loc")
        inv_clean, inv_quarantine = quarantine_and_split(
            inv_fixed, "inventory_id",
            [("orphan_foreign_key", F.col("_orphan_item")),
             ("invalid_restaurant_id", F.col("_orphan_loc"))],
            "Inventory", logger,
        )
        cleaned["Inventory"] = inv_clean.drop("_orphan_item", "_orphan_loc").cache()
        logger.log(f"Inventory cleaned: {cleaned['Inventory'].count()} rows")
        write_quarantine(inv_quarantine, "Inventory", logger)

    # ---- Wastage: normalize unit, missing item_id/exceeds-prepared/invalid date/orphan -> quarantine ----
    with logger.timer("clean Wastage"):
        map_expr = F.create_map([F.lit(x) for pair in UNIT_MAP.items() for x in pair])
        wst = raw["Wastage"]
        normalized = wst.withColumn("_norm_unit", map_expr[F.col("unit")])
        changed = normalized.filter(F.col("_norm_unit").isNotNull() & (F.col("_norm_unit") != F.col("unit")))
        for row in changed.select("wastage_id", "unit", "_norm_unit").collect():
            log_action("Wastage", "wastage_id", row["wastage_id"], "inconsistent_unit",
                       "normalized", f"{row['unit']} -> {row['_norm_unit']}")
        wst_fixed = normalized.withColumn(
            "unit", F.coalesce(F.col("_norm_unit"), F.col("unit"))
        ).drop("_norm_unit")

        inv_ctx = cleaned["Inventory"].select(
            F.col("item_id").alias("_i_item"), F.col("location_id").alias("_i_loc"),
            F.col("date").alias("_i_date"), F.col("prepared_quantity"), F.col("consumed_stock"),
        )
        wst_ctx = wst_fixed.join(
            inv_ctx,
            (wst_fixed.item_id == inv_ctx._i_item) & (wst_fixed.location_id == inv_ctx._i_loc)
            & (wst_fixed.date == inv_ctx._i_date),
            "left",
        )
        exceeds = F.col("quantity_wasted") > (F.col("prepared_quantity") - F.col("consumed_stock"))
        wst_ctx = wst_ctx.withColumn("_exceeds", exceeds)
        for row in wst_ctx.filter(F.col("_exceeds")).select(
            "wastage_id", "quantity_wasted", "prepared_quantity", "consumed_stock"
        ).collect():
            avail = (row["prepared_quantity"] or 0) - (row["consumed_stock"] or 0)
            log_action("Wastage", "wastage_id", row["wastage_id"], "wastage_exceeds_prepared",
                       "quarantined", f"wasted={row['quantity_wasted']} available={avail} "
                       f"excess={row['quantity_wasted'] - avail}")

        col, lo, hi = VALID_RANGES["Wastage"]
        wst_ctx = wst_ctx.drop("_i_item", "_i_loc", "_i_date", "prepared_quantity", "consumed_stock")
        wst_ctx = with_orphan_flag(wst_ctx, "item_id", cleaned["Menu_Items"], "item_id", "_orphan_item")
        wst_ctx = with_orphan_flag(wst_ctx, "location_id", raw["Restaurants"], "location_id", "_orphan_loc")
        wst_clean, wst_quarantine = quarantine_and_split(
            wst_ctx, "wastage_id",
            [("missing_value", F.col("item_id").isNull()),
             ("wastage_exceeds_prepared", F.col("_exceeds")),
             ("invalid_date", F.col(col).isNull() | (F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))),
             ("invalid_restaurant_id", F.col("_orphan_loc")),
             ("orphan_foreign_key", F.col("_orphan_item"))],
            "Wastage", logger,
        )
        cleaned["Wastage"] = wst_clean.drop("_orphan_item", "_orphan_loc", "_exceeds").cache()
        logger.log(f"Wastage cleaned: {cleaned['Wastage'].count()} rows")
        write_quarantine(wst_quarantine, "Wastage", logger)

    # ---- write cleaned dimension + fact tables, row counts, cleaning log ----
    with logger.timer("write cleaned outputs"):
        for name, df in cleaned.items():
            df.cache()
            after_counts[name] = df.count()
            write_clean(df, name, logger)

    with logger.timer("write cleaning log"):
        import csv as csv_mod
        log_path = REPORTS_DIR / "cleaning_log.csv"
        with open(log_path, "w", newline="") as fh:
            writer = csv_mod.DictWriter(fh, fieldnames=["table", "id_column", "row_id", "issue", "action", "detail"])
            writer.writeheader()
            writer.writerows(cleaning_log_rows)
        logger.log(f"wrote {log_path} with {len(cleaning_log_rows)} entries")

    with logger.timer("write before/after row counts"):
        counts_path = REPORTS_DIR / "cleaning_row_counts.csv"
        with open(counts_path, "w", newline="") as fh:
            fh.write("table,rows_before,rows_after,rows_removed\n")
            for name in SCHEMAS:
                b = before_counts.get(name, 0)
                a = after_counts.get(name, b)
                fh.write(f"{name},{b},{a},{b - a}\n")
        logger.log(f"wrote {counts_path}")

    logger.log("cleaning job complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

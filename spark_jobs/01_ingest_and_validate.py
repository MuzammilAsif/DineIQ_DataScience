"""Step 2.1-2.3 — ingestion, schema inference demo, data-type validation demo,
large/multi-file ingestion demo, referential integrity checks, data quality report.

Run: .venv/bin/python spark_jobs/01_ingest_and_validate.py
"""
import csv
import sys
from pathlib import Path

import pyspark.sql.functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import SCHEMAS, load_table, load_table_raw_strings  # noqa: E402
from spark_utils import (  # noqa: E402
    JobLogger,
    RAW_DIR,
    REPORTS_DIR,
    ensure_dirs,
    get_spark,
)

TABLES = list(SCHEMAS.keys())

# Business-valid ranges per documentation/data_dictionary.md. Bounds are inclusive strings
# compared lexicographically, which is safe for ISO dates/timestamps.
VALID_RANGES = {
    ("Customers", "signup_date"): ("2021-01-01", "2025-12-20"),
    ("Orders", "order_datetime"): ("2025-01-01 00:00:00", "2025-12-31 23:59:59"),
    ("Ratings", "rating_date"): ("2025-01-01", "2026-01-10"),
    ("Wastage", "date"): ("2025-01-01", "2025-12-31"),
}

# Columns where the injected corruption includes malformed (non-parseable) values,
# used for the typed-vs-raw-string cast diff that demonstrates data-type validation.
TYPE_VALIDATION_TARGETS = {
    "Customers": ["signup_date"],
    "Orders": ["order_datetime"],
    "Ratings": ["rating_date"],
    "Wastage": ["date"],
}

# Columns that are business-required even though the physical schema marks them
# nullable=True (done so Spark doesn't choke on the injected missing-value rows).
BUSINESS_REQUIRED = {
    "Orders": ["customer_id"],
    "Order_Items": ["item_id"],
    "Ratings": ["rating_value"],
    "Wastage": ["item_id"],
}

UNIT_CANON = ["pcs", "kg", "liters"]

# (child_table, child_col, parent_table, parent_col)
REFERENTIAL = [
    ("Orders", "customer_id", "Customers", "customer_id"),
    ("Orders", "location_id", "Restaurants", "location_id"),
    ("Orders", "promotion_id", "Promotions", "promotion_id"),
    ("Order_Items", "order_id", "Orders", "order_id"),
    ("Order_Items", "item_id", "Menu_Items", "item_id"),
    ("Menu_Items", "category_id", "Menu_Categories", "category_id"),
    ("Ratings", "customer_id", "Customers", "customer_id"),
    ("Ratings", "item_id", "Menu_Items", "item_id"),
    ("Ratings", "location_id", "Restaurants", "location_id"),
    ("Ratings", "order_id", "Orders", "order_id"),
    ("Inventory", "item_id", "Menu_Items", "item_id"),
    ("Inventory", "location_id", "Restaurants", "location_id"),
    ("Wastage", "item_id", "Menu_Items", "item_id"),
    ("Wastage", "location_id", "Restaurants", "location_id"),
    ("Pricing_History", "item_id", "Menu_Items", "item_id"),
    ("Pricing_History", "location_id", "Restaurants", "location_id"),
    ("Customers", "home_location_id", "Restaurants", "location_id"),
]

# child_table/child_col -> SRS report category for the referential checks.
RESTAURANT_ID_CHECKS = {
    ("Orders", "location_id"),
    ("Ratings", "location_id"),
    ("Wastage", "location_id"),
    ("Inventory", "location_id"),
}
LOCATION_REF_CHECKS = {
    ("Customers", "home_location_id"),
    ("Pricing_History", "location_id"),
}


def count_orphans(child_df, child_col, parent_df, parent_col):
    child_nonnull = child_df.filter(F.col(child_col).isNotNull())
    parent_keys = parent_df.select(F.col(parent_col).alias("_parent_key")).distinct()
    orphans = child_nonnull.join(
        parent_keys, child_nonnull[child_col] == parent_keys["_parent_key"], "left_anti"
    )
    return orphans.count()


def main():
    ensure_dirs(REPORTS_DIR)
    logger = JobLogger("ingest")
    spark = get_spark("dineiq-ingest")
    spark.sparkContext.setLogLevel("WARN")

    dq_issues = []  # dicts: table, column, rule, rows_affected, table_rows, pct, note

    def add_issue(table, column, rule, n, note=""):
        table_rows = row_counts[table]
        pct = round(100.0 * n / table_rows, 4) if table_rows else 0.0
        dq_issues.append(
            {
                "table": table,
                "column": column,
                "rule": rule,
                "rows_affected": n,
                "table_rows": table_rows,
                "pct_of_table": pct,
                "note": note,
            }
        )

    # ---- 1. explicit-schema ingestion of all 13 tables ----
    tables = {}
    row_counts = {}
    with logger.timer("load all 13 tables with explicit StructType schema"):
        for name in TABLES:
            with logger.timer(f"load {name}"):
                df = load_table(spark, name, RAW_DIR).cache()
                n = df.count()
                tables[name] = df
                row_counts[name] = n
                logger.log(f"{name}: {n} rows, {len(df.columns)} cols, schema={df.schema.simpleString()}")

    # ---- 2. schema inference demo ----
    with logger.timer("schema inference demo (Restaurants, inferSchema=True)"):
        inferred = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(RAW_DIR / "Restaurants.csv"))
        )
        logger.log("Explicit schema (Restaurants):")
        for f in SCHEMAS["Restaurants"].fields:
            logger.log(f"  {f.name}: {f.dataType}")
        logger.log("Inferred schema (Restaurants, inferSchema=True):")
        for f in inferred.schema.fields:
            logger.log(f"  {f.name}: {f.dataType}")

    # ---- 3. large-file + multi-file ingestion demo (Order_Items) ----
    with logger.timer("large/multi-file ingestion demo (Order_Items split by month)"):
        parts_dir = RAW_DIR.parent / "raw_data_parts" / "order_items"
        orders_months = (
            tables["Orders"]
            .select("order_id", "order_datetime")
            .dropDuplicates(["order_id"])
            .select("order_id", F.date_format("order_datetime", "yyyy-MM").alias("order_month"))
        )
        oi_with_month = tables["Order_Items"].join(orders_months, "order_id", "left")
        (
            oi_with_month.write.mode("overwrite")
            .option("header", True)
            .partitionBy("order_month")
            .csv(str(parts_dir))
        )
        part_files = list(parts_dir.rglob("*.csv"))
        multi_df = spark.read.option("header", True).csv(str(parts_dir))
        multi_count = multi_df.count()
        logger.log(
            f"wrote Order_Items to {len(part_files)} CSV part files across order_month partitions; "
            f"re-read {multi_count} rows from that directory as a single DataFrame "
            f"(original Order_Items had {row_counts['Order_Items']} rows)"
        )
        if multi_count != row_counts["Order_Items"]:
            logger.log("WARNING: multi-file re-read row count does not match original load")

    # ---- 4. data-type validation demo (typed load vs raw-string load diff) ----
    with logger.timer("data-type validation (typed vs raw-string cast diff)"):
        for table, cols in TYPE_VALIDATION_TARGETS.items():
            raw_df = load_table_raw_strings(spark, table, RAW_DIR).cache()
            typed_df = tables[table]
            for col in cols:
                typed_nulls = typed_df.filter(F.col(col).isNull()).count()
                raw_blanks = raw_df.filter(
                    F.col(col).isNull() | (F.trim(F.col(col)) == "")
                ).count()
                mismatch = typed_nulls - raw_blanks
                declared_type = SCHEMAS[table][col].dataType
                logger.log(
                    f"{table}.{col}: declared type {declared_type}; "
                    f"{raw_blanks} truly blank, {mismatch} rows fail to parse as {declared_type} "
                    f"despite a non-empty raw value (type mismatch)"
                )
                if raw_blanks:
                    add_issue(table, col, "missing_value", raw_blanks)
                if mismatch:
                    add_issue(table, col, "invalid_date", mismatch, note="malformed/unparseable format")

    # ---- 5. referential integrity ----
    with logger.timer("referential integrity checks"):
        for child_table, child_col, parent_table, parent_col in REFERENTIAL:
            n = count_orphans(tables[child_table], child_col, tables[parent_table], parent_col)
            logger.log(f"{child_table}.{child_col} -> {parent_table}.{parent_col}: {n} orphan rows")
            if (child_table, child_col) in RESTAURANT_ID_CHECKS:
                rule = "invalid_restaurant_id"
            elif (child_table, child_col) in LOCATION_REF_CHECKS:
                rule = "invalid_location_reference"
            else:
                rule = "orphan_foreign_key"
            if n:
                add_issue(child_table, child_col, rule, n, note=f"-> {parent_table}.{parent_col}")

    # ---- 6. remaining data quality checks (SRS Step 4 list) ----
    with logger.timer("missing value checks (non-date required columns)"):
        missing_cols = {t: list(cols) for t, cols in BUSINESS_REQUIRED.items()}
        for table in TABLES:
            date_targets = set(TYPE_VALIDATION_TARGETS.get(table, []))
            for f in SCHEMAS[table].fields:
                if not f.nullable and f.name not in date_targets:
                    missing_cols.setdefault(table, [])
                    if f.name not in missing_cols[table]:
                        missing_cols[table].append(f.name)
        for table, cols in missing_cols.items():
            for col in cols:
                n = tables[table].filter(F.col(col).isNull()).count()
                if n:
                    add_issue(table, col, "missing_value", n)

    with logger.timer("duplicate checks"):
        dup_orders = row_counts["Orders"] - tables["Orders"].select("order_id").distinct().count()
        add_issue("Orders", "order_id", "duplicate_orders", dup_orders)
        dup_oi = (
            row_counts["Order_Items"]
            - tables["Order_Items"].select("order_id", "item_id").distinct().count()
        )
        add_issue("Order_Items", "order_id+item_id", "duplicate_order_items", dup_oi)

    with logger.timer("invalid price / negative quantity checks"):
        add_issue(
            "Menu_Items", "base_price", "invalid_price",
            tables["Menu_Items"].filter(F.col("base_price") <= 0).count(),
        )
        add_issue(
            "Pricing_History", "price", "invalid_price",
            tables["Pricing_History"].filter(F.col("price") <= 0).count(),
        )
        add_issue(
            "Order_Items", "unit_price", "invalid_price",
            tables["Order_Items"].filter(F.col("unit_price") <= 0).count(),
        )
        add_issue(
            "Order_Items", "quantity", "negative_quantity",
            tables["Order_Items"].filter(F.col("quantity") < 0).count(),
        )

    with logger.timer("invalid date range checks"):
        for (table, col), (lo, hi) in VALID_RANGES.items():
            n = (
                tables[table]
                .filter(F.col(col).isNotNull() & ((F.col(col) < F.lit(lo)) | (F.col(col) > F.lit(hi))))
                .count()
            )
            if n:
                add_issue(table, col, "invalid_date", n, note=f"outside valid range {lo}..{hi}")

    with logger.timer("invalid rating checks"):
        n = (
            tables["Ratings"]
            .filter(F.col("rating_value").isNotNull() & ((F.col("rating_value") < 1) | (F.col("rating_value") > 5)))
            .count()
        )
        add_issue("Ratings", "rating_value", "invalid_rating", n)

    with logger.timer("impossible wastage quantity check"):
        wi = tables["Wastage"].join(
            tables["Inventory"].select("item_id", "location_id", "date", "prepared_quantity", "consumed_stock"),
            ["item_id", "location_id", "date"],
            "inner",
        )
        n = wi.filter(F.col("quantity_wasted") > (F.col("prepared_quantity") - F.col("consumed_stock"))).count()
        add_issue("Wastage", "quantity_wasted", "impossible_wastage_quantity", n)

    with logger.timer("incorrect discount check"):
        n = tables["Orders"].filter(F.col("discount_amount") > F.col("subtotal")).count()
        add_issue("Orders", "discount_amount", "incorrect_discount", n)

    with logger.timer("cancelled transaction count"):
        n = tables["Orders"].filter(F.col("order_status") == "Cancelled").count()
        add_issue("Orders", "order_status", "cancelled_transaction_count", n, note="not a defect, informational")

    with logger.timer("inconsistent unit checks"):
        for table in ("Inventory", "Wastage"):
            n = tables[table].filter(~F.col("unit").isin(UNIT_CANON)).count()
            add_issue(table, "unit", "inconsistent_unit", n)

    # ---- 7. write the data quality report ----
    with logger.timer("write data quality report"):
        write_report(dq_issues, row_counts, logger)

    logger.log("ingestion + validation job complete")
    logger.close()
    spark.stop()


def write_report(dq_issues, row_counts, logger):
    csv_path = REPORTS_DIR / "data_quality_report.csv"
    with open(csv_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["table", "column", "rule", "rows_affected", "table_rows", "pct_of_table", "note"])
        writer.writeheader()
        writer.writerows(dq_issues)
    logger.log(f"wrote {csv_path} ({len(dq_issues)} rows) — machine-readable version for tests")

    summary_path = REPORTS_DIR / "dirty_injection_summary.csv"
    injected = {}
    if summary_path.exists():
        with open(summary_path) as fh:
            for row in csv.DictReader(fh):
                key = (row["table"], row["column"], row["rule"])
                injected[key] = int(row["rows_affected"])

    lines = ["# Data Quality Report", "", "Generated by `spark_jobs/01_ingest_and_validate.py`.", ""]
    lines += ["## Table row counts", "", "| Table | Rows |", "|---|---|"]
    for table, n in row_counts.items():
        lines.append(f"| {table} | {n} |")

    lines += ["", "## Issues found", "", "| Table | Column | Rule | Rows | % of table | Note |", "|---|---|---|---|---|---|"]
    for issue in dq_issues:
        lines.append(
            f"| {issue['table']} | {issue['column']} | {issue['rule']} | {issue['rows_affected']} | "
            f"{issue['pct_of_table']}% | {issue['note']} |"
        )

    lines += ["", "## Cross-check against Step 1 dirty_injection_summary.csv", ""]
    matched_rules = {"missing_value", "duplicate_orders", "duplicate_order_items", "invalid_price",
                      "negative_quantity", "invalid_date", "invalid_rating", "orphan_foreign_key",
                      "invalid_restaurant_id", "invalid_location_reference",
                      "impossible_wastage_quantity", "incorrect_discount", "inconsistent_unit"}
    rule_alias = {
        "duplicate_orders": "duplicate_row", "duplicate_order_items": "duplicate_row",
        "invalid_rating": "rating_out_of_range", "invalid_restaurant_id": "orphan_foreign_key",
        "invalid_location_reference": "orphan_foreign_key",
        "impossible_wastage_quantity": "wastage_exceeds_prepared",
        "incorrect_discount": "discount_exceeds_subtotal",
    }
    found_totals = {}
    for issue in dq_issues:
        rule = rule_alias.get(issue["rule"], issue["rule"])
        key = (issue["table"], rule)
        found_totals[key] = found_totals.get(key, 0) + issue["rows_affected"]

    injected_totals = {}
    for (table, column, rule), n in injected.items():
        injected_totals[(table, rule)] = injected_totals.get((table, rule), 0) + n

    lines += ["| Table | Rule | Injected (Step 1) | Found (Step 2) | Gap |", "|---|---|---|---|---|"]
    all_keys = sorted(set(injected_totals) | set(found_totals))
    for table, rule in all_keys:
        inj = injected_totals.get((table, rule), 0)
        found = found_totals.get((table, rule), 0)
        gap = "" if inj == found else f"diff {found - inj:+d}"
        lines.append(f"| {table} | {rule} | {inj} | {found} | {gap} |")

    lines += [
        "",
        "Gaps are expected where one Step 2 check spans multiple Step 1 rules (e.g. referential-integrity "
        "checks split by report category) or where Step 2's range/format checks catch cases the generator "
        "didn't specifically log. Specific gaps observed on the scale-1 seed-42 dataset:",
        "- `Order_Items duplicate_order_items` runs ~68 over the injected count: the (order_id, item_id) key "
        "also catches unrelated rows that share an order_id and both have a null item_id (from the "
        "missing_value injection), which collide under that key even though they weren't injected as duplicates.",
        "- `Orders cancelled_transaction_count` has no Step 1 counterpart by design — cancellations are real "
        "business data (~4.2% of orders), not injected dirtiness.",
        "- `Wastage wastage_exceeds_prepared` runs 3 under the injected count: the check inner-joins Wastage "
        "to Inventory on (item_id, location_id, date), and a handful of those Wastage rows have no matching "
        "Inventory row to compare against, so they're excluded rather than miscounted.",
    ]

    (REPORTS_DIR / "data_quality_report.md").write_text("\n".join(lines) + "\n")
    logger.log(f"wrote {REPORTS_DIR / 'data_quality_report.md'} with {len(dq_issues)} issue rows")


if __name__ == "__main__":
    main()

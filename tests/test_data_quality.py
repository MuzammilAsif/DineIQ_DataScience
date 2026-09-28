"""Checks reports/data_quality_report.csv (written by spark_jobs/01_ingest_and_validate.py)
against reports/dirty_injection_summary.csv (written by Step 1) — non-zero detection for
every intentionally injected issue, and roughly-matching counts.

Requires spark_jobs/01_ingest_and_validate.py to have been run at least once.
"""
import csv

from spark_utils import REPORTS_DIR

# Step 1 rule name -> Step 2 report rule name(s) expected to catch it.
INJECTED_RULE_ALIASES = {
    "missing_value": {"missing_value"},
    "duplicate_row": {"duplicate_orders", "duplicate_order_items"},
    "invalid_price": {"invalid_price"},
    "negative_quantity": {"negative_quantity"},
    "invalid_date": {"invalid_date"},
    "rating_out_of_range": {"invalid_rating"},
    "orphan_foreign_key": {"orphan_foreign_key", "invalid_restaurant_id", "invalid_location_reference"},
    "wastage_exceeds_prepared": {"impossible_wastage_quantity"},
    "discount_exceeds_subtotal": {"incorrect_discount"},
    "inconsistent_unit": {"inconsistent_unit"},
}

# Injected rules whose Step 2 count is expected to diverge more than a few percent,
# documented in reports/data_quality_report.md's cross-check section.
LOOSE_MATCH_RULES = {"duplicate_row", "wastage_exceeds_prepared"}


def _load_csv(path):
    with open(path) as fh:
        return list(csv.DictReader(fh))


def _found_totals():
    found = _load_csv(REPORTS_DIR / "data_quality_report.csv")
    totals = {}
    for row in found:
        key = (row["table"], row["rule"])
        totals[key] = totals.get(key, 0) + int(row["rows_affected"])
    return totals


def test_reports_exist():
    assert (REPORTS_DIR / "data_quality_report.csv").exists(), "run spark_jobs/01_ingest_and_validate.py first"
    assert (REPORTS_DIR / "dirty_injection_summary.csv").exists(), "Step 1 output missing"


def test_injected_issues_are_detected():
    injected = _load_csv(REPORTS_DIR / "dirty_injection_summary.csv")
    found_totals = _found_totals()

    for row in injected:
        table, rule, n = row["table"], row["rule"], int(row["rows_affected"])
        if n == 0:
            continue
        expected_rules = INJECTED_RULE_ALIASES.get(rule, {rule})
        total_found = sum(v for (t, r), v in found_totals.items() if t == table and r in expected_rules)
        assert total_found > 0, f"Step 1 injected {n} '{rule}' rows into {table}, but Step 2 found none of {expected_rules}"


def test_counts_roughly_match_injection_log():
    injected = _load_csv(REPORTS_DIR / "dirty_injection_summary.csv")
    found_totals = _found_totals()

    for row in injected:
        table, rule, n = row["table"], row["rule"], int(row["rows_affected"])
        if n == 0 or rule in LOOSE_MATCH_RULES:
            continue
        expected_rules = INJECTED_RULE_ALIASES.get(rule, {rule})
        total_found = sum(v for (t, r), v in found_totals.items() if t == table and r in expected_rules)
        assert total_found >= n * 0.9, f"{table}.{rule}: Step 1 injected {n}, Step 2 found only {total_found}"

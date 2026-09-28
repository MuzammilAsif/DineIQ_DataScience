import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd

import dimensions
import dirty
import operations
import transactions
from common import SEED, Context

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(description="Generate the DineIQ raw dataset.")
    parser.add_argument("--scale", type=float, default=1.0,
                        help="multiplier on base volumes of customers, orders, order lines and ratings")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "raw_data")
    parser.add_argument("--report-dir", type=Path, default=PROJECT_ROOT / "reports")
    parser.add_argument("--clean", action="store_true", help="skip dirty-data injection")
    return parser.parse_args()


def timed(label, fn, *args):
    start = time.perf_counter()
    result = fn(*args)
    print(f"  {label:<40} {time.perf_counter() - start:6.1f}s")
    return result


def generate(scale, seed):
    ctx = Context(rng=np.random.default_rng(seed), scale=scale)
    tables = {}
    tables["Restaurants"] = timed("Restaurants", dimensions.build_restaurants, ctx)
    tables["Menu_Categories"], tables["Menu_Items"] = timed("Menu_Categories, Menu_Items", dimensions.build_menu, ctx)
    tables["Pricing_History"] = timed("Pricing_History", dimensions.build_pricing_history, ctx, tables["Menu_Items"])
    tables["Customers"] = timed("Customers", dimensions.build_customers, ctx)
    tables["Promotions"], tables["Promotion_Items"], tables["Promotion_Locations"] = timed(
        "Promotions and bridge tables", dimensions.build_promotions, ctx)
    timed("Orders (skeleton)", transactions.build_orders, ctx)
    tables["Orders"], tables["Order_Items"] = timed("Order_Items and order totals", transactions.build_order_items, ctx)
    tables["Ratings"] = timed("Ratings", operations.build_ratings, ctx)
    tables["Inventory"], tables["Wastage"] = timed("Inventory and Wastage", operations.build_inventory_and_wastage, ctx)
    return ctx, tables


def sanity_report(tables):
    counts, nulls = [], []
    for name, df in tables.items():
        counts.append((name, len(df), len(df.columns)))
        pct = (df.isna().mean() * 100).round(3)
        nulls += [(name, col, float(pct[col])) for col in df.columns]
    return (pd.DataFrame(counts, columns=["table", "rows", "columns"]),
            pd.DataFrame(nulls, columns=["table", "column", "null_pct"]))


def main():
    args = parse_args()
    print(f"Generating DineIQ dataset (scale={args.scale}, seed={args.seed})")
    ctx, tables = generate(args.scale, args.seed)

    args.report_dir.mkdir(parents=True, exist_ok=True)
    if not args.clean:
        log, summary = timed("Dirty-data injection", dirty.inject, tables, ctx)
        log.to_csv(args.report_dir / "dirty_injection_log.csv", index=False)
        summary.to_csv(args.report_dir / "dirty_injection_summary.csv", index=False)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    for name, df in tables.items():
        df.to_csv(args.output_dir / f"{name}.csv", index=False)
    print(f"  {'Write CSVs':<40} {time.perf_counter() - start:6.1f}s")

    counts, nulls = sanity_report(tables)
    counts.to_csv(args.report_dir / "generation_row_counts.csv", index=False)
    nulls.to_csv(args.report_dir / "generation_null_percentages.csv", index=False)
    print("\nRow counts")
    print(counts.to_string(index=False))
    print("\nColumns with nulls (%)")
    print(nulls[nulls["null_pct"] > 0].to_string(index=False))
    if not args.clean:
        print("\nInjected dirtiness")
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()

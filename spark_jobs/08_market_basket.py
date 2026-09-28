"""Market-basket analysis with Spark MLlib FPGrowth (SRS Steps 17-18).

Output: parquet_data/market_basket/{itemsets,rules}/, report section 08.
Run: .venv/bin/python spark_jobs/08_market_basket.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml.fpm import FPGrowth

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from spark_utils import JobLogger, get_spark  # noqa: E402


def baskets(fact):
    return (fact.filter(F.col("item_id").isNotNull())
            .groupBy("order_id").agg(F.array_sort(F.collect_set("item_id")).alias("items")))


def mine(basket_df, cfg):
    model = FPGrowth(itemsCol="items", minSupport=cfg["min_support"],
                     minConfidence=cfg["min_confidence"]).fit(basket_df)
    return model.freqItemsets, model.associationRules


def recommendation(row, names, min_lift):
    lhs = " + ".join(names.get(i, i) for i in row.antecedent)
    rhs = " + ".join(names.get(i, i) for i in row.consequent)
    verdict = (f"Candidate bundle: {lhs} + {rhs}." if row.lift >= min_lift
               else "Weak association, not a bundle candidate.")
    return (f"Customers who order {lhs} also order {rhs} {row.confidence:.0%} of the time "
            f"({row.lift:.2f}x the base rate, in {row.support:.1%} of orders). {verdict}")


def report(n_orders, n_multi, n_itemsets, n_rules, top, names, cfg):
    lines = [
        "## 2. Market-basket analysis",
        f"Spark MLlib `FPGrowth` on {n_orders:,} completed orders ({n_multi:,} with two or more "
        f"distinct items), minSupport {cfg['min_support']}, minConfidence "
        f"{cfg['min_confidence']}. {n_itemsets:,} frequent itemsets, {n_rules:,} rules.",
    ]
    if not top:
        lines.append("No rules met the thresholds. Lower `market_basket.min_support` or "
                     "`min_confidence` in the config.")
        return "\n\n".join(lines)
    max_lift = top[0].lift
    strong = sum(r.lift >= cfg["bundle_min_lift"] for r in top)
    lines.append(
        f"The highest lift is {max_lift:.2f}. Lift is how much more often two items appear "
        "together than they would by chance, so a lift near 1 means no real association. "
        f"{strong} of the top {len(top)} rules reach the bundle threshold of "
        f"{cfg['bundle_min_lift']}. "
        + ("Item choices in this dataset are close to independent, so the list below is weak "
           "evidence for bundling. The strongest pairs are still the best candidates to test."
           if strong == 0 else ""))
    lines.append(f"Top {len(top)} rules by lift:")
    lines.append("\n".join(f"{i}. {recommendation(r, names, cfg['bundle_min_lift'])}"
                           for i, r in enumerate(top, 1)))
    return "\n\n".join(lines)


def main():
    cfg = ac.load_config()["market_basket"]
    logger = JobLogger("market_basket")
    spark = get_spark("dineiq-market-basket")
    spark.sparkContext.setLogLevel("WARN")
    fact = ac.completed_fact(spark)
    names = {r.item_id: r.item_name for r in
             fact.select("item_id", "item_name").distinct().collect()}

    with logger.timer("FPGrowth"):
        b = baskets(fact).cache()
        itemsets, rules = mine(b, cfg)
        itemsets.write.mode("overwrite").parquet(ac.out_path("market_basket/itemsets"))
        rules.write.mode("overwrite").parquet(ac.out_path("market_basket/rules"))
    rules = spark.read.parquet(ac.out_path("market_basket/rules"))
    top = rules.orderBy(F.desc("lift"), F.desc("confidence")).limit(cfg["top_rules"]).collect()
    n_orders, n_multi = b.count(), b.filter(F.size("items") >= 2).count()
    n_itemsets = spark.read.parquet(ac.out_path("market_basket/itemsets")).count()
    logger.log(f"orders={n_orders} multi={n_multi} itemsets={n_itemsets} rules={rules.count()}")
    ac.write_section("08", report(n_orders, n_multi, n_itemsets, rules.count(), top, names, cfg))
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

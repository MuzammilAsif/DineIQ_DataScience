"""Step 2.5-2.6 — integration joins (spark_sql/integration_queries.sql), the base
fact table, and storage output (partitioned Parquet + CSV/JSON demonstration).

Run: .venv/bin/python spark_jobs/03_integrate_and_store.py
"""
import re
import sys
from pathlib import Path

import pyspark.sql.functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from schemas import SCHEMAS, load_clean_table  # noqa: E402
from spark_utils import (  # noqa: E402
    JobLogger,
    PARQUET_DIR,
    PROCESSED_DIR,
    REPORTS_DIR,
    SPARK_SQL_DIR,
    ensure_dirs,
    get_spark,
    write_single_file,
)

VIEW_NAMES = {
    "Orders": "orders", "Order_Items": "order_items", "Customers": "customers",
    "Restaurants": "restaurants", "Promotions": "promotions", "Menu_Items": "menu_items",
    "Menu_Categories": "menu_categories", "Pricing_History": "pricing_history",
    "Ratings": "ratings", "Inventory": "inventory", "Wastage": "wastage",
    "Promotion_Items": "promotion_items", "Promotion_Locations": "promotion_locations",
}

# table -> its own date column, for the section-5 partitioned parquet requirement.
# Order_Items has no date column of its own; it's partitioned via a join to Orders below.
DIRECT_DATE_PARTITION_TABLES = {"Orders": "order_datetime", "Inventory": "date", "Wastage": "date"}

JSON_DEMO_TABLES = ["Menu_Categories", "Promotions"]


def parse_sql_file(path: Path):
    text = path.read_text()
    parts = re.split(r"--\s*@name:\s*(\w+)", text)
    queries = {}
    for i in range(1, len(parts), 2):
        name = parts[i].strip()
        sql = parts[i + 1].strip().rstrip(";").strip()
        queries[name] = sql
    return queries


def main():
    ensure_dirs(PARQUET_DIR, PROCESSED_DIR, REPORTS_DIR)
    logger = JobLogger("integrate")
    spark = get_spark("dineiq-integrate")
    spark.sparkContext.setLogLevel("WARN")

    cleaned = {}
    with logger.timer("load cleaned tables and register temp views"):
        for name in SCHEMAS:
            df = load_clean_table(spark, name, PROCESSED_DIR).cache()
            cleaned[name] = df
            view = VIEW_NAMES[name]
            df.createOrReplaceTempView(view)
            logger.log(f"{name} -> view '{view}': {df.count()} rows")

    queries = parse_sql_file(SPARK_SQL_DIR / "integration_queries.sql")
    logger.log(f"parsed {len(queries)} queries from integration_queries.sql: {list(queries)}")

    with logger.timer("run the 10 integration join queries"):
        for name, sql in queries.items():
            if name == "fact_order_line":
                continue
            n = spark.sql(sql).count()
            logger.log(f"query '{name}': {n} rows")

    with logger.timer("build integrated fact table (order-line grain)"):
        fact = spark.sql(queries["fact_order_line"]).cache()
        fact_count = fact.count()
        oi_count = cleaned["Order_Items"].count()
        logger.log(f"fact_order_line: {fact_count} rows (cleaned Order_Items: {oi_count})")
        if fact_count != oi_count:
            logger.log("WARNING: fact table row count does not match cleaned Order_Items row count")

    with logger.timer("write fact table to Parquet, partitioned by order_year/order_month"):
        out = PARQUET_DIR / "fact_order_line"
        fact.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(str(out))
        logger.log(f"wrote {out} ({fact_count} rows)")

    for table, date_col in DIRECT_DATE_PARTITION_TABLES.items():
        with logger.timer(f"write {table} to Parquet, partitioned by year/month"):
            df2 = (
                cleaned[table]
                .withColumn("year", F.year(F.col(date_col)))
                .withColumn("month", F.month(F.col(date_col)))
            )
            out = PARQUET_DIR / table.lower()
            df2.write.mode("overwrite").partitionBy("year", "month").parquet(str(out))
            logger.log(f"wrote {out} ({cleaned[table].count()} rows)")

    with logger.timer("write Order_Items to Parquet, partitioned by order year/month"):
        oi_with_date = (
            cleaned["Order_Items"]
            .join(cleaned["Orders"].select("order_id", "order_datetime"), "order_id", "left")
            .withColumn("year", F.year("order_datetime"))
            .withColumn("month", F.month("order_datetime"))
            .drop("order_datetime")
        )
        out = PARQUET_DIR / "order_items"
        oi_with_date.write.mode("overwrite").partitionBy("year", "month").parquet(str(out))
        logger.log(f"wrote {out} ({cleaned['Order_Items'].count()} rows)")

    with logger.timer("write small dimension tables to JSON (storage-format demonstration)"):
        for table in JSON_DEMO_TABLES:
            out = PROCESSED_DIR / f"{table}.json"
            write_single_file(cleaned[table], out, fmt="json")
            logger.log(f"wrote {out}")

    logger.log("integration + storage job complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

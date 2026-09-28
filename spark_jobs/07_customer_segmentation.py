"""Customer RFM scoring and KMeans segmentation (SRS Steps 15-16).

Output: parquet_data/customer_segments/, models/spark/customer_segmentation/, report section 07.
Run: .venv/bin/python spark_jobs/07_customer_segmentation.py
"""
import sys
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analytics_common as ac  # noqa: E402
from spark_utils import JobLogger, ROOT, get_spark  # noqa: E402

MODEL_DIR = ROOT / "models" / "spark" / "customer_segmentation"
CLUSTER_INPUTS = ["customer_recency", "log_frequency", "log_monetary", "average_order_value",
                  "promo_order_share"]
SEGMENTS = ["High-Value Loyal", "Frequent", "Promotion-Driven", "At-Risk", "New", "Occasional"]


def customer_order_stats(orders, as_of_date):
    return (orders.filter(F.to_date("order_datetime") <= F.lit(as_of_date))
            .groupBy("customer_id")
            .agg(F.avg(F.col("promotion_id").isNotNull().cast("double")).alias("promo_order_share"),
                 F.datediff(F.lit(as_of_date), F.to_date(F.min("order_datetime")))
                 .alias("tenure_days")))


def rfm_scores(df):
    def score(col, ascending):
        order = F.col(col).asc() if ascending else F.col(col).desc()
        return F.ntile(5).over(Window.orderBy(order))
    # low recency is good, so recency is ranked descending to give recent customers a 5
    df = (df.withColumn("r_score", score("customer_recency", False))
          .withColumn("f_score", score("customer_frequency", True))
          .withColumn("m_score", score("customer_monetary_value", True)))
    return (df.withColumn("rfm_code", F.concat_ws("", "r_score", "f_score", "m_score"))
            .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score")))


def add_cluster_inputs(df):
    # frequency and spend are heavily right-skewed; log keeps a few big spenders from
    # owning whole clusters
    return (df.withColumn("log_frequency", F.log1p("customer_frequency"))
            .withColumn("log_monetary", F.log1p("customer_monetary_value")))


def fit_kmeans(df, k, seed):
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=CLUSTER_INPUTS, outputCol="raw_features"),
        StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True),
        KMeans(k=k, seed=seed, featuresCol="features", predictionCol="cluster_id"),
    ])
    return pipeline.fit(df)


def cluster_profiles(df):
    return (df.groupBy("cluster_id")
            .agg(F.count("*").alias("customers"),
                 F.avg("customer_recency").alias("recency"),
                 F.avg("customer_frequency").alias("frequency"),
                 F.avg("customer_monetary_value").alias("monetary"),
                 F.avg("average_order_value").alias("aov"),
                 F.avg("promo_order_share").alias("promo_share"),
                 F.avg("tenure_days").alias("tenure"))
            .orderBy("cluster_id").toPandas())


def label_clusters(profiles, cfg):
    """Maps cluster_id to a segment by ranking cluster means; each segment is used at most once.

    Absolute cutoffs don't transfer well here: most customers order once or twice, so the
    typical recency is already ~100 days and a fixed "recency > 90" marks most clusters At-Risk.
    """
    p = profiles.set_index("cluster_id")
    labels = {}

    def take(label, col, largest=True, cond=None):
        rest = p[~p.index.isin(labels)]
        if cond is not None:
            rest = rest[cond(rest)]
        if len(rest):
            labels[rest[col].idxmax() if largest else rest[col].idxmin()] = label

    take("High-Value Loyal", "monetary")
    take("Promotion-Driven", "promo_share", cond=lambda d: d.promo_share > cfg["promo_share_min"])
    take("At-Risk", "recency")
    take("New", "tenure", largest=False)
    take("Frequent", "frequency")
    for cid in p.index:
        labels.setdefault(cid, "Occasional")
    return labels


def add_split(df, mod):
    return df.withColumn("split", F.when(F.crc32("customer_id") % mod == 0, "holdout")
                         .otherwise("train"))


def add_distance(df, centers):
    """Euclidean distance, in scaled feature space, from each row to its assigned centre."""
    table = F.array(*[F.array(*[F.lit(float(x)) for x in c]) for c in centers])
    sq = F.zip_with(vector_to_array("features"), table[F.col("cluster_id")],
                    lambda a, b: (a - b) * (a - b))
    return df.withColumn("distance_to_centroid",
                         F.sqrt(F.aggregate(sq, F.lit(0.0), lambda acc, x: acc + x)))


def segment(customer_features, orders, as_of_date, cfg):
    df = customer_features.join(customer_order_stats(orders, as_of_date), "customer_id", "left")
    df = df.fillna({"promo_order_share": 0.0, "tenure_days": 0})
    df = add_split(add_cluster_inputs(rfm_scores(df)), cfg["holdout_mod"])
    model = fit_kmeans(df.filter(F.col("split") == "train"), cfg["k"], cfg["seed"])
    df = add_distance(model.transform(df), model.stages[-1].clusterCenters())
    profiles = cluster_profiles(df)
    labels = label_clusters(profiles, cfg)
    mapping = F.create_map(*[x for cid, lab in labels.items()
                             for x in (F.lit(int(cid)), F.lit(lab))])
    df = df.withColumn("segment", mapping[F.col("cluster_id")])
    profiles["segment"] = profiles.cluster_id.map(labels)
    return df, model, profiles


def report(profiles, rfm_dist, as_of, n, cfg):
    pf = profiles[["cluster_id", "segment", "customers", "recency", "frequency", "monetary", "aov",
                   "promo_share", "tenure"]]
    seg = (profiles.groupby("segment").customers.sum().reindex(SEGMENTS).dropna().astype(int)
           .reset_index())
    return "\n\n".join([
        "## 1. Customer segmentation and RFM",
        f"{n:,} customers with at least one completed order by {as_of} "
        "(`parquet_data/features/customer_features/`).",
        "**RFM.** Recency, frequency and monetary value are each bucketed into quintiles with "
        "`ntile(5)` (5 is best: most recent, most frequent, highest spend). `rfm_code` "
        "concatenates the three and `rfm_score` sums them (3-15). Frequency has many ties at 1-2 "
        "orders, so customers with the same order count can land in adjacent quintiles.",
        md_counts(rfm_dist),
        f"**KMeans.** Spark MLlib `KMeans`, k={cfg['k']}, fitted on the customers with "
        f"crc32(customer_id) % {cfg['holdout_mod']} != 0 (the rest are held out for the Step 7 "
        "comparison and only assigned), on standardized recency, log(1 + frequency), "
        "log(1 + monetary), average order value, and promotion order share (the fraction of the "
        "customer's orders that used a promotion). The log is used because frequency and spend "
        "are heavily skewed. Clusters are labelled by ranking their means, each label used once, "
        "in this order: highest spend is High-Value Loyal; highest promo share (if above "
        f"{cfg['promo_share_min']}) is Promotion-Driven; highest recency is At-Risk; lowest "
        "tenure (days since first order) is New; highest remaining frequency is Frequent; the "
        "rest are Occasional. Fixed cutoffs were tried first and did not work: with most "
        "customers ordering once or twice, a mean recency over 90 days labelled four of six "
        "clusters At-Risk.",
        ac.md_table(pf, {"recency": "{:.0f}", "frequency": "{:.1f}", "monetary": "{:,.0f}",
                         "aov": "{:,.0f}", "promo_share": "{:.2f}", "tenure": "{:.0f}",
                         "customers": "{:,}"}),
        ac.md_table(seg, {"customers": "{:,}"}),
        "Output: `parquet_data/customer_segments/` (customer_id, cluster_id, segment, R/F/M "
        "scores, rfm_code, rfm_score). The fitted pipeline is saved to "
        "`models/spark/customer_segmentation/`.",
    ])


def md_counts(rfm_dist):
    return "RFM score distribution: " + ", ".join(
        f"{int(r.rfm_score)}: {int(r['count']):,}" for _, r in rfm_dist.iterrows()) + "."


def main():
    cfg = ac.load_config()["segmentation"]
    logger = JobLogger("segmentation")
    spark = get_spark("dineiq-segmentation")
    spark.sparkContext.setLogLevel("WARN")
    as_of = ac.latest_snapshot("customer_features")
    cf = spark.read.parquet(str(ac.FEATURES_DIR / "customer_features" / f"as_of_date={as_of}"))
    orders = ac.completed_orders(spark)

    with logger.timer("RFM + KMeans"):
        df, model, profiles = segment(cf, orders, as_of, cfg)
        df = df.select("customer_id", "split", "cluster_id", "distance_to_centroid", "segment",
                       "r_score", "f_score", "m_score",
                       "rfm_code", "rfm_score", "customer_recency", "customer_frequency",
                       "customer_monetary_value", "average_order_value", "promo_order_share",
                       "tenure_days").cache()
        df.write.mode("overwrite").parquet(ac.out_path("customer_segments"))
        model.write().overwrite().save(str(MODEL_DIR / f"as_of_date={as_of}"))
    logger.log(profiles.to_string())
    rfm_dist = df.groupBy("rfm_score").count().orderBy("rfm_score").toPandas()
    ac.write_section("07", report(profiles, rfm_dist, as_of, df.count(), cfg))
    logger.log("segmentation complete")
    logger.close()
    spark.stop()


if __name__ == "__main__":
    main()

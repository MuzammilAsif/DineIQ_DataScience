from schemas import load_clean_table
from spark_utils import PROCESSED_DIR


def test_parquet_write_read_roundtrip(spark, tmp_path):
    df = load_clean_table(spark, "Menu_Items", PROCESSED_DIR)
    sample = df.limit(20).cache()
    sample_count = sample.count()

    out = str(tmp_path / "menu_items_sample.parquet")
    sample.write.mode("overwrite").parquet(out)
    reread = spark.read.parquet(out)

    assert reread.count() == sample_count
    assert reread.schema == sample.schema

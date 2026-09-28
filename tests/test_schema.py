from schemas import SCHEMAS, load_table
from spark_utils import RAW_DIR


def test_thirteen_tables_declared():
    assert len(SCHEMAS) == 13


def test_loaded_schema_matches_declared_struct_type(spark):
    # Spark's CSV reader always reports every column as nullable=True regardless of
    # the declared schema (CSV can't enforce non-null at the format level — that's
    # exactly what the data-quality/cleaning checks are for), so compare names+types only.
    for name, declared in SCHEMAS.items():
        df = load_table(spark, name, RAW_DIR)
        loaded_shape = [(f.name, f.dataType) for f in df.schema.fields]
        declared_shape = [(f.name, f.dataType) for f in declared.fields]
        assert loaded_shape == declared_shape, f"{name}: loaded column names/types differ from the declared StructType"

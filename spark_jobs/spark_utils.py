"""Shared helpers: Spark session, timing, logging, paths."""
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw_data"
PROCESSED_DIR = ROOT / "processed_data"
QUARANTINE_DIR = PROCESSED_DIR / "quarantine"
PARQUET_DIR = ROOT / "parquet_data"
REPORTS_DIR = ROOT / "reports"
SPARK_SQL_DIR = ROOT / "spark_sql"


def get_spark(app_name: str) -> SparkSession:
    # The machine's global HADOOP_CONF_DIR points fs.defaultFS at an HDFS namenode
    # that isn't running here. This project reads/writes local directories only, so
    # pin the default filesystem to local regardless of that global config.
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


class JobLogger:
    """Writes to both stdout and a per-job log file under reports/."""

    def __init__(self, job_name: str):
        self.path = REPORTS_DIR / f"spark_execution_log_{job_name}.txt"
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "w")

    def log(self, msg: str):
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
        print(line)
        self._fh.write(line + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()

    @contextmanager
    def timer(self, stage: str):
        start = time.time()
        self.log(f"START  {stage}")
        try:
            yield
        finally:
            duration = time.time() - start
            self.log(f"END    {stage} (duration {duration:.2f}s)")


def ensure_dirs(*dirs: Path):
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def write_single_file(df, out_path: Path, fmt: str = "csv", **write_options):
    """Write a Spark DataFrame as one named output file instead of a part-file directory."""
    tmp_dir = out_path.with_suffix(out_path.suffix + ".tmp_write_dir")
    writer = df.coalesce(1).write.mode("overwrite")
    for k, v in write_options.items():
        writer = writer.option(k, v)
    getattr(writer, fmt)(str(tmp_dir))
    part_file = next(tmp_dir.glob(f"part-*.{fmt}"))
    if out_path.exists():
        out_path.unlink()
    part_file.rename(out_path)
    for leftover in tmp_dir.iterdir():
        leftover.unlink()
    tmp_dir.rmdir()

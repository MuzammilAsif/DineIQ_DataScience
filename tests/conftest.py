import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "spark_jobs"))


@pytest.fixture(scope="session")
def spark():
    from spark_utils import get_spark

    s = get_spark("dineiq-tests")
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()

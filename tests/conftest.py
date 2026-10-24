import os
import sys
import time
from pathlib import Path

import pytest

# Spark returns naive datetimes in the *Python process* timezone; pin it to UTC.
os.environ["TZ"] = "UTC"
time.tzset()

ROOT = Path(__file__).resolve().parents[1]
# Pipeline code imports `pos_lib` relative to the pipeline root_path, as in Databricks.
sys.path.insert(0, str(ROOT / "pipelines" / "pos_country"))

_JDK = Path.home() / ".local/opt/jdk17/Contents/Home"
if "JAVA_HOME" not in os.environ and _JDK.exists():
    os.environ["JAVA_HOME"] = str(_JDK)


def pytest_addoption(parser):
    parser.addoption("--env", default="staging", help="deployed environment for integration tests")


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    assert pyspark
    s = (SparkSession.builder.master("local[2]").appName("pos-tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true")  # serverless default
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()

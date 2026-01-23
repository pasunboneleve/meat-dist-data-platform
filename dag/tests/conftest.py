import os
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession

# Set env vars early to prevent import errors in DAG files during collection
os.environ.setdefault("BRONZE_BUCKET", "test-bronze")
os.environ.setdefault("SILVER_BUCKET", "test-silver")
os.environ.setdefault("GOLD_BUCKET", "test-gold")
os.environ.setdefault("DEPS_BUCKET", "test-deps")
os.environ.setdefault("GCP_PROJECT_ID", "test-project")
os.environ.setdefault("DATAPROC_REGION", "us-central1")
os.environ.setdefault("CATALOG_NAME", "test_catalog")
os.environ.setdefault("DB_NAME", "test_db")
os.environ.setdefault(
    "DATAPROC_BATCH_SERVICE_ACCOUNT", "test-sa@project.iam.gserviceaccount.com"
)
os.environ.setdefault("SYNTHETIC_MEAT_URL", "http://localhost:8080")


@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a SparkSession."""
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("unittest-bronze-to-silver")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()

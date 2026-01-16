# dags/assets.py (or similar)
import os

from airflow.sdk import Asset

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET")

bronze_carcasses_asset = Asset(
    uri=f"gcs://{BRONZE_BUCKET}/carcasses",
    name="bronze_carcasses",  # Choose ONE clear, unique name
    extra={"layer": "bronze", "domain": "synthetic_meat"},
)


silver_dv2_asset = Asset(
    uri=f"gcs://{SILVER_BUCKET}/iceberg_warehouse",
    name="silver_dv2_iceberg",
    extra={"layer": "silver", "format": "iceberg"},
)

gold_kimball_asset = Asset(
    uri=f"gcs://{GOLD_BUCKET}/fact_carcass_transactions",
    name="gold_kimball_fact_carcass_transactions",
    extra={"layer": "gold", "model": "kimball"},
)

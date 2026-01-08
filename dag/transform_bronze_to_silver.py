#!/usr/bin/env python3
# PySpark script for Bronze -> Silver DV2 Iceberg
# Run via DataprocServerlessSparkBatchOperator
# Filters to target_date partition only (passed via spark.conf)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import hashlib
from datetime import datetime, date
import sys
import argparse

def hash_key(*cols):
    """Generate HK as hex(SHA256 of concatenated cols)."""
    def _hash(col_values):
        key_str = "".join([str(v) for v in col_values if v is not None])
        return hashlib.sha256(key_str.encode()).hexdigest()
    return udf(_hash, StringType())

def main():
    spark = SparkSession.builder \
        .appName("bronze-to-silver-dv2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    # Get params from Spark conf (passed via Airflow templating)
    project_id = spark.conf.get("spark.sql.project_id")
    bronze_bucket = spark.conf.get("spark.sql.bronze_bucket")
    silver_bucket = spark.conf.get("spark.sql.silver_bucket")
    target_date_str = spark.conf.get("spark.sql.target_date")  # e.g., "2024/12/27"
    year, month, day = map(int, target_date_str.split("/"))
    load_dts = datetime.now().isoformat()

    bronze_path = f"gs://{bronze_bucket}/carcasses/*/"
    bronze_path += f"year={year}/month={month:02d}/day={day:02d}/*.parquet"

    # Assume indicator data in same Parquet (or union if separate prefix)
    df = spark.read.parquet(bronze_path) \
        .filter(col("year") == year) \
        .filter(col("month") == month) \
        .filter(col("day") == day)

    # Cache for multiple uses
    df.cache()

    # DV2 Entities (simplified, add SCD/eff dates as needed)
    # Hub_Carcass
    hub_carcass = df.select(
        col("carcass_id").alias("carcass_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src")
    ).distinct()
    hub_carcass.write \
        .format("iceberg") \
        .mode("append") \
        .saveAsTable("iceberg.hub_carcass")  # MERGE not needed for hub if no updates

    # For idempotency, use MERGE (example for hub_carcass)
    spark.sql(f"""
        MERGE INTO iceberg.hub_carcass t
        USING (SELECT * FROM {hub_carcass.createOrReplaceTempView('source_hub_carcass')})
        s ON t.carcass_id = s.carcass_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Hub_Plant
    hub_plant = df.select(
        col("plant_id").alias("plant_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src")
    ).distinct()
    spark.sql("""
        MERGE INTO iceberg.hub_plant t
        USING source_hub_plant s ON t.plant_id = s.plant_id
        WHEN NOT MATCHED THEN INSERT *
    """)  # Assume temp view created similarly

    # Hub_Indicator (new)
    hub_indicator = df.select(
        col("indicator_id").alias("indicator_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src")
    ).distinct()
    hub_indicator.createOrReplaceTempView("source_hub_indicator")
    spark.sql("""
        MERGE INTO iceberg.hub_indicator t
        USING source_hub_indicator s ON t.indicator_id = s.indicator_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Sat_Carcass_Detail (hash diff for SCD2)
    carcass_hk = hash_key(col("carcass_id"), lit(load_dts))
    sat_carcass = df.select(
        carcass_hk.alias("carcass_hk"),
        col("weight_kg"),
        col("grade"),
        col("price_per_kg"),
        col("marbling_score"),
        col("indicator_des"),  # Include indicator details
        col("species_id"),
        col("indicator_units"),
        col("slaughter_date").cast("date").alias("slaughter_date"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src"),
    )
    sat_carcass.createOrReplaceTempView("source_sat_carcass")
    spark.sql("""
        MERGE INTO iceberg.sat_carcass_detail t
        USING source_sat_carcass s
        ON t.carcass_hk = s.carcass_hk
        WHEN NOT MATCHED THEN INSERT *
        -- Add SCD logic: WHEN MATCHED AND hash_diff THEN update eff_to, insert new
    """)

    # Link_Carcass_Process (plant)
    plant_hk = hash_key(col("plant_id"), lit(load_dts))
    link_carcass_plant = df.select(
        carcass_hk.alias("carcass_hk"),
        plant_hk.alias("plant_hk"),
        lit(load_dts).alias("load_dts"),
        col("process_date").cast("date").alias("process_date"),
    ).distinct()
    link_carcass_plant.createOrReplaceTempView("source_link_carcass_plant")
    spark.sql("""
        MERGE INTO iceberg.link_carcass_plant t
        USING source_link_carcass_plant s
        ON t.carcass_hk = s.carcass_hk AND t.plant_hk = s.plant_hk
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Optional: Link to indicator if multi
    indicator_hk = hash_key(col("indicator_id"), lit(load_dts))
    # Similar MERGE for link_carcass_indicator

    df.unpersist()
    spark.stop()

if __name__ == "__main__":
    main()

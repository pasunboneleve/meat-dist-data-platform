#!/usr/bin/env python3
# PySpark script for Bronze -> Silver DV2 Iceberg
# Run via DataprocCreateBatchOperator
# Filters to target_date partition only (passed via spark.conf)

import hashlib
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, dayofmonth, lit, month, udf, year
from pyspark.sql.types import StringType


class DagConfigError(Exception):
    pass


def hash_key(*cols) -> Column:
    """Generate HK as hex(SHA256 of concatenated cols)."""

    def _hash(col_values):
        key_str = "".join([str(v) for v in col_values if v is not None])
        return hashlib.sha256(key_str.encode()).hexdigest()

    udf_func = udf(_hash, StringType())
    return udf_func(*cols)


def main():
    spark = (
        SparkSession.builder.appName("bronze-to-silver-dv2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    # Get params from Spark conf (passed via Airflow templating)
    bronze_bucket = spark.conf.get("spark.sql.bronze_bucket")
    silver_bucket = spark.conf.get("spark.sql.silver_bucket")
    target_date_str = spark.conf.get("spark.sql.target_date_str")  # e.g., "2024-12-27"
    if not target_date_str:
        raise DagConfigError("target_date_str missing.")
    target_date = date.fromisoformat(target_date_str)
    load_dts = datetime.now().isoformat()

    bronze_path = f"gs://{bronze_bucket}/carcasses/"
    bronze_path += f"year={target_date.year}/month={target_date.month}/day={target_date.day}/*.parquet"

    df = spark.read.parquet(bronze_path).filter(
        (year("slaughter_date") == target_date.year)
        & (month("slaughter_date") == target_date.month)
        & (dayofmonth("slaughter_date") == target_date.day)
    )

    # Read indicator table
    indicator_df = spark.read.parquet(
        f"gs://{bronze_bucket}/indicator/indicator.parquet"
    )

    # Cache for multiple uses
    df.cache()
    indicator_df.cache()

    # DV2 Entities (simplified, add SCD/eff dates as needed)
    # Hub_Carcass
    hub_carcass = df.select(
        col("carcass_id").alias("carcass_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src"),
    ).distinct()
    hub_carcass.createOrReplaceTempView("source_hub_carcass")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.hub_carcass USING iceberg AS SELECT * FROM source_hub_carcass LIMIT 0
    """)
    spark.sql("""
        MERGE INTO iceberg.hub_carcass t
        USING source_hub_carcass s
        ON t.carcass_id = s.carcass_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Hub_Plant
    hub_plant = df.select(
        col("plant_id").alias("plant_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src"),
    ).distinct()
    hub_plant.createOrReplaceTempView("source_hub_plant")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.hub_plant USING iceberg AS SELECT * FROM source_hub_plant LIMIT 0
    """)
    spark.sql("""
        MERGE INTO iceberg.hub_plant t
        USING source_hub_plant s ON t.plant_id = s.plant_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Hub_Indicator (new)
    hub_indicator = indicator_df.select(
        col("indicator_id").alias("indicator_id"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src"),
    ).distinct()
    hub_indicator.createOrReplaceTempView("source_hub_indicator")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.hub_indicator USING iceberg AS SELECT * FROM source_hub_indicator LIMIT 0
    """)
    spark.sql("""
        MERGE INTO iceberg.hub_indicator t
        USING source_hub_indicator s ON t.indicator_id = s.indicator_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Sat_Carcass_Detail (hash diff for SCD2)
    carcass_hk = hash_key(col("carcass_id"))
    sat_carcass = df.select(
        carcass_hk.alias("carcass_hk"),
        col("hscw_kg"),
        col("animal_class"),
        col("price_aud_per_kg"),
        col("marbling_score"),
        col("quality_score"),
        col("fat_depth_mm"),
        col("total_price_aud"),
        col("slaughter_date").cast("date").alias("slaughter_date"),
        lit(load_dts).alias("load_dts"),
        lit("BRONZE").alias("rec_src"),
    )
    sat_carcass.createOrReplaceTempView("source_sat_carcass")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.sat_carcass_detail USING iceberg AS SELECT * FROM source_sat_carcass LIMIT 0
    """)
    spark.sql("""
        MERGE INTO iceberg.sat_carcass_detail t
        USING source_sat_carcass s
        ON t.carcass_hk = s.carcass_hk
        WHEN NOT MATCHED THEN INSERT *
        -- Add SCD logic: WHEN MATCHED AND hash_diff THEN update eff_to, insert new
    """)

    # Link_Carcass_Process (plant)
    plant_hk = hash_key(col("plant_id"))
    link_carcass_plant = df.select(
        carcass_hk.alias("carcass_hk"),
        plant_hk.alias("plant_hk"),
        lit(load_dts).alias("load_dts"),
        col("slaughter_date").cast("date").alias("process_date"),
    ).distinct()
    link_carcass_plant.createOrReplaceTempView("source_link_carcass_plant")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.link_carcass_plant USING iceberg AS SELECT * FROM source_link_carcass_plant LIMIT 0
    """)
    spark.sql("""
        MERGE INTO iceberg.link_carcass_plant t
        USING source_link_carcass_plant s
        ON t.carcass_hk = s.carcass_hk AND t.plant_hk = s.plant_hk
        WHEN NOT MATCHED THEN INSERT *
    """)

    # Optional: Link to indicator if multi
    indicator_hk = hash_key(col("indicator_id"))
    # Similar MERGE for link_carcass_indicator

    df.unpersist()
    indicator_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

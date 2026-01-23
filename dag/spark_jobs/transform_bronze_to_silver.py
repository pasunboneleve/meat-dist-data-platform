#!/usr/bin/env python3
# PySpark script for Bronze -> Silver DV2 Iceberg
# Run via DataprocCreateBatchOperator
# Filters to target_date partition only (passed via xcom_push)

import hashlib
import logging
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import col, dayofmonth, lit, month, udf, year
from pyspark.sql.types import StringType

# --- CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class DagConfigError(Exception):
    pass


def hash_key(*cols) -> Column:
    """Generate HK as hex(SHA256 of concatenated cols)."""

    def _hash(*col_values):
        key_str = "".join([str(v) for v in col_values if v is not None])
        return hashlib.sha256(key_str.encode()).hexdigest()

    udf_func = udf(_hash, StringType())
    return udf_func(*cols)


def main():
    # Get params from Spark conf (passed via Airflow templating)
    gcp_project = spark.conf.get("spark.sql.gcp_project")
    dataproc_region = spark.conf.get("spark.sql.dataproc_region")
    catalog_name = spark.conf.get("spark.sql.catalog_name", "meat_iceberg_catalog")
    db_name = spark.conf.get("spark.sql.db_name", "silver_meat_market_db")

    spark = (
        SparkSession.builder.appName("bronze-to-silver-dv2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(f"spark.sql.catalog.biglake", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.biglake.catalog-impl",
            "org.apache.iceberg.gcp.biglake.BigLakeCatalog",
        )
        .config(f"spark.sql.catalog.biglake.gcp_project", gcp_project)
        .config(f"spark.sql.catalog.biglake.gcp_location", dataproc_region)
        .config(f"spark.sql.catalog.biglake.blms_catalog", catalog_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )
    try:
        logger.info("Starting Bronze-to-Silver transformation.")
        bronze_bucket = spark.conf.get("spark.sql.bronze_bucket")
        silver_bucket = spark.conf.get("spark.sql.silver_bucket")
        target_date_str = spark.conf.get(
            "spark.sql.target_date_str"
        )  # e.g., "2024-12-27"
        if not target_date_str:
            raise DagConfigError("target_date_str missing.")
        target_date = date.fromisoformat(target_date_str)
        load_dts = datetime.now()
        logger.info(
            "Loaded configuration",
            extra={
                "bronze_bucket": bronze_bucket,
                "silver_bucket": silver_bucket,
                "target_date": target_date_str,
            },
        )

        bronze_path = f"gs://{bronze_bucket}/carcasses/"
        bronze_path += f"year={target_date.year}/month={target_date.month:02d}/day={target_date.day:02d}/*.parquet"

        logger.info(f"Reading source Parquet data from {bronze_path}")
        carcass_df = spark.read.parquet(bronze_path).filter(
            (year("slaughter_date") == target_date.year)
            & (month("slaughter_date") == target_date.month)
            & (dayofmonth("slaughter_date") == target_date.day)
        )
        logger.info(f"Read {carcass_df.count()} rows from bronze.")

        # Read indicator table
        logger.info("Reading indicator data.")
        indicator_df = spark.read.parquet(
            f"gs://{bronze_bucket}/indicator/indicator.parquet"
        )

        # Cache for multiple uses
        carcass_df.cache()
        indicator_df.cache()

        # Read saleyard table
        logger.info("Reading saleyard data.")
        saleyard_df = spark.read.parquet(
            f"gs://{bronze_bucket}/saleyard/saleyard.parquet"
        )
        saleyard_df.cache()

        # Create DB if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS biglake.{db_name}")

        # DV2 Entities (simplified, add SCD/eff dates as needed)
        # Hub_Carcass
        logger.info("Creating and merging Hub_Carcass.")
        hub_carcass = carcass_df.select(
            hash_key(col("carcass_id")).alias("carcass_hk"),
            col("carcass_id"),
            lit(load_dts).alias("load_dts"),
            lit("BRONZE").alias("rec_src"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.hub_carcass (
                carcass_hk STRING,
                carcass_id STRING,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        hub_carcass.createOrReplaceTempView("source_hub_carcass")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.hub_carcass t
            USING source_hub_carcass s
            ON t.carcass_hk = s.carcass_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Hub_Plant
        logger.info("Creating and merging Hub_Plant.")
        hub_plant = carcass_df.select(
            hash_key(col("plant_id")).alias("plant_hk"),
            col("plant_id"),
            lit(load_dts).alias("load_dts"),
            lit("BRONZE").alias("rec_src"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.hub_plant (
                plant_hk STRING,
                plant_id STRING,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        hub_plant.createOrReplaceTempView("source_hub_plant")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.hub_plant t
            USING source_hub_plant s ON t.plant_hk = s.plant_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Hub_Indicator (new)
        logger.info("Creating and merging Hub_Indicator.")
        hub_indicator = indicator_df.select(
            hash_key(col("indicator_id")).alias("indicator_hk"),
            col("indicator_id"),
            lit(load_dts).alias("load_dts"),
            lit("BRONZE").alias("rec_src"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.hub_indicator (
                indicator_hk STRING,
                indicator_id LONG,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        hub_indicator.createOrReplaceTempView("source_hub_indicator")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.hub_indicator t
            USING source_hub_indicator s ON t.indicator_hk = s.indicator_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Hub_Saleyard
        logger.info("Creating and merging Hub_Saleyard.")
        hub_saleyard = saleyard_df.select(
            hash_key(col("saleyard_id")).alias("saleyard_hk"),
            col("saleyard_id"),
            lit(load_dts).alias("load_dts"),
            lit("BRONZE").alias("rec_src"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.hub_saleyard (
                saleyard_hk STRING,
                saleyard_id STRING,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        hub_saleyard.createOrReplaceTempView("source_hub_saleyard")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.hub_saleyard t
            USING source_hub_saleyard s ON t.saleyard_hk = s.saleyard_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Sat_Carcass_Detail (hash diff for SCD2)
        logger.info("Creating and merging Sat_Carcass_Detail.")
        carcass_hk = hash_key(col("carcass_id"))
        sat_carcass = carcass_df.select(
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
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.sat_carcass_detail (
                carcass_hk STRING,
                hscw_kg DECIMAL(10, 2),
                animal_class STRING,
                price_aud_per_kg DECIMAL(10, 2),
                marbling_score LONG,
                quality_score LONG,
                fat_depth_mm LONG,
                total_price_aud DECIMAL(10, 2),
                slaughter_date DATE,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        sat_carcass.createOrReplaceTempView("source_sat_carcass")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.sat_carcass_detail t
            USING source_sat_carcass s
            ON t.carcass_hk = s.carcass_hk
            WHEN NOT MATCHED THEN INSERT *
            -- Add SCD logic: WHEN MATCHED AND hash_diff THEN update eff_to, insert new
        """)

        # Sat_Saleyard_Detail (hash diff for SCD2)
        logger.info("Creating and merging Sat_Saleyard_Detail.")
        sat_saleyard = saleyard_df.select(
            hash_key(col("saleyard_id")).alias("saleyard_hk"),
            col("saleyard_desc"),
            col("state_id"),
            col("nrmr_desc"),
            lit(load_dts).alias("load_dts"),
            lit("BRONZE").alias("rec_src"),
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.sat_saleyard_detail (
                saleyard_hk STRING,
                saleyard_desc STRING,
                state_id STRING,
                nrmr_desc STRING,
                load_dts TIMESTAMP,
                rec_src STRING
            )
        """)
        sat_saleyard.createOrReplaceTempView("source_sat_saleyard")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.sat_saleyard_detail t
            USING source_sat_saleyard s
            ON t.saleyard_hk = s.saleyard_hk
            WHEN NOT MATCHED THEN INSERT *
            -- Add SCD logic: WHEN MATCHED AND hash_diff THEN update eff_to, insert new
        """)

        # Link_Carcass_Process (plant)
        logger.info("Creating and merging Link_Carcass_Process.")
        plant_hk = hash_key(col("plant_id"))
        link_carcass_plant = carcass_df.select(
            carcass_hk.alias("carcass_hk"),
            plant_hk.alias("plant_hk"),
            lit(load_dts).alias("load_dts"),
            col("slaughter_date").cast("date").alias("process_date"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.link_carcass_plant (
                carcass_hk STRING,
                plant_hk STRING,
                load_dts TIMESTAMP,
                process_date DATE
            )
        """)
        link_carcass_plant.createOrReplaceTempView("source_link_carcass_plant")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.link_carcass_plant t
            USING source_link_carcass_plant s
            ON t.carcass_hk = s.carcass_hk AND t.plant_hk = s.plant_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Link_Carcass_Indicator
        logger.info("Creating and merging Link_Carcass_Indicator.")
        indicator_hk = hash_key(col("indicator_id"))
        link_carcass_indicator = carcass_df.select(
            carcass_hk.alias("carcass_hk"),
            indicator_hk.alias("indicator_hk"),
            lit(load_dts).alias("load_dts"),
        ).distinct()
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.link_carcass_indicator (
                carcass_hk STRING,
                indicator_hk STRING,
                load_dts TIMESTAMP
            )
        """)
        link_carcass_indicator.createOrReplaceTempView("source_link_carcass_indicator")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.link_carcass_indicator t
            USING source_link_carcass_indicator s
            ON t.carcass_hk = s.carcass_hk AND t.indicator_hk = s.indicator_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        # Link_Carcass_Saleyard
        logger.info("Creating and merging Link_Carcass_Saleyard.")
        saleyard_hk = hash_key(col("saleyard_id"))
        link_carcass_saleyard = carcass_df.select(
            carcass_hk.alias("carcass_hk"),
            saleyard_hk.alias("saleyard_hk"),
            lit(load_dts).alias("load_dts").distinct(),
        )
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS biglake.{db_name}.link_carcass_saleyard (
                carcass_hk STRING,
                saleyard_hk STRING,
                load_dts TIMESTAMP
            )
        """)
        link_carcass_saleyard.createOrReplaceTempView("source_link_carcass_saleyard")
        spark.sql(f"""
            MERGE INTO biglake.{db_name}.link_carcass_saleyard t
            USING source_link_carcass_saleyard s
            ON t.carcass_hk = s.carcass_hk AND t.saleyard_hk = s.saleyard_hk
            WHEN NOT MATCHED THEN INSERT *
        """)

        logger.info("Unpersisting cached dataframes.")
        carcass_df.unpersist()
        indicator_df.unpersist()
        saleyard_df.unpersist()
        logger.info("Bronze-to-Silver transformation completed successfully.")
        spark.stop()
    except DagConfigError as e:
        logger.error(f"Configuration Error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()


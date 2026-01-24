#!/usr/bin/env python3
# PySpark script for Silver -> Gold Kimball Iceberg
# Run via DataprocCreateBatchOperator

import logging
import sys
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# --- CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class DagConfigError(Exception):
    pass


def run_transform(spark: SparkSession, config: dict):
    """
    Executes the Silver to Gold transformation.
    args:
        spark: The SparkSession object.
        config: A dictionary containing configuration.
    """
    silver_bucket = config.get("silver_bucket")
    gold_bucket = config.get("gold_bucket")
    target_date_str = config.get("target_date_str")
    silver_db = config.get("silver_db_name")
    gold_db = config.get("gold_db_name", "gold")
    load_dts = config.get("load_dts", datetime.now())

    if not all([silver_bucket, gold_bucket, target_date_str, silver_db]):
        raise DagConfigError(f"Missing config. Provided: {config}")

    target_date = date.fromisoformat(target_date_str)  # type: ignore

    logger.info(
        "Loaded configuration",
        extra={
            "silver_bucket": silver_bucket,
            "gold_bucket": gold_bucket,
            "target_date": target_date_str,
            "silver_db": silver_db,
            "gold_db": gold_db,
        },
    )

    # Ensure Gold DB exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS biglake.{gold_db}")

    # 1. Read Silver Data (Iceberg tables from Catalog)
    logger.info("Reading Silver Iceberg tables.")

    # We read directly from the biglake catalog where silver tables reside
    hub_carcass = spark.read.table(f"biglake.{silver_db}.hub_carcass")
    sat_carcass = spark.read.table(f"biglake.{silver_db}.sat_carcass_detail")
    hub_plant = spark.read.table(f"biglake.{silver_db}.hub_plant")
    hub_saleyard = spark.read.table(f"biglake.{silver_db}.hub_saleyard")
    link_carcass_saleyard = spark.read.table(
        f"biglake.{silver_db}.link_carcass_saleyard"
    )
    link_carcass_plant = spark.read.table(f"biglake.{silver_db}.link_carcass_plant")

    # 2. Filter Satellite for incremental processing
    logger.info(f"Filtering satellite data for slaughter date: {target_date}")
    # Using 'slaughter_date' from sat_carcass_detail
    sat_carcass_incremental = sat_carcass.filter(col("slaughter_date") == target_date)

    # 3. Join Logic (Denormalization)
    logger.info("Joining Silver tables...")

    # Start with Sat Carcass (filtered)
    df = sat_carcass_incremental.alias("sat").join(
        hub_carcass.alias("hub"), on="carcass_hk", how="inner"
    )

    # Join Saleyard info
    df = df.join(
        link_carcass_saleyard.alias("lnk_sy"), on="carcass_hk", how="left"
    ).join(hub_saleyard.alias("hub_sy"), on="saleyard_hk", how="left")

    # Join Plant info
    df = df.join(link_carcass_plant.alias("lnk_pl"), on="carcass_hk", how="left").join(
        hub_plant.alias("hub_pl"), on="plant_hk", how="left"
    )

    # Select Final Columns
    fact_df = df.select(
        col("hub.carcass_id"),
        col("hub_pl.plant_id"),
        col("hub_sy.saleyard_id"),
        col("sat.animal_class"),
        col("sat.slaughter_date"),
        col("sat.hscw_kg").cast("decimal(10, 2)"),
        col("sat.price_aud_per_kg").cast("decimal(10, 2)"),
        col("sat.total_price_aud").cast("decimal(10, 2)"),
        col("sat.quality_score").cast("int"),
        col("sat.marbling_score").cast("int"),
        col("sat.fat_depth_mm").cast("int"),
        lit(load_dts).alias("load_dts"),
    )

    # 4. Write to Gold Iceberg Table
    table_name = f"biglake.{gold_db}.fact_carcass_transactions"

    logger.info(f"Creating/Verifying table {table_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            carcass_id STRING,
            plant_id STRING,
            saleyard_id STRING,
            animal_class STRING,
            slaughter_date DATE,
            hscw_kg DECIMAL(10, 2),
            price_aud_per_kg DECIMAL(10, 2),
            total_price_aud DECIMAL(10, 2),
            quality_score INT,
            marbling_score INT,
            fat_depth_mm INT,
            load_dts TIMESTAMP
        )
        PARTITIONED BY (slaughter_date)
    """)

    logger.info(f"Writing fact data for {target_date}...")

    # Register temp view for SQL based insert
    fact_df.createOrReplaceTempView("source_fact")

    # INSERT OVERWRITE for idempotency on the partition
    spark.sql(f"""
        INSERT OVERWRITE {table_name}
        SELECT * FROM source_fact
    """)

    logger.info("Silver-to-Gold transformation completed successfully.")


def main():
    spark = SparkSession.builder.appName("silver-to-gold-kimball").getOrCreate()

    try:
        logger.info("Starting Silver-to-Gold transformation.")

        # Read config from Spark conf (passed by Dataproc Operator)
        conf = spark.conf
        config = {
            "silver_bucket": conf.get("spark.sql.silver_bucket"),
            "gold_bucket": conf.get("spark.sql.gold_bucket"),
            "target_date_str": conf.get("spark.sql.target_date_str"),
            "silver_db_name": conf.get("spark.sql.silver_db_name"),
            "gold_db_name": conf.get("spark.sql.gold_db_name", "gold"),
        }

        run_transform(spark, config)

        spark.stop()
    except DagConfigError as e:
        logger.error(f"Configuration Error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

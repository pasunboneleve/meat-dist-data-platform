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

    # --- 1. Load Silver Data ---
    logger.info("Reading Silver Iceberg tables.")
    hub_carcass = spark.read.table(f"biglake.{silver_db}.hub_carcass")
    sat_carcass = spark.read.table(f"biglake.{silver_db}.sat_carcass_detail")
    hub_plant = spark.read.table(f"biglake.{silver_db}.hub_plant")
    hub_saleyard = spark.read.table(f"biglake.{silver_db}.hub_saleyard")
    link_carcass_saleyard = spark.read.table(
        f"biglake.{silver_db}.link_carcass_saleyard"
    )
    link_carcass_plant = spark.read.table(f"biglake.{silver_db}.link_carcass_plant")

    # Filter Satellite for incremental processing
    # Note: For dimensions, we might want to look at full history or just incremental.
    # For this implementation, we'll process dimensions from the incremental slice
    # effectively assuming new attributes appear in the daily batch.
    sat_carcass_incremental = sat_carcass.filter(col("slaughter_date") == target_date)

    # --- 2. Create/Update Dimensions ---

    # DIM_PLANT
    # Logic: Source from Hub_Plant. Simple Type 1 for now (overwrite/ignore).
    logger.info("Processing DIM_PLANT")
    dim_plant_df = hub_plant.select(
        col("plant_id"),
        lit("Unknown").alias("location"),  # Placeholder as location isn't in Silver yet
    ).distinct()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS biglake.{gold_db}.dim_plant (
            plant_key LONG,
            plant_id STRING,
            location STRING
        )
    """)
    # We use a simple hash or monotonic id for keys in this demo, or just use business key if allowed.
    # Kimball prefers integer surrogate keys.
    # Generating surrogate keys in Spark in parallel is tricky without distinct sequences.
    # We will use dense_rank over order or just hash for now to keep it stateless/simple for this iteration
    # or rely on an internal ID if available. Hub_Plant has plant_hk (hash).
    # Let's simple use creating a predictable integer hash from the ID for this demo
    # to avoid maintaining a stateful sequence map table for now.
    from pyspark.sql.functions import abs as spark_abs
    from pyspark.sql.functions import hash as spark_hash

    dim_plant_prepared = dim_plant_df.withColumn(
        "plant_key", spark_abs(spark_hash("plant_id")).cast("long")
    )
    dim_plant_prepared.createOrReplaceTempView("source_dim_plant")

    # Merge Dim Plant
    spark.sql(f"""
        MERGE INTO biglake.{gold_db}.dim_plant t
        USING source_dim_plant s ON t.plant_id = s.plant_id
        WHEN NOT MATCHED THEN INSERT *
        WHEN MATCHED THEN UPDATE SET location = s.location
    """)

    # DIM_PRODUCT
    # Logic: Derived from distinct attributes in Sat_Carcass (Animal Class, Grade, etc.)
    logger.info("Processing DIM_PRODUCT")
    # Attributes: grade, breed, marbling_score, fat_depth_mm
    # Note: 'grade' and 'breed' might need mappings. map animal_class to grade/breed for now.
    dim_product_source = sat_carcass_incremental.select(
        col("animal_class").alias("grade"),  # Proxy
        col("animal_class").alias("breed"),  # Proxy
        col("marbling_score"),
        col("fat_depth_mm"),
    ).distinct()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS biglake.{gold_db}.dim_product (
            product_key LONG,
            grade STRING,
            breed STRING,
            marbling_score INT,
            fat_depth_mm INT
        )
    """)

    dim_product_prepared = dim_product_source.withColumn(
        "product_key",
        spark_abs(spark_hash("grade", "breed", "marbling_score", "fat_depth_mm")).cast(
            "long"
        ),
    )
    dim_product_prepared.createOrReplaceTempView("source_dim_product")

    spark.sql(f"""
        MERGE INTO biglake.{gold_db}.dim_product t
        USING source_dim_product s ON t.product_key = s.product_key
        WHEN NOT MATCHED THEN INSERT *
    """)

    # DIM_DATE
    # Logic: Ensure the target date exists in dim_date
    logger.info("Processing DIM_DATE")
    # target_date is a date object
    date_key = int(target_date.strftime("%Y%m%d"))
    dim_date_data = [
        {
            "date_key": date_key,
            "full_date": target_date,
            "year": target_date.year,
            "month": target_date.month,
            "day": target_date.day,
        }
    ]
    dim_date_df = spark.createDataFrame(dim_date_data)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS biglake.{gold_db}.dim_date (
            date_key INT,
            full_date DATE,
            year INT,
            month INT,
            day INT
        )
    """)

    dim_date_df.createOrReplaceTempView("source_dim_date")
    spark.sql(f"""
        MERGE INTO biglake.{gold_db}.dim_date t
        USING source_dim_date s ON t.date_key = s.date_key
        WHEN NOT MATCHED THEN INSERT *
    """)

    # DIM_SALEYARD
    # Logic: Source from Hub_Saleyard + Sat_Saleyard_Detail (if joined).
    # Since we didn't explicitly join sat_saleyard to hub_saleyard in the dimension prep block yet,
    # we'll use hub_saleyard and if details are needed we can join sat_saleyard_detail.
    # Looking at silver tables: hub_saleyard, sat_saleyard_detail.
    # We should join them to get descriptions.

    logger.info("Processing DIM_SALEYARD")
    sat_saleyard = spark.read.table(f"biglake.{silver_db}.sat_saleyard_detail")

    # Get latest/distinct saleyard info.
    # For this batch, we can arguably just take all unique saleyards referenced or full update.
    # Full update from Silver Hub+Sat is safest for dimensions usually.
    # Let's do a join of Hub and Sat Saleyard to get attributes.
    dim_saleyard_source = (
        hub_saleyard.join(sat_saleyard, "saleyard_hk", "left")
        .select(
            col("saleyard_id"), col("saleyard_desc"), col("state_id"), col("nrmr_desc")
        )
        .distinct()
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS biglake.{gold_db}.dim_saleyard (
            saleyard_key LONG,
            saleyard_id STRING,
            saleyard_desc STRING,
            state_id STRING,
            nrmr_desc STRING
        )
    """)

    dim_saleyard_prepared = dim_saleyard_source.withColumn(
        "saleyard_key", spark_abs(spark_hash("saleyard_id")).cast("long")
    )
    dim_saleyard_prepared.createOrReplaceTempView("source_dim_saleyard")

    spark.sql(f"""
        MERGE INTO biglake.{gold_db}.dim_saleyard t
        USING source_dim_saleyard s ON t.saleyard_id = s.saleyard_id
        WHEN NOT MATCHED THEN INSERT *
        WHEN MATCHED THEN UPDATE SET
            saleyard_desc = s.saleyard_desc,
            state_id = s.state_id,
            nrmr_desc = s.nrmr_desc
    """)

    # --- 3. Create FACT_SALES ---
    logger.info("Processing FACT_SALES")

    # Join Logic
    # Sat -> Hub Carcass -> Link Plant -> Hub Plant -> Link Saleyard (if needed)
    # We need to lookup keys from Dimensions

    # 3.1 Denormalize
    df = (
        sat_carcass_incremental.alias("sat")
        .join(hub_carcass.alias("hub"), on="carcass_hk", how="inner")
        .join(link_carcass_plant.alias("lnk_pl"), on="carcass_hk", how="left")
        .join(hub_plant.alias("hub_pl"), on="plant_hk", how="left")
        .join(link_carcass_saleyard.alias("lnk_sy"), on="carcass_hk", how="left")
        .join(hub_saleyard.alias("hub_sy"), on="saleyard_hk", how="left")
    )

    # 3.2 Measure Calculations & Key Lookups
    # Product Key Lookup
    # We do a broadcast join since dim_product is small-ish or we calculate hash again
    # Calculating hash is safer if we trust the logic is identical
    df_with_keys = (
        df.withColumn(
            "product_key",
            spark_abs(
                spark_hash(
                    col("sat.animal_class"),
                    col("sat.animal_class"),
                    col("sat.marbling_score"),
                    col("sat.fat_depth_mm"),
                )
            ).cast("long"),
        )
        .withColumn(
            "plant_key", spark_abs(spark_hash(col("hub_pl.plant_id"))).cast("long")
        )
        .withColumn(
            "saleyard_key",
            spark_abs(spark_hash(col("hub_sy.saleyard_id"))).cast("long"),
        )
        .withColumn("date_key", lit(date_key).cast("int"))
    )

    fact_sales_df = df_with_keys.select(
        col("date_key"),
        col("product_key"),
        col("plant_key"),
        col("saleyard_key"),
        col("hub.carcass_id"),
        lit("UNKNOWN").alias(
            "batch_id"
        ),  # Placeholder, batch_id not in current silver Sat payload
        col("sat.total_price_aud").cast("decimal(10, 2)").alias("total_price"),
        col("sat.hscw_kg").cast("decimal(10, 2)").alias("total_weight_kg"),
        lit(None).cast("float").alias("average_yield"),  # Placeholder
        lit(load_dts).alias("load_dts"),
    )

    # Add Facts Primary Key (Surrogate)
    fact_sales_df = fact_sales_df.withColumn(
        "fact_key", spark_abs(spark_hash("carcass_id", "date_key")).cast("long")
    )

    # Validations / Deduplication
    # Ensure one row per carcass per day

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS biglake.{gold_db}.fact_sales (
            fact_key LONG,
            date_key INT,
            product_key LONG,
            plant_key LONG,
            saleyard_key LONG,
            carcass_id STRING,
            batch_id STRING,
            total_price DECIMAL(10, 2),
            total_weight_kg DECIMAL(10, 2),
            average_yield FLOAT,
            load_dts TIMESTAMP
        )
        PARTITIONED BY (date_key)
    """)

    fact_sales_df.createOrReplaceTempView("source_fact_sales")

    # Overwrite partition for robustness
    spark.sql(f"""
        INSERT OVERWRITE biglake.{gold_db}.fact_sales
        SELECT * FROM source_fact_sales
    """)

    logger.info("Silver-to-Gold (Kimball) transformation completed successfully.")


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

import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date

# --- CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


class DagConfigError(Exception):
    """Custom exception for missing DAG configuration."""

    pass


def get_spark_session() -> SparkSession:
    """
    Initializes and returns a Spark session with necessary configurations for Iceberg.
    The configurations are expected to be set by the Dataproc operator.
    """
    return SparkSession.builder.appName("SilverToGoldKimball").getOrCreate()


def get_config(spark: SparkSession) -> dict:
    """
    Retrieves configuration parameters from the Spark session.
    """
    conf = spark.conf
    required_configs = [
        "spark.sql.gold_bucket",
        "spark.sql.target_date_str",
    ]
    config = {}
    for key in required_configs:
        value = conf.get(key, None)
        if not value:
            raise DagConfigError(f"Required Spark config '{key}' is not set.")
        config[key.split(".")[-1]] = value

    # Convert target_date_str to date object
    config["target_date"] = datetime.fromisoformat(config["target_date_str"]).date()
    return config


def main():
    """
    Main ETL logic for transforming Silver Data Vault tables into a Gold Kimball fact table.
    """
    spark = get_spark_session()
    try:
        config = get_config(spark)
        logger.info(f"Loaded configuration: {config}")

        target_date = config["target_date"]
        gold_bucket = config["gold_bucket"]

        # 1. Load Silver Iceberg Tables
        logger.info("Loading Silver Iceberg tables...")
        hub_carcass_df = spark.read.table("spark_catalog.default.hub_carcass")
        sat_carcass_detail_df = spark.read.table(
            "spark_catalog.default.sat_carcass_detail"
        )
        hub_plant_df = spark.read.table("spark_catalog.default.hub_plant")
        hub_saleyard_df = spark.read.table("spark_catalog.default.hub_saleyard")
        link_carcass_saleyard_df = spark.read.table(
            "spark_catalog.default.link_carcass_saleyard"
        )

        # 2. Filter Satellite for incremental processing based on load date
        logger.info(f"Filtering satellite data for load date: {target_date}")
        sat_carcass_incremental_df = sat_carcass_detail_df.where(
            to_date(col("load_dts")) == target_date
        )

        if sat_carcass_incremental_df.rdd.isEmpty():
            logger.info("No new data in sat_carcass_detail to process. Exiting.")
            sys.exit(0)

        # 3. Denormalize by joining Hubs, Satellites, and Links
        logger.info("Joining Silver tables to create denormalized fact view...")

        # Join carcass hub and sat
        carcass_denormalized = sat_carcass_incremental_df.join(
            hub_carcass_df, "carcass_hk", "inner"
        )

        # Join with saleyard info
        carcass_denormalized = (
            carcass_denormalized.join(link_carcass_saleyard_df, "carcass_hk", "left")
            .join(hub_saleyard_df, "saleyard_hk", "left")
        )

        # Join with plant info (assuming plant_id business key is on satellite)
        carcass_denormalized = carcass_denormalized.join(hub_plant_df, "plant_id", "left")

        # 4. Create the Gold Fact Table
        logger.info("Creating fact_carcass_transactions table...")
        fact_carcass_transactions = carcass_denormalized.select(
            # Degenerate Dimensions & Business Keys
            col("carcass_id"),
            col("plant_id"),
            col("saleyard_id"),
            col("animal_class"),
            # Date Dimension Key
            to_date(col("slaughter_date")).alias("slaughter_date"),
            # Measures
            col("hscw_kg").cast("decimal(10, 2)"),
            col("price_aud_per_kg").cast("decimal(10, 2)"),
            col("total_price_aud").cast("decimal(10, 2)"),
            col("quality_score").cast("integer"),
            col("marbling_score").cast("integer"),
            col("fat_depth_mm").cast("integer"),
            # Audit columns
            col("load_dts"),
        )

        # 5. Add partitioning columns based on slaughter_date
        fact_carcass_transactions = (
            fact_carcass_transactions.withColumn(
                "year", date_format(col("slaughter_date"), "yyyy")
            )
            .withColumn("month", date_format(col("slaughter_date"), "MM"))
            .withColumn("day", date_format(col("slaughter_date"), "dd"))
        )

        # 6. Write to Gold GCS bucket
        output_path = f"gs://{gold_bucket}/fact_carcass_transactions"
        logger.info(f"Writing fact table to {output_path}")

        fact_carcass_transactions.write.partitionBy("year", "month", "day").mode(
            "overwrite"
        ).parquet(output_path)

        logger.info("Silver-to-Gold transformation completed successfully.")

    except DagConfigError as e:
        logger.error(f"Configuration Error: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

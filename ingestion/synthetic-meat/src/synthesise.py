import logging
import os
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from logging import Logger
from pathlib import Path
from typing import Any, List

import functions_framework
import gcsfs
import google.cloud.logging
import polars as pl
import pyarrow.parquet as pq
import requests
import tomllib
from dotenv import load_dotenv
from faker import Faker
from google.cloud.logging.handlers import CloudLoggingHandler

load_dotenv()


def project_name() -> str:
    """
    Get project name from pyproject.toml.
    """
    pyproject_path = Path("pyproject.toml")
    with pyproject_path.open("rb") as toml:
        project = tomllib.load(toml)
    if "project" in project and "name" in project["project"]:
        return project["project"]["name"]
    return "synthetic-meat"


def init_logging() -> Logger:
    client = google.cloud.logging.Client()
    if os.getenv("ENV") == "LOCAL":
        import structlog

        processors = [
            structlog.stdlib.ExtraAdder(),
            # Add level (shows [info], [error], etc.)
            structlog.stdlib.add_log_level,
            # Timestamp
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        ]
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=True),
            foreign_pre_chain=processors,
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
    else:
        handler = CloudLoggingHandler(client, name=project_name())

    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    try:
        log_level = getattr(logging, log_level_str)
    except AttributeError:
        log_level = logging.INFO

    root = logging.getLogger()
    root.setLevel(log_level)
    root.addHandler(handler)

    return root


logger = init_logging()

# --- Configuration ---
BUCKET_NAME = os.environ.get("BRONZE_BUCKET", "meatislife-bronze-bucket")
MLA_API_URL = "https://api-mlastatistics.mla.com.au"

# --- Helper Functions ---


def fetch_data(endpoint: str, params: dict[str, Any]) -> pl.DataFrame:
    """
    Fetches data from the MLA Statistics API.
    """

    logger.info(
        "Requesting data from MLA API",
        extra=dict(endpoint=endpoint, params=params),
    )
    try:
        response = requests.get(f"{MLA_API_URL}{endpoint}", params=params)
        response.raise_for_status()

        data = response.json()

        if "total number rows" in data:
            logger.info(
                f"API returned {data['total number rows']} rows.",
                extra=dict(total_rows=data["total number rows"]),
            )

        df = pl.DataFrame(data["data"])
        # Preprocessing from the original load_base_data function
        if df.is_empty():
            logger.debug("fetched data empty")
        else:
            logger.debug(
                "fetched data",
                extra=dict(shape=df.shape, columns=df.columns),
            )
        return df

    except requests.exceptions.HTTPError as http_err:
        response = http_err.response
        if response.status_code >= 500:
            logger.error(
                "MLA API server error",
                extra={
                    "http_request": dict(
                        requestMethod=response.request.method,
                        requestURL=response.request.url,
                        status=response.status_code,
                        responseBody=response.text,
                        params=params,
                        endpoint=endpoint,
                    )
                },
            )
            try:
                error_json = response.json()
                error_msg = error_json.get("Message", str(error_json))
            except ValueError:
                error_msg = response.text[:1000]  # truncate if not json
            raise RuntimeError(
                f"MLA API {response.status_code}: {error_msg}"
            ) from http_err
        else:
            logger.warning(
                "MLA API client error",
                extra={
                    "http_request": dict(
                        requestMethod=response.request.method,
                        requestURL=response.request.url,
                        status=response.status_code,
                        responseBody=response.text,
                        params=params,
                        endpoint=endpoint,
                    )
                },
            )
            return pl.DataFrame()
    except requests.exceptions.RequestException as e:
        logger.warning(
            "MLA API request error",
            extra=dict(params=params, endpoint=endpoint, error=str(e)),
        )
        return pl.DataFrame()


def fetch_saleyard() -> pl.DataFrame:
    return fetch_data("/saleyard", {})


def fetch_base_data(params) -> pl.DataFrame:
    """
    Fetches cattle pricing data from the MLA Statistics API for the last 90 days.
    """
    endpoint = "/report/6"

    df = fetch_data(endpoint, params)

    # Preprocessing from the original load_base_data function
    if "indicator_desc" in df.columns:
        df = df.rename({"indicator_desc": "category"})
    if "calendar_date" in df.columns:
        df = df.rename({"calendar_date": "report_date"})
    # Add indicator_id from params for linking
    if len(df):
        df = df.with_columns(pl.lit(params["indicatorID"]).alias("indicator_id"))
    logger.debug("base data loaded", extra=dict(shape=df.shape))
    return df


def load_base_data(file_path: Path) -> pl.DataFrame:
    """Loads and preprocesses the base market data from a JSON fixture file."""
    if not file_path.exists():
        logger.warning(
            "Fixture file not found",
            extra=dict(file_path=str(file_path)),
        )
        return pl.DataFrame()

    try:
        df = pl.read_json(file_path)
        # The `generate_synthetic_carcasses` function expects a 'category' column
        # to derive animal classes from. We'll use 'indicator_desc'.
        if "indicator_desc" in df.columns:
            df = df.rename({"indicator_desc": "category"})
        else:
            logger.warning("indicator_desc column not found in fixture")
            return pl.DataFrame()

        # The API provides 'calendar_date', but we use 'report_date' internally.
        if "calendar_date" in df.columns:
            df = df.rename({"calendar_date": "report_date"})
        else:
            logger.warning("calendar_date column not found in fixture")
            return pl.DataFrame()

        return df
    except Exception as e:
        logger.error("Error reading fixture JSON", extra=dict(error=str(e)))
        return pl.DataFrame()


def generate_synthetic_carcasses(
    base_df: pl.DataFrame, target_date: date
) -> pl.DataFrame:
    """
    Generates synthetic carcass data based on distributions from the base data
    for a specific target date.
    """
    fake = Faker("en_AU")

    # Try to derive parameters from the entry in base_df for the target_date
    logger.debug("base_df for generation", extra=dict(shape=base_df.shape))
    if base_df.shape[0] > 1:
        base_df = base_df.head(1)

    if "head_count" in base_df.columns:
        head_count = base_df.get_column("head_count").item()
    else:
        head_count = 0
    num_records = int(head_count) if int(head_count) > 0 else 0

    price_value = base_df.get_column("indicator_value").item()
    avg_price = float(price_value) / 100.0
    price_std_dev = 0.75

    categories = base_df["category"].unique().to_list()
    saleyard_id = base_df.get_column("saleyard_id").item()
    indicator_id = base_df.get_column("indicator_id").item()

    if num_records == 0:
        logger.info(
            "Head count was 0, generating no records",
            extra=dict(target_date=target_date, head_count=head_count),
        )
        return pl.DataFrame()

    data = {
        "carcass_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "rfid_tag": [fake.ean(length=13) for _ in range(num_records)],
        "slaughter_date": [
            datetime.combine(target_date, fake.time_object()).isoformat()
            for _ in range(num_records)
        ],
        "plant_id": [f"P{random.randint(1, 5):02d}" for _ in range(num_records)],
        "animal_class": [random.choice(categories) for _ in range(num_records)],
        "indicator_id": [indicator_id] * num_records,
        "hscw_kg": [
            round(random.normalvariate(320, 40), 2) for _ in range(num_records)
        ],
        "price_aud_per_kg": [
            round(random.normalvariate(avg_price, price_std_dev), 2)
            for _ in range(num_records)
        ],
        "quality_score": [random.randint(1, 100) for _ in range(num_records)],
        "marbling_score": [random.randint(1, 9) for _ in range(num_records)],
        "fat_depth_mm": [random.randint(3, 22) for _ in range(num_records)],
        "saleyard_id": [saleyard_id] * num_records,
    }

    synthetic_df = pl.DataFrame(data)
    synthetic_df = synthetic_df.with_columns(
        (pl.col("hscw_kg") * pl.col("price_aud_per_kg")).alias("total_price_aud")
    )
    return synthetic_df


def write_unpartitioned_to_gcs(df: pl.DataFrame, bucket_name: str, name: str):
    """
    Writes a table to GCS, without partition.
    """
    if not bucket_name:
        raise ValueError("GCS bucket name is not configured via BRONZE_BUCKET env var.")

    if df.is_empty():
        logger.info(
            "DataFrame empty, skipping unpartitioned write",
            extra=dict(table=name),
        )
        return

    gcs_base_path = f"{bucket_name}/{name}/{name}.parquet"

    logger.info(
        "Writing unpartitioned records to GCS",
        extra=dict(record_count=len(df), gcs_path=gcs_base_path),
    )
    # Convert Polars DF to PyArrow Table
    table = df.to_arrow()

    # Create GCS filesystem which uses Application Default Credentials
    fs = gcsfs.GCSFileSystem()

    # Write partitioned dataset using PyArrow
    pq.write_table(
        table,
        where=gcs_base_path,
        filesystem=fs,
    )
    logger.info("Write unpartitioned to GCS successful", extra=dict(table=name))


def write_to_gcs(
    df: pl.DataFrame,
    bucket_name: str,
    target_date: date,
    partition_cols: List[str] = ["year", "month", "day"],
):
    """Writes a Polars DataFrame to GCS as a partitioned Parquet dataset using PyArrow."""
    if not bucket_name:
        raise ValueError("GCS bucket name is not configured via BRONZE_BUCKET env var.")

    if df.is_empty():
        logger.info(
            "DataFrame empty, skipping partitioned write",
            extra=dict(target_date=target_date),
        )
        return

    # Add date parts as columns for partitioning
    df_with_partitions = df.with_columns(
        pl.lit(target_date.year).alias("year"),
        pl.lit(target_date.month).alias("month"),
        pl.lit(target_date.day).alias("day"),
    )

    gcs_base_path = f"{bucket_name}/carcasses"

    logger.info(
        "Writing partitioned records to GCS",
        extra={
            "json_fields": dict(
                record_count=len(df_with_partitions),
                base_path=gcs_base_path,
                partition_cols=partition_cols,
            )
        },
    )

    # Convert Polars DF to PyArrow Table
    table = df_with_partitions.to_arrow()

    # Create GCS filesystem which uses Application Default Credentials
    fs = gcsfs.GCSFileSystem()

    # Write partitioned dataset using PyArrow
    pq.write_to_dataset(
        table,
        root_path=gcs_base_path,
        partition_cols=partition_cols,
        filesystem=fs,
        basename_template=f"{uuid.uuid4()}-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    logger.info("Write partitioned to GCS successful")


def synthesise_and_write(base_data: pl.DataFrame, from_date: date) -> None:
    """
    We may fetch many dates, but we need to synthesise and write singly.
    """

    batch_plant_id = f"P{random.randint(1, 5):02d}"

    logger.debug(
        "synthesising carcasses",
        extra=dict(base_shape=base_data.shape),
    )
    synthetic_data = generate_synthetic_carcasses(base_data, from_date)
    # Overwrite plant_id with the one for this batch
    synthetic_data = synthetic_data.with_columns(
        pl.lit(batch_plant_id).alias("plant_id")
    )

    logger.debug(
        "writing synthetic data to GCS",
        extra=dict(synthetic_shape=synthetic_data.shape),
    )
    write_to_gcs(synthetic_data, BUCKET_NAME, from_date)


def workflow(params: dict[str, Any]) -> str | None:
    """
    Single download and write workflow, to be parallelised.
    """
    try:
        base_data = fetch_base_data(params)
        if base_data.is_empty():
            logger.info(
                f"Skipping workflow: invalid base_data in {params['saleyardId']}",
                extra={
                    "json_fields": dict(
                        params=params,
                        height=base_data.height,
                        width=base_data.width,
                        columns=list(base_data.columns),
                    )
                },
            )
            return None

        for report_date, report in base_data.group_by("report_date"):
            from_date = datetime.fromisoformat(report_date[0]).date()
            synthesise_and_write(report, from_date)
        return f"Data generation and upload complete for {params}"
    except Exception as e:
        # Log exceptions from within the worker process
        logger.error(
            "Error in workflow",
            extra=dict(params=params, error=str(e), exc_info=True),
        )
        return None


@functions_framework.http
def generate_and_upload(request):
    """Cloud Function entry point. Generates synthetic data and uploads it to GCS.
    Accepts a 'target_date' (YYYY-MM-DD) JSON field. Defaults to yesterday.

    If backfill is intended, instead of 'target_date' use 'from_date'
    and 'to_date'. The HTTP requests run in parallel, but for
    simplicity (they're fast) the fake data creation and writing to
    GCS are sequential (after the data is fetched).
    """

    try:
        if request.is_json and request.get_json().get("table"):
            table = request.get_json().get("table")
            table_data = fetch_data(f"/{table}", {})
            write_unpartitioned_to_gcs(
                df=table_data, bucket_name=BUCKET_NAME, name=table
            )
            return ("Raw data upload complete.", 200)

        # Accommodate POST (json body) requests for backfills and manual
        if request.is_json and request.get_json().get("target_date"):
            target_date_str = request.get_json().get("target_date")
            target_date = datetime.fromisoformat(target_date_str).date()

            # prepare params
            from_date = target_date - timedelta(days=1)
            from_date_str = from_date.isoformat()
            to_date_str = target_date.isoformat()

        elif (
            request.is_json
            and request.get_json().get("from_date")
            and request.get_json().get("to_date")
        ):
            jdata = request.get_json()
            from_date_str = jdata.get("from_date")
            to_date_str = jdata.get("to_date")

        else:  # fetch yesterday
            target_date = datetime.now(UTC).date() - timedelta(days=1)
            from_date_str = target_date.isoformat()
            to_date_str = target_date.isoformat()

        # Fetch live data from the MLA API.
        saleyards = fetch_saleyard()

        params = [
            {
                "indicatorID": indicator,
                "saleyardID": saleyard,
                "fromDate": from_date_str,
                "toDate": to_date_str,
                "page": 1,
            }
            for saleyard in saleyards["saleyard_id"].to_list()
            for indicator in list(range(4))
        ]

        for param in params:
            result = workflow(param)
            if result:
                logger.info("workflow result", extra=dict(message=result))
        return ("Data generation and upload complete.", 200)

    except Exception as e:
        logger.exception("Error in function execution")
        return (f"An internal error occurred: {e}", 500)

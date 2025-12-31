import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import functions_framework
import polars as pl
import requests
from faker import Faker
from google.cloud import storage

# --- Configuration ---
BUCKET_NAME = os.environ.get("BRONZE_BUCKET")
MLA_API_URL = "https://api-mlastatistics.mla.com.au"

# --- Helper Functions ---


def fetch_base_data() -> pl.DataFrame:
    """
    Fetches cattle pricing data from the MLA Statistics API for the last 90 days.
    """
    to_date = datetime.utcnow() - timedelta(days=1)
    from_date = to_date - timedelta(days=90)
    from_date_str = from_date.strftime("%Y-%m-%d")
    to_date_str = to_date.strftime("%Y-%m-%d")

    endpoint = "/report/6"
    params = {
        "indicatorID": 3,
        "saleyardID": "DAL",
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    print(f"Requesting data from {MLA_API_URL}{endpoint} with params: {params}")
    try:
        response = requests.get(f"{MLA_API_URL}{endpoint}", params=params)
        response.raise_for_status()

        data = response.json().get("data", [])
        if not data:
            print("Warning: No data found in API response. Using fallback values.")
            return pl.DataFrame()

        df = pl.DataFrame(data)
        # Preprocessing from the original load_base_data function
        if "indicator_desc" in df.columns:
            df = df.rename({"indicator_desc": "category"})
        if "calendar_date" in df.columns:
            df = df.rename({"calendar_date": "report_date"})
        return df

    except requests.exceptions.RequestException as e:
        print(f"Error fetching base data from API: {e}. Using fallback values.")
        return pl.DataFrame()


def load_base_data(file_path: Path) -> pl.DataFrame:
    """Loads and preprocesses the base market data from a JSON fixture file."""
    if not file_path.exists():
        print(
            f"Warning: Fixture file not found at {file_path}. Returning empty DataFrame."
        )
        return pl.DataFrame()

    try:
        df = pl.read_json(file_path)
        # The `generate_synthetic_carcasses` function expects a 'category' column
        # to derive animal classes from. We'll use 'indicator_desc'.
        if "indicator_desc" in df.columns:
            df = df.rename({"indicator_desc": "category"})
        else:
            print(
                "Warning: 'indicator_desc' not found in fixture. Using fallback categories."
            )
            return pl.DataFrame()

        # The API provides 'calendar_date', but we use 'report_date' internally.
        if "calendar_date" in df.columns:
            df = df.rename({"calendar_date": "report_date"})
        else:
            print(
                "Warning: 'calendar_date' not found in fixture. Cannot determine report dates."
            )
            return pl.DataFrame()

        return df
    except Exception as e:
        print(f"Error reading or processing JSON file: {e}")
        return pl.DataFrame()


def generate_synthetic_carcasses(
    base_df: pl.DataFrame, target_date: date
) -> pl.DataFrame:
    """
    Generates synthetic carcass data based on distributions from the base data
    for a specific target date.
    """
    fake = Faker("en_AU")

    # Default fallback values
    num_records = 1000
    avg_price = 8.50  # AUD per kg
    price_std_dev = 0.75
    categories = ["Yearling", "Cow", "Bull", "Steer"]

    # Try to derive parameters from the entry in base_df for the target_date
    if not base_df.is_empty():
        stat_for_date = base_df.filter(
            pl.col("report_date").str.to_date() == target_date
        ).head(1)

        if not stat_for_date.is_empty():
            head_count = stat_for_date.get_column("head_count").item()
            num_records = int(head_count) if head_count and int(head_count) > 0 else 0

            price_value = stat_for_date.get_column("indicator_value").item()
            if price_value:
                avg_price = float(price_value) / 100.0
        else:
            print(
                f"Warning: No market data found for target date {target_date}. "
                "Using fallback values."
            )

        unique_categories = base_df["category"].unique().to_list()
        if unique_categories:
            categories = unique_categories

    if num_records == 0:
        print(f"Note: Head count for {target_date} was 0. Generating no records.")
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
    }

    synthetic_df = pl.DataFrame(data)
    synthetic_df = synthetic_df.with_columns(
        (pl.col("hscw_kg") * pl.col("price_aud_per_kg")).alias("total_price_aud")
    )
    return synthetic_df


def write_to_gcs(df: pl.DataFrame, bucket_name: str, target_date: date):
    """Writes a Polars DataFrame to GCS, partitioned by plant and date."""
    if not bucket_name:
        raise ValueError("GCS bucket name is not configured via BRONZE_BUCKET env var.")

    if df.is_empty():
        print(f"DataFrame is empty. Skipping write to GCS for date {target_date}.")
        return

    # Add date parts as columns for partitioning
    df_with_partitions = df.with_columns(
        pl.lit(target_date.year).alias("year"),
        pl.lit(target_date.month).alias("month"),
        pl.lit(target_date.day).alias("day"),
    )

    gcs_base_path = f"gs://{bucket_name}/carcasses/"

    print(
        f"Writing {len(df_with_partitions)} records to {gcs_base_path} partitioned by "
        "plant_id, year, month, day..."
    )

    # Polars will create the hive-style partitions automatically
    df_with_partitions.write_parquet(
        gcs_base_path,
        partition_by=["plant_id", "year", "month", "day"],
        use_pyarrow=True,
        pyarrow_options={"partition_filename_template": f"batch-{uuid.uuid4()}.parquet"},
    )
    print("Write to GCS successful.")


@functions_framework.http
def generate_and_upload(request):
    """
    Cloud Function entry point. Generates synthetic data and uploads it to GCS.
    Accepts a 'target_date' (YYYY-MM-DD) query parameter. Defaults to yesterday.
    """
    try:
        target_date_str = None
        # Accommodate both GET (query params) and POST (json body) requests
        if request.args and "target_date" in request.args:
            target_date_str = request.args.get("target_date")
        elif request.is_json and request.get_json().get("target_date"):
            target_date_str = request.get_json().get("target_date")

        if target_date_str:
            try:
                target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
            except ValueError:
                return (
                    "Invalid date format for 'target_date'. Please use YYYY-MM-DD.",
                    400,
                )
        else:
            target_date = datetime.utcnow().date() - timedelta(days=1)

        # Fetch live data from the MLA API.
        base_data = fetch_base_data()
        if base_data.is_empty():
            print(
                "Warning: Base data is empty or could not be fetched. "
                "Using fallback values."
            )

        batch_plant_id = f"P{random.randint(1, 5):02d}"

        synthetic_data = generate_synthetic_carcasses(base_data, target_date)
        # Overwrite plant_id with the one for this batch
        synthetic_data = synthetic_data.with_columns(
            pl.lit(batch_plant_id).alias("plant_id")
        )

        write_to_gcs(synthetic_data, BUCKET_NAME, target_date)

        return ("Data generation and upload complete.", 200)

    except Exception as e:
        print(f"Error in function execution: {e}")
        return (f"An internal error occurred: {e}", 500)

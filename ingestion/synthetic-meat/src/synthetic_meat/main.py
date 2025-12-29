import os
import uuid
import random
from datetime import datetime
from pathlib import Path

import polars as pl
from faker import Faker
from google.cloud import storage
import functions_framework

# --- Configuration ---
BUCKET_NAME = os.environ.get("BRONZE_BUCKET")
# For local testing, we can use the downloaded fixture. In a real deployment,
# this base data might come from a GCS location or be packaged with the function.
FIXTURE_PATH = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "market_report.xlsx"

# --- Helper Functions ---

def load_base_data(file_path: Path) -> pl.DataFrame:
    """Loads and preprocesses the base market data from an Excel file."""
    if not file_path.exists():
        print(f"Warning: Fixture file not found at {file_path}. Returning empty DataFrame.")
        return pl.DataFrame()

    try:
        # The actual data in the report may start several rows down.
        # This approach is brittle and depends on the specific report format.
        df = pl.read_excel(file_path, read_options={"skip_rows": 2})
        df = df.rename({df.columns[0]: "category", df.columns[1]: "indicator"})
        df = df.filter(pl.col("category").is_not_null())
        return df
    except Exception as e:
        print(f"Error reading or processing Excel file: {e}")
        return pl.DataFrame()

def generate_synthetic_carcasses(base_df: pl.DataFrame, num_records: int = 1000) -> pl.DataFrame:
    """Generates synthetic carcass data based on distributions from the base data."""
    fake = Faker("en_AU")
    
    # Fallback values if base data is unavailable or cannot be parsed
    avg_price = 8.50  # AUD per kg
    price_std_dev = 0.75
    categories = ["Yearling", "Cow", "Bull", "Steer"]

    # Try to derive categories from the loaded data
    if not base_df.is_empty():
        unique_categories = base_df["category"].unique().to_list()
        if unique_categories:
            categories = unique_categories

    data = {
        "carcass_id": [str(uuid.uuid4()) for _ in range(num_records)],
        "rfid_tag": [fake.ean(length=13) for _ in range(num_records)],
        "slaughter_date": [fake.date_time_this_year().isoformat() for _ in range(num_records)],
        "plant_id": [f"P{random.randint(1, 5):02d}" for _ in range(num_records)],
        "animal_class": [random.choice(categories) for _ in range(num_records)],
        "hscw_kg": [round(random.normalvariate(320, 40), 2) for _ in range(num_records)],
        "price_aud_per_kg": [round(random.normalvariate(avg_price, price_std_dev), 2) for _ in range(num_records)],
        "quality_score": [random.randint(1, 100) for _ in range(num_records)],
        "marbling_score": [random.randint(1, 9) for _ in range(num_records)],
        "fat_depth_mm": [random.randint(3, 22) for _ in range(num_records)],
    }
    
    synthetic_df = pl.DataFrame(data)
    synthetic_df = synthetic_df.with_columns(
        (pl.col("hscw_kg") * pl.col("price_aud_per_kg")).alias("total_price_aud")
    )
    return synthetic_df

def write_to_gcs(df: pl.DataFrame, bucket_name: str, plant_id: str):
    """Writes a Polars DataFrame to GCS as a partitioned Parquet file."""
    if not bucket_name:
        raise ValueError("GCS bucket name is not configured via BRONZE_BUCKET env var.")

    now = datetime.utcnow()
    batch_id = uuid.uuid4()
    
    gcs_path = (
        f"gs://{bucket_name}/carcasses/"
        f"plant_id={plant_id}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{batch_id}.parquet"
    )
    
    print(f"Writing {len(df)} records to {gcs_path}...")
    df.write_parquet(gcs_path)
    print("Write to GCS successful.")


@functions_framework.http
def generate_and_upload(request):
    """
    Cloud Function entry point. Generates synthetic data and uploads it to GCS.
    """
    try:
        # TODO: Replace fixture loading with a robust data fetching mechanism
        # directly from the MLA website for production use.
        base_data = load_base_data(FIXTURE_PATH)
        if base_data.is_empty():
            print("Warning: Base data is empty or could not be loaded. Using fallback values.")

        batch_plant_id = f"P{random.randint(1, 5):02d}"

        synthetic_data = generate_synthetic_carcasses(base_data, num_records=1000)
        synthetic_data = synthetic_data.with_columns(pl.lit(batch_plant_id).alias("plant_id"))

        write_to_gcs(synthetic_data, BUCKET_NAME, batch_plant_id)
        
        return ("Data generation and upload complete.", 200)

    except Exception as e:
        print(f"Error in function execution: {e}")
        return (f"An internal error occurred: {e}", 500)

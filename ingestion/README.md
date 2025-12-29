# Ingestion

This directory contains the data ingestion components for the data platform.

## `synthetic-meat`

This sub-directory holds the source code for the Google Cloud Function responsible for generating synthetic meat processing data. The function is designed to:

1.  **Fetch Baseline Data**: It queries a public API (MLA Statistics) to get real-world aggregate data on livestock head counts and prices. This data is used as a statistical baseline for a given day.
2.  **Generate Synthetic Records**: It creates a larger, more granular dataset of fictional individual carcass records. The number of records and average price are derived from the baseline data.
3.  **Write to Bronze Layer**: The generated data is written in Parquet format to the bronze GCS bucket, partitioned by date and processing plant.

This function serves as the entry point for raw data into the lakehouse.

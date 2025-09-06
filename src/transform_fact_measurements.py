"""
Transforms raw meausurement data and uploads it to S3.
"""

# Imports
import os
import logging
from pathlib import Path
import re
import datetime as dt
from dotenv import load_dotenv
import boto3
import pandas as pd

dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

from .utils.extract_openaq_utils import (
    read_json_from_s3,
    find_latest_s3_key,
    create_s3_key,
    upload_bytes_to_s3,
)
from .utils.transform_utils import (
    list_s3_keys_by_prefix,
    transform_records_to_df,
    df_to_parquet,
    archive_s3_file,
)

# Constants
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/measurements"
PROCESSED_PREFIX = "processed/fact_measurements"
REQUIRED_DIMENSION_TABLE = "dim_sensor"
SENSOR_PREFIX = "processed/dim_sensor"

# Transformation parameters
COLUMN_RENAME_MAP = {
    "value": "value",
    "parameter_id": "openaq_parameter_id",
    "period_datetimeFrom_utc": "utc_timestamp",
    "openaq_sensor_id": "openaq_sensor_id",
    "date_id": "date_id",
    "time_id": "time_id",
    # "location_id": "location_id", # location_id is being merged manually
}

FINAL_SCHEMA = {
    COLUMN_RENAME_MAP["value"]: "float64",
    COLUMN_RENAME_MAP["parameter_id"]: "Int64",
    COLUMN_RENAME_MAP["period_datetimeFrom_utc"]: "datetime64[ms, UTC]",
    COLUMN_RENAME_MAP["openaq_sensor_id"]: "Int64",
    COLUMN_RENAME_MAP["date_id"]: "Int64",
    COLUMN_RENAME_MAP["time_id"]: "Int64",
    # COLUMN_RENAME_MAP["location_id"]: "Int64", # location_id is being merged manually
    "ingest_ts": "datetime64[ns, UTC]",
}

FINAL_COLUMNS = list(FINAL_SCHEMA.keys())

DEDUPLICATION_SUBSET = [
    "openaq_sensor_id",
    "utc_timestamp",
    "openaq_parameter_id",
    "openaq_location_id",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    """Orchestrates the transformation of measurement (hourly) files."""

    logging.info("Starting measurements transformation (hourly) task.")

    s3 = boto3.client("s3")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)

    logging.info(f"Loading required dimension table: {REQUIRED_DIMENSION_TABLE}")

    dim_sensor_key = find_latest_s3_key(s3, BUCKET, SENSOR_PREFIX, ".parquet")
    dim_sensor_df = pd.read_parquet(f"s3://{BUCKET}/{dim_sensor_key}")

    if not raw_keys:
        logging.warning("No raw files found to process. Exiting.")
        return

    all_records = []

    logging.info(f"Found {len(raw_keys)} raw files to process.")

    # Loop through each file
    for source_key in raw_keys:
        logging.info(f"--- Processing file: {source_key} ---")

        # Get sensor_id from this specific file
        try:
            sensor_id_str = re.search(
                r"._sensor_(\d+)", source_key, re.IGNORECASE
            ).group(1)
            sensor_id = int(sensor_id_str)
        except AttributeError:
            logging.warning(
                f"Could not find sensor_id in filename: {source_key}. Skipping file."
            )
            continue

        # Read the list of page-responses for this file
        pages = read_json_from_s3(s3, BUCKET, source_key)
        if not pages:
            continue

        # Temporary list for records from this file
        records_from_this_file = []
        for page in pages:
            records_on_page = page.get("results")
            if records_on_page:
                records_from_this_file.extend(records_on_page)

        for record in records_from_this_file:
            record["openaq_sensor_id"] = sensor_id

        all_records.extend(records_from_this_file)

    if not all_records:
        logging.warning("No valid records found after processing all files. Exiting.")
        return

    logging.info(
        f"Collected a total of {len(all_records)} records. Starting final transformation."
    )

    # Transform Records to df
    measurements_df = transform_records_to_df(
        all_records,
        COLUMN_RENAME_MAP,
        DEDUPLICATION_SUBSET,
        FINAL_COLUMNS,
        FINAL_SCHEMA,
    )

    # Merge dim_sensor + fact_measurement dfs
    logging.info("Enriching measurement data with sensor dimensions...")
    fact_measurements_df = pd.merge(
        measurements_df, dim_sensor_df, on="openaq_sensor_id", how="left"
    )

    # Create the 'date_id' column (YYYYMMDD format)
    fact_measurements_df["date_id"] = (
        fact_measurements_df["utc_timestamp"].dt.strftime("%Y%m%d").astype("Int64")
    )

    # Create the 'time_id' column (HHMMSS format)
    fact_measurements_df["time_id"] = (
        fact_measurements_df["utc_timestamp"].dt.strftime("%H%M%S").astype("Int64")
    )

    # Remove timezone as this causes problems in SF
    fact_measurements_df["utc_timestamp"] = fact_measurements_df[
        "utc_timestamp"
    ].dt.tz_localize(None)

    final_df = fact_measurements_df.drop(
        columns=["sensor_name", "parameter_id", "ingest_ts_y"]
    )

    # Transform to parquet
    parquet_bytes = df_to_parquet(final_df)

    # Create S3 Key for sensors.parquet
    s3_key = create_s3_key(PROCESSED_PREFIX, "measurements", ".parquet")

    # Upload to S3 for measurements.parquet
    upload_bytes_to_s3(s3, BUCKET, s3_key, parquet_bytes)

    # Archive processed raw files
    for key in raw_keys:
        archive_s3_file(s3, BUCKET, key)


if __name__ == "__main__":
    main()

"""
Transforms raw sensor data and uploads it to S3.
"""

# Imports
import os
import logging
import datetime as dt
from dotenv import load_dotenv
import boto3
import pandas as pd
from pathlib import Path

dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

from .utils.extract_openaq_utils import (
    read_json_from_s3,
    create_s3_key,
    find_latest_s3_key,
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
RAW_PREFIX = "raw/sensors"
PROCESSED_PREFIX = "processed/dim_sensor"
PROCESSED_LOCATION_PREFIX = "processed/dim_location"

# Transformation parameters
COLUMN_RENAME_MAP = {
    "id": "openaq_sensor_id",
    "name": "sensor_name",
    "parameter_id": "parameter_id",
    "location_id": "location_id",
}

FINAL_SCHEMA = {
    COLUMN_RENAME_MAP["id"]: "Int64",
    COLUMN_RENAME_MAP["name"]: "string",
    COLUMN_RENAME_MAP["parameter_id"]: "Int64",
    # COLUMN_RENAME_MAP["location_id"]: "Int64", This column is being merged later in the process manually
    "ingest_ts": "datetime64[ns, UTC]",
}

FINAL_COLUMNS = list(FINAL_SCHEMA.keys())

# The column for deduplication is defined separately
DEDUPLICATION_SUBSET = ["openaq_sensor_id"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    """Orchestrates the transformation of sensor files and generates list of sensor_ids."""

    logging.info("Starting transformation task for dim_sensor.")

    s3 = boto3.client("s3")

    # Load sensor_location map
    map_key = find_latest_s3_key(
        s3, BUCKET, PROCESSED_LOCATION_PREFIX, "sensor_to_location_map"
    )

    sensor_location_map = read_json_from_s3(s3, BUCKET, map_key)

    # Create df from sensor_location_map
    sensor_map_dict = sensor_location_map[0]
    location_id_df = pd.DataFrame(
        sensor_map_dict.items(), columns=["openaq_sensor_id", "location_id"]
    )

    # Get correct datatype for openaq_sensor_id
    schema = {"openaq_sensor_id": "Int64", "location_id": "Int64"}
    location_id_df = location_id_df.astype(schema)

    # Process sensor data
    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)

    # raw_keys = ["raw/sensors/2025/08/27/sensors_sensor_7559_20250827174551.json", "raw/sensors/2025/08/27/sensors_sensor_10117359_20250827183303.json"]

    if not raw_keys:
        logging.warning("No raw files found to process. Exiting.")
        return

    all_records = []

    logging.info(f"Found {len(raw_keys)} raw files to process.")

    # Loop through each file
    for source_key in raw_keys:
        logging.info(f"--- Processing file: {source_key} ---")

        # Process file
        # Read file
        sensor = read_json_from_s3(s3, BUCKET, source_key)

        # Add payloads from paginated api responses
        for page in sensor:
            records_on_page = page.get("results")

            if records_on_page:
                all_records.extend(records_on_page)

    # Check if any records were collected at all
    if not all_records:
        logging.warning("No valid records found after processing all files. Exiting.")
        return

    logging.info(
        f"Collected a total of {len(all_records)} records. Starting final transformation."
    )

    # Transform Records to df
    sensors_df = transform_records_to_df(
        all_records,
        COLUMN_RENAME_MAP,
        DEDUPLICATION_SUBSET,
        FINAL_COLUMNS,
        FINAL_SCHEMA,
    )

    # Merge location_id
    merged_df = pd.merge(
        left=sensors_df, right=location_id_df, on="openaq_sensor_id", how="left"
    )

    # Transform to parquet
    parquet_bytes = df_to_parquet(merged_df)

    # Create S3 Key for sensors.parquet
    s3_key = create_s3_key(PROCESSED_PREFIX, "sensors", ".parquet")

    # Upload to S3 for sensors.parquet
    upload_bytes_to_s3(s3, BUCKET, s3_key, parquet_bytes)

    # Archive processed raw files
    for key in raw_keys:
        archive_s3_file(s3, BUCKET, key)


if __name__ == "__main__":
    main()

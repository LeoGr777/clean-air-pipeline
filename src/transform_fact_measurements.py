"""
Transforms raw meausurement data and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import datetime as dt
from pathlib import Path 
import json
import re

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3
import pandas as pd

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.extract_openaq_utils import read_json_from_s3, find_latest_s3_key
from utils.transform_utils import list_s3_keys_by_prefix, transform_records_to_df, archive_s3_file

# ─── Load env vars and set up logging ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/measurements"
PROCESSED_PREFIX = "processed/fact_measurements"
REQUIRED_DIMENSION_TABLE = "dim_sensor"
SENSOR_PREFIX = "processed/dim_sensor"

# Transformation parameters
COLUMN_RENAME_MAP = {
    "period_datetimeFrom_utc": "utc_timestamp",
    "sensor_id": "openaq_sensor_id"

}
FINAL_COLUMNS = [
    "value",
    "openaq_sensor_id",
    "parameter_id",
    "utc_timestamp",
    "ingest_ts"
]

DEDUPLICATION_SUBSET = ['sensor_id', 'utc_timestamp', 'parameter_id']


def main():
    """Orchestrates the transformation of measurement (hour of day) files."""

    logging.info("Starting measurements transformation (hour of day) task.")

    s3 = boto3.client("s3")
    # raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)

    logging.info(f"Loading required dimension table: {REQUIRED_DIMENSION_TABLE}")

    dim_sensor_key = find_latest_s3_key(s3, BUCKET, SENSOR_PREFIX, ".parquet")
    dim_sensor_df = pd.read_parquet(f"s3://{BUCKET}/{dim_sensor_key}")

    # TESTING
    # print(dim_sensor_key)
    #print(dim_sensor_df.head())
    #exit()

    # TESTING
    raw_keys = [
        "raw/measurements/2025/08/22/measurements_sensor_3019_20250822174840.json",
        "raw/measurements/2025/08/22/measurements_sensor_3018_20250822174837.json"
    ]  # FOR TESTING

    if not raw_keys:
        logging.warning("No raw files found to process. Exiting.")
        return
    
    all_records = []

    logging.info(f"Found {len(raw_keys)} raw files to process.")

    # Loop through each file
    for source_key in raw_keys:
        logging.info(f"--- Processing file: {source_key} ---")

        # Get sensor_id
        try:
            sensor_id = re.search(r"._sensor_(\d+)",source_key, re.IGNORECASE).group(1)
        except AttributeError:
            sensor_id = ""
            logging.warning(f"Could not find sensor_id in filename: {source_key}. Skipping file.")
            continue

        # TESTING
       #print(sensor_id)

        # Process file
        # Read file
        sensor = read_json_from_s3(s3, BUCKET, source_key)

        # Add payloads from paginated api responses
        for page in sensor:
            records_on_page = page.get("results")

            # TESTING
            #print(records_on_page)

            if records_on_page:
                all_records.extend(records_on_page)

            # TESTING
            #print(all_records)

        # Enrich with sensor_id
        for record in all_records:
            record["sensor_id"] = sensor_id

            # TESTING
            #print(all_records)
    
    # Check if any records were collected at all
    if not all_records:
        logging.warning("No valid records found after processing all files. Exiting.")
        return
        
    logging.info(f"Collected a total of {len(all_records)} records. Starting final transformation.")

    # Transform Records to df
    measurements_df = transform_records_to_df(all_records, COLUMN_RENAME_MAP, DEDUPLICATION_SUBSET, FINAL_COLUMNS)

    # Convert the column to the nullable integer type
    measurements_df['openaq_sensor_id'] = measurements_df['openaq_sensor_id'].astype('Int64')

        # TESTING
        # Get all column names and sort them alphabetically
        #sorted_column_list = sorted(df.columns)

        # Print the resulting list
       # print(sorted_column_list)

        # Transform
        #print(df)
    print(dim_sensor_df.info())
    print(measurements_df.info())
    #exit()

    # Merge dim_sensor + fact_measurement dfs
    logging.info("Enriching measurement data with sensor dimensions...")
    fact_measurements_df = pd.merge(measurements_df, dim_sensor_df, on="openaq_sensor_id", how="left")

    print(fact_measurements_df.head())

            
        
        

     







# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()


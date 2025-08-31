"""
Transforms raw parameter data and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import datetime as dt
from pathlib import Path 

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3
import pandas as pd

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from .utils.extract_openaq_utils import read_json_from_s3, create_s3_key, upload_bytes_to_s3
from .utils.transform_utils import list_s3_keys_by_prefix, transform_records_to_df, df_to_parquet, archive_s3_file

# ─── Load env vars and set up logging ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/parameters"
PROCESSED_PREFIX = "processed/dim_parameter"

# This map is the SINGLE SOURCE OF TRUTH for columns
COLUMN_RENAME_MAP = {
    "id": "openaq_parameter_id",
    "name": "parameter_name",
    "displayName": "parameter_display_name",
    "units": "parameter_unit"
}

FINAL_SCHEMA = {
    COLUMN_RENAME_MAP["id"]: "Int64",
    COLUMN_RENAME_MAP["name"]: "string",
    COLUMN_RENAME_MAP["displayName"]: "string",
    COLUMN_RENAME_MAP["units"]: "string",
    "ingest_ts": "datetime64[ns, UTC]",
}

FINAL_COLUMNS = list(FINAL_SCHEMA.keys())

# The column for deduplication is defined separately
DEDUPLICATION_SUBSET = ["openaq_parameter_id"]

def main():
    """Orchestrates the transformation of parameter files."""
    s3 = boto3.client("s3")

    logging.info("Starting transformation task for dim_sensor.")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)


    all_records = [] # Empty list to collect dataframes

    logging.info(f"Found {len(raw_keys)} raw files to process.")

    # Loop through each file
    for source_key in raw_keys:
        logging.info(f"--- Processing file: {source_key} ---")

        # Process file
        # Read file
        parameter = read_json_from_s3(s3, BUCKET, source_key)

        # Add payloads from paginated api responses
        for page in parameter:
            records_on_page = page.get("results")

            if records_on_page:
                all_records.extend(records_on_page)

    # Check if any records were collected at all
    if not all_records:
        logging.warning("No valid records found after processing all files. Exiting.")
        return
    
    logging.info(f"Collected a total of {len(all_records)} records. Starting final transformation.")

    # Transform Records to df
    parameters_df = transform_records_to_df(all_records, COLUMN_RENAME_MAP,DEDUPLICATION_SUBSET,FINAL_COLUMNS,FINAL_SCHEMA)

    # Transform locations_df to parquet
    parquet_bytes = df_to_parquet(parameters_df)

    # Create S3 Key for parameters.parquet
    s3_key = create_s3_key(PROCESSED_PREFIX, "parameters", ".parquet")

    # Upload to S3 for parameters.parquet
    upload_bytes_to_s3(s3, BUCKET, s3_key, parquet_bytes)

    # Archive processed raw parameters
    for key in raw_keys:
        archive_s3_file(s3, BUCKET, key)

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
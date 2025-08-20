"""
Transforms raw parameter data and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import datetime as dt

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3
import pandas as pd

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.transform_utils import list_s3_keys_by_prefix, transform_json_to_parquet, archive_s3_file

# ─── Load env vars and set up logging ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/parameters"
PROCESSED_PREFIX = "processed/dim_parameter"

# This map is the SINGLE SOURCE OF TRUTH for columns
PARAMETERS_COLUMN_MAP = {
    "id": "openaq_parameter_id",
    "name": "parameter_name",
    "displayName": "parameter_display_name",
    "units": "parameter_unit"
}

# The column for deduplication is defined separately
DEDUPLICATION_COLUMNS = ["openaq_sensor_id"]

# The final schema is now DERIVED from the map's values
FINAL_COLUMNS = list(PARAMETERS_COLUMN_MAP.values()) + ["ingest_ts"]

def main():
    """Orchestrates the transformation of parameter files."""
    s3 = boto3.client("s3")
    logging.info("Starting transformation task for dim_sensor.")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)
    all_processed_dfs = [] # Empty list to collect dataframes

    # --- TRANSFORM LOOP ---
    for key in raw_keys:
        try:
            # Call the generic utility with all the specific configurations
            processed_df = transform_json_to_parquet(
                s3_client=s3,
                bucket_name=BUCKET,
                source_key=key,
                processed_prefix=PROCESSED_PREFIX,
                column_rename_map=PARAMETERS_COLUMN_MAP,
                deduplication_subset=DEDUPLICATION_COLUMNS,
                final_columns=FINAL_COLUMNS
            )
            if processed_df is not None:
                all_processed_dfs.append(processed_df)
        except Exception as e:
            logging.error(f"Failed to transform {key}: {e}", exc_info=True)

        
    # --- ARCHIVE LOOP ---
    logging.info("Archiving processed raw files.")
    for key in raw_keys:
        try:
            archive_s3_file(s3, BUCKET, key)
        
        except Exception as e:
            # # Log the error but continue with next file
            logging.error(f"Could not archive {key}: {e}")
    
    logging.info("Transformation task for dim_parameter finished.")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
"""
Transforms raw sensor data and uploads it to S3.
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

# 1.3 Local application modules
from utils.transform_utils import list_s3_keys_by_prefix, transform_json_to_parquet, archive_s3_file
from utils.extract_openaq_utils import upload_to_s3

# ─── Load env vars and set up logging ──────────────────────────────────────────
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/sensors"
PROCESSED_PREFIX = "processed/dim_sensor"

# This map is the SINGLE SOURCE OF TRUTH for columns
SENSORS_COLUMN_MAP = {
    "id": "openaq_sensor_id",
    "name": "sensor_name"
}

# The column for deduplication is defined separately
DEDUPLICATION_COLUMNS = ["openaq_sensor_id"]

# The final schema is now DERIVED from the map's values
FINAL_COLUMNS = list(SENSORS_COLUMN_MAP.values()) + ["ingest_ts"]

def main():
    """Orchestrates the transformation of sensor files and generates list of sensor_ids."""
    s3 = boto3.client("s3")
    logging.info("Starting transformation task for dim_sensor.")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)
    all_processed_dfs = [] # Empty list to collect dataframes

    # --- STEP 1: TRANSFORM LOOP ---
    for key in raw_keys:
        try:
            # Call the generic utility with all the specific configurations
            processed_df = transform_json_to_parquet(
                s3_client=s3,
                bucket_name=BUCKET,
                source_key=key,
                processed_prefix=PROCESSED_PREFIX,
                column_rename_map=SENSORS_COLUMN_MAP,
                deduplication_subset=DEDUPLICATION_COLUMNS,
                final_columns=FINAL_COLUMNS
            )
            if processed_df is not None:
                all_processed_dfs.append(processed_df)
        except Exception as e:
            logging.error(f"Failed to transform {key}: {e}", exc_info=True)

    # --- STEP 2: GENERATE SENSOR IDS AND UPLOAD---
    if not all_processed_dfs:
        logging.warning("No data was processed. Skipping upload of sensor_ids.")
        return
        
    try:
        logging.info("Generating and uploading sensor_ids list.")
        combined_df = pd.concat(all_processed_dfs, ignore_index=True)
        sensor_ids = combined_df['openaq_sensor_id'].unique().tolist()

        processed_path = "processed/dim_sensor"

        upload_to_s3(
        s3_client=s3,
        bucket_name=BUCKET,
        endpoint=processed_path, 
        data=sensor_ids,
        file_prefix="sensor_id_list_"      
        )  
        logging.info("Successfully uploaded sensor_ids list.")
    
    except Exception as e:
        logging.error(f"Failed to upload sensor_ids list: {e}. Raw files will NOT be archived.", exc_info=True)
        # Stop here to not archive
        return
    
    # --- STEP 3: ARCHIVE LOOP ---
    logging.info("Archiving processed raw files.")
    for key in raw_keys:
        try:
            archive_s3_file(s3, BUCKET, key)
        
        except Exception as e:
            # # Log the error but continue with next file
            logging.error(f"Could not archive {key}: {e}")
    
    logging.info("Transformation task for dim_sensor finished.")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
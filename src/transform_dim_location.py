"""
Transforms raw location data and uploads it to S3.
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
RAW_PREFIX = "raw/locations"
PROCESSED_PREFIX = "processed/dim_location"

# This map is the SINGLE SOURCE OF TRUTH for columns
LOCATIONS_COLUMN_MAP = {
    "id": "openaq_location_id",
    "name": "location_name",
    "locality": "locality",
    "country": "country_code",
    "coordinates_latitude": "location_latitude",
    "coordinates_longitude": "location_longitude",
    "timezone": "timezone",
}

# The column for deduplication is defined separately
DEDUPLICATION_COLUMNS = ["openaq_location_id"]

# The final schema is now DERIVED from the map's values
FINAL_COLUMNS = list(LOCATIONS_COLUMN_MAP.values()) + ["ingest_ts"]
# ───────────────────────────────────────────────────────────────────────────────

def main():
    """Orchestrates the transformation of location files and generates list of location_ids."""
    s3 = boto3.client("s3")
    logging.info("Starting transformation task for dim_location.")

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
                column_rename_map=LOCATIONS_COLUMN_MAP,
                deduplication_subset=DEDUPLICATION_COLUMNS,
                final_columns=FINAL_COLUMNS
            )
            if processed_df is not None:
                all_processed_dfs.append(processed_df)
        except Exception as e:
            logging.error(f"Failed to transform {key}: {e}", exc_info=True)

    # --- STEP 2: GENERATE LOCATION IDS AND UPLOAD---
    if not all_processed_dfs:
        logging.warning("No data was processed. Skipping upload of location_ids.")
        return
        
    try:
        logging.info("Generating and uploading location_ids list.")
        combined_df = pd.concat(all_processed_dfs, ignore_index=True)
        location_ids = combined_df['openaq_location_id'].unique().tolist()

        processed_path = "processed/dim_location"

        upload_to_s3(
        s3_client=s3,
        bucket_name=BUCKET,
        endpoint=processed_path, 
        data=location_ids,
        file_prefix="location_id_list_"      
        )  
        logging.info("Successfully uploaded location_ids list.")
    
    except Exception as e:
        logging.error(f"Failed to upload location_ids list: {e}. Raw files will NOT be archived.", exc_info=True)
        # Stoppe hier, damit die Archivierung nicht stattfindet
        return
    
    # --- STEP 3: ARCHIVE LOOP ---
    logging.info("Archiving processed raw files.")
    for key in raw_keys:
        try:
            archive_s3_file(s3, BUCKET, key)
        
        except Exception as e:
            # Logge den Fehler, aber fahre mit dem nächsten File fort
            logging.error(f"Could not archive {key}: {e}")
    
    logging.info("Transformation task for dim_location finished.")


if __name__ == "__main__":
    main()
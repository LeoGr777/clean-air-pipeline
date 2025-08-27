"""
Transforms raw location data and uploads it to S3.
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
from utils.extract_openaq_utils import upload_to_s3, upload_bytes_to_s3, read_json_from_s3, create_s3_key
from utils.transform_utils import list_s3_keys_by_prefix, transform_records_to_df, df_to_parquet, archive_s3_file

# ─── Load env vars and set up logging ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/locations"
PROCESSED_PREFIX = "processed/dim_location"

# Transformation parameters
COLUMN_RENAME_MAP = {
    "id": "openaq_location_id",
    "name": "location_name",
    "locality": "locality",
    "country": "country_code",
    "sensors": "sensors",
    "coordinates_latitude": "location_latitude",
    "coordinates_longitude": "location_longitude",
    "timezone": "timezone",
}


FINAL_SCHEMA = {
    COLUMN_RENAME_MAP["id"]: "Int64",
    COLUMN_RENAME_MAP["name"]: "string",
    COLUMN_RENAME_MAP["locality"]: "string",
    COLUMN_RENAME_MAP["country"]: "string",
    COLUMN_RENAME_MAP["sensors"]: "object",
    COLUMN_RENAME_MAP["coordinates_latitude"]: "float64",
    COLUMN_RENAME_MAP["coordinates_longitude"]: "float64",
    COLUMN_RENAME_MAP["timezone"]: "string",
    "ingest_ts": "datetime64[ns, UTC]",
}

FINAL_COLUMNS = list(FINAL_SCHEMA.keys())


# The column for deduplication is defined separately
DEDUPLICATION_SUBSET = ["openaq_location_id"]
# ───────────────────────────────────────────────────────────────────────────────

def main():
    """Orchestrates the transformation of location files and generates list of location_ids and mapping file for sensor --> location"""

    s3 = boto3.client("s3")

    logging.info("Starting transformation task for dim_location.")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)

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
        location = read_json_from_s3(s3, BUCKET, source_key)

        # Add payloads from paginated api responses
        for page in location:
            records_on_page = page.get("results")

            if records_on_page:
                all_records.extend(records_on_page)

    # Check if any records were collected at all
    if not all_records:
        logging.warning("No valid records found after processing all files. Exiting.")
        return
    
    logging.info(f"Collected a total of {len(all_records)} records. Starting final transformation.")

    # Transform Records to df
    locations_df = transform_records_to_df(all_records, COLUMN_RENAME_MAP,DEDUPLICATION_SUBSET,FINAL_COLUMNS,FINAL_SCHEMA)

    # Transform locations_df to parquet
    parquet_bytes = df_to_parquet(locations_df)
    # parquet_buffer = io.BytesIO(parquet_bytes)
    # parquet_df = pd.read_parquet(parquet_buffer)

    # Create S3 Key for locations.pqrquet
    s3_key = create_s3_key(PROCESSED_PREFIX, "locations", ".parquet")

    # Upload to S3 for locations.pqrquet
    upload_bytes_to_s3(s3, BUCKET, s3_key, parquet_bytes)

    # Archive processed raw locations
    for key in raw_keys:
        archive_s3_file(s3, BUCKET, key)

    # Extract sensor_ids
    locations_df['sensor_ids'] = locations_df['sensors'].apply(
        lambda list_of_dicts: [d.get('id') for d in list_of_dicts if isinstance(d, dict)]
    )

    # Create location_id list as base for sensor extract step
    location_ids = locations_df['openaq_location_id'].unique().tolist()

    # Upload location_id list to S3
    upload_to_s3(s3, BUCKET, PROCESSED_PREFIX, location_ids, "location_ids")

    # Select location_id and sensor_ids and create one row per sensor_id
    flat_location_df = locations_df[["sensor_ids","openaq_location_id"]].explode('sensor_ids')

    # Create dict with sensor_id : location_id
    sensor_to_location_map = [flat_location_df.set_index('sensor_ids')['openaq_location_id'].to_dict()]

    # Remove sensor data
    # clean_locations_df = locations_df.drop(columns='sensors')

    # Upload sensor_to_location_map to S3
    upload_to_s3(s3, BUCKET, PROCESSED_PREFIX, sensor_to_location_map, "sensor_to_location_map")

    logging.info("Transformation task for dim_location finished.")

if __name__ == "__main__":
    main()
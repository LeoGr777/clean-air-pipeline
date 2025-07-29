# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

# 1.3 Local application modules
from utils.transform_utils import list_s3_keys_by_prefix, transform_json_to_parquet


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
    """Orchestrates the transformation of location files."""
    s3 = boto3.client("s3")
    logging.info("Starting transformation task for dim_location.")

    raw_keys = list_s3_keys_by_prefix(s3, BUCKET, RAW_PREFIX)

    for key in raw_keys:
        try:
            # Call the generic utility with all the specific configurations
            transform_json_to_parquet(
                s3_client=s3,
                bucket_name=BUCKET,
                source_key=key,
                processed_prefix=PROCESSED_PREFIX,
                column_rename_map=LOCATIONS_COLUMN_MAP,
                deduplication_subset=DEDUPLICATION_COLUMNS,
                final_columns=FINAL_COLUMNS
            )
        except Exception as e:
            logging.error(f"Failed to transform {key}: {e}", exc_info=True)
    
    logging.info("Transformation task for dim_location finished.")


if __name__ == "__main__":
    main()
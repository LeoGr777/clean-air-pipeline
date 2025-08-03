"""
Extracts sensor endpoint data for a given list of locations from openaq API and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

# 1.3 Local application modules
from utils.extract_openaq_utils import fetch_all_pages, upload_to_s3, fetch_sequentially, find_latest_s3_key, read_json_from_s3


# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
load_dotenv()

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_SENSORS = "raw/sensors"
BASE_URL = "https://api.openaq.org/v3/sensors"
PROCESSED_LOCATIONS_PREFIX = "processed/dim_location"


# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s"
)

# The S3 client is initialized once
s3 = boto3.client("s3")

# =============================================================================
# 3. MAIN LOGIC
# =============================================================================
def main():
    """
    Orchestrates the fetching of sensors and uploading the result to S3.
    """
    logging.info(f"Starting sensors extraction task")

    # Dynamically find the latest location_ids file
    location_key = find_latest_s3_key(
        s3_client=s3,
        bucket_name=S3_BUCKET,
        prefix=PROCESSED_LOCATIONS_PREFIX,
        file_pattern="location_id_list_"
    )

    if not location_key:
        logging.error("Could not key for location list. Aborting.")
        return
    
    location_ids = read_json_from_s3(
    s3_client=s3, 
    bucket_name=S3_BUCKET,
    s3_key=location_key
    )

    # Build a list of all URLs to fetch
    urls_to_fetch = [f"{BASE_URL}/{loc_id}" for loc_id in location_ids]
    print(urls_to_fetch)
    
    logging.info(f"Prepared {len(urls_to_fetch)} URLs for sensor data fetching.")

    # Fetch all data sequentially
    sensor_data = fetch_sequentially(urls=urls_to_fetch, requests_per_minute=55)

    # Instead of uploading the raw sensor_data, create a clean list
    all_sensor_records = []

    for response in sensor_data:
        # Check if the response was successful and has a 'results' key
        if response and "results" in response:
            all_sensor_records.extend(response["results"])

    # 4. Upload the combined results to S3
    if sensor_data:
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_SENSORS,
            data=all_sensor_records,
        )
    else:
        logging.warning("No sensor data was fetched.")
    
    logging.info("Sensor extraction task finished.")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
 
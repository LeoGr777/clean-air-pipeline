"""
Extracts meausurements (hour of day) endpoint data for a given list of sensors from openaq API and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import time
from pathlib import Path 

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.extract_openaq_utils import upload_to_s3, fetch_all_pages, find_latest_s3_key, read_json_from_s3

# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================

API_KEY = os.getenv("API_KEY")

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_MEASUREMENTS = "raw/measurements"
BASE_URL = "https://api.openaq.org/v3"
EDNDPOINT = "sensors"
MEASUREMENT_SUFFIX = "hours/hourofday"
PROCESSED_SENSORS_PREFIX = "processed/dim_sensor"
SENSOR_LIST_FILE_PATTERN = "sensor_id_list"
URL_PARAMS = {"datetime_from": "2025-08-16T21:00:00Z"}

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
    Orchestrates the fetching of measurements and uploading the result to S3.
    """
    logging.info(f"Starting measurements (hour of day) extraction task")

     # Dynamically find the latest sensor_id file
    sensor_key = find_latest_s3_key(
        s3_client=s3,
        bucket_name=S3_BUCKET,
        prefix=PROCESSED_SENSORS_PREFIX,
        file_pattern=SENSOR_LIST_FILE_PATTERN
    )

    if not sensor_key:
        logging.error("Could not find key for sensor list. Aborting.")
        return
    
    sensor_ids = read_json_from_s3(
    s3_client=s3, 
    bucket_name=S3_BUCKET,
    s3_key=sensor_key
    )
 
    # This dictionary will hold the final raw data, mapping URL to its response.
    raw_data_map = {}

    logging.info("Starting sequential data fetching...")

    num_urls = len(sensor_ids)

    logging.info(f"{num_urls} URLs to fetch")
    i = 1

    # Loop through each sensor ID one by one
    for sensor_id in sensor_ids:
        # Build the specific URL for this ID
        url = f"{BASE_URL}/{EDNDPOINT}/{sensor_id}/{MEASUREMENT_SUFFIX}"
        endpoint = f"{EDNDPOINT}/{sensor_id}/{MEASUREMENT_SUFFIX}"
        logging.info(f"{i}/{num_urls} Fetching data for sensor {sensor_id} from {url}")

        # Fetch the data for this url only
        response_data = fetch_all_pages(endpoint, URL_PARAMS)

        # Store the raw response (or None if it failed) in the dictionary
        raw_data_map[url] = response_data
        i +=1
        
    logging.info(f"Finished fetching. Collected data for {len(raw_data_map)} URLs.")

    # Upload the combined results to S3
    if raw_data_map:
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_MEASUREMENTS,
            data=raw_data_map,
        )
    else:
        logging.warning("No measurement (hour of day)  data was fetched.")
    
    logging.info("Measurement (hour of day) extraction task finished.")
# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()



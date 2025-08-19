"""
Extracts meausurements (hour of day) endpoint data for a given list of sensors from openaq API and uploads it to S3.
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
from test_run import fetch_sequentially_test


# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
load_dotenv()

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_MEASUREMENTS = "raw/measurements"
BASE_URL = "https://api.openaq.org/v3/sensors/"
MEASUREMENT_SUFFIX = "/hours/hourofday"
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
        logging.error("Could not key for sensor list. Aborting.")
        return
    
    sensor_ids = read_json_from_s3(
    s3_client=s3, 
    bucket_name=S3_BUCKET,
    s3_key=sensor_key
    )

    # Build a list of all URLs to fetch
    #urls_to_fetch = [f"{BASE_URL}{meausure_id}{MEASUREMENT_SUFFIX}" for meausure_id in sensor_ids]

    # Build a Dict of request URLS and sensor_ids
    #url_to_id_map = {f"{BASE_URL}{sensor_id}{MEASUREMENT_SUFFIX}": sensor_id for sensor_id in sensor_ids}
    url_to_id_map = {f"{BASE_URL}3051{MEASUREMENT_SUFFIX}": 3051}

    # Request URLS
    urls_to_fetch = list(url_to_id_map.keys())
    sensor_ids = ["3051"]

    # DEBUGGING
    #urls_to_fetch = [f"{BASE_URL}3051{MEASUREMENT_SUFFIX}"]
    # print(url_to_id_map)
    # print(urls_to_fetch)
    
    logging.info(f"Prepared {len(urls_to_fetch)} URLs for measurement (hour of day) data fetching.")

    # Fetch all data sequentially
    meausurement_data = fetch_sequentially_test(urls_to_fetch, URL_PARAMS, requests_per_minute = 55)

    # Check that both lists have the same length
    #if len(sensor_ids) != len(meausurement_data):
    if 1 != 1:
        logging.error("Mismatch between number of sensor IDs and number of responses. Aborting.")

    else:
        all_measurement_records = []
        for sensor_id, response in zip(sensor_ids, meausurement_data):
            if response and "results" in response:
                for record in response["results"]:
                    # Create a new dictionary to ensure 'sensor_id' is the first key.
                    # Unpack the rest of the original dictionary after it.
                    formatted_record = {'sensor_id': sensor_id, **record}
                    
                    # Add the newly formatted dictionary to the list.
                    all_measurement_records.append(formatted_record)

    print(all_measurement_records)

    # Upload the combined results to S3
    if meausurement_data:
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_MEASUREMENTS,
            data=all_measurement_records,
        )
    else:
        logging.warning("No measurement (hour of day)  data was fetched.")
    
    logging.info("Measurement (hour of day) extraction task finished.")
# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()



"""
Extracts meausurements (hourly) endpoint data for a given list of sensors from openaq API and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import datetime as dt
from pathlib import Path 
import pandas as pd

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.extract_openaq_utils import upload_to_s3, fetch_all_pages_new, find_latest_s3_key, read_json_from_s3, get_yesterday_iso_date

# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_MEASUREMENTS = "raw/measurements"
BASE_URL = "https://api.openaq.org/v3"
EDNDPOINT = "sensors"
MEASUREMENT_SUFFIX = "hours"
PROCESSED_SENSORS_PREFIX = "processed/dim_sensor"
SENSOR_LIST_FILE_PATTERN = "sensors"

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
    Orchestrates the fetching of measurements (hourly) and uploading the result to S3.
    """
    logging.info(f"Starting measurements (hourly) extraction task")

    # Fetch only data from yesterday
    URL_PARAMS = {"datetime_from": get_yesterday_iso_date()}

    # Dynamically find the latest sensor_id file to generate endpoint urls later
    sensor_key = find_latest_s3_key(
        s3_client=s3,
        bucket_name=S3_BUCKET,
        prefix=PROCESSED_SENSORS_PREFIX,
        file_pattern=SENSOR_LIST_FILE_PATTERN
    )

    if not sensor_key:
        logging.error("Could not find key for sensor list. Aborting.")
        return
    
    s3_uri = f"s3://{S3_BUCKET}/{sensor_key}"

    # Read the file directly from S3 into a DataFrame
    print(f"Reading file from {s3_uri}...")
    
    df = pd.read_parquet(s3_uri)
    sensor_id_list = df["openaq_sensor_id"].to_list()
 
    logging.info("Starting paginated, sequential data fetching...")

    num_urls = len(sensor_id_list)

    logging.info(f"{num_urls} URLs to fetch")

    # Logging progress counter for sensor_id_list
    i = 1

    fetched_urls = []

     # Loop through each sensor ID one by one
    for sensor_id in sensor_id_list:
        try:
            # Build the specific URL for this ID
            url = f"{BASE_URL}/{EDNDPOINT}/{sensor_id}/{MEASUREMENT_SUFFIX}"
            endpoint = f"{EDNDPOINT}/{sensor_id}/{MEASUREMENT_SUFFIX}"
            logging.info(f"{i}/{num_urls} Fetching data for sensor {sensor_id} from {url} with params: {URL_PARAMS}")

            # Build file_prefix
            resource = RAW_S3_ENDPOINT_MEASUREMENTS.split("/")[1]
            file_prefix = f"{resource}_sensor_{sensor_id}"

            # Fetch data, one url at a time
            response_data = fetch_all_pages_new(endpoint, URL_PARAMS)

            if response_data[0]["results"]:
                fetched_urls.append(f"{url}?{URL_PARAMS}")
            
            i += 1

            # Upload results per endpoint to S3
            if response_data:
                upload_to_s3(
                    s3_client=s3,
                    bucket_name=S3_BUCKET,
                    endpoint=RAW_S3_ENDPOINT_MEASUREMENTS,
                    data=response_data,
                    file_prefix=file_prefix,
                )
            else:
                logging.warning("No measurement (hourly) data was fetched.")
        except Exception as e:
            logging.error("Error executing meausurement extract task")

    logging.info(f"Finished extract task for meausurement (hourly) for {len(fetched_urls)} urls")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
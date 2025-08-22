"""
Extracts meausurements (hourly) endpoint data for a given list of sensors from openaq API and uploads it to S3.
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

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.extract_openaq_utils import upload_to_s3, fetch_all_pages_new, find_latest_s3_key, read_json_from_s3

# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================

API_KEY = os.getenv("API_KEY")

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_MEASUREMENTS = "raw/measurements"
BASE_URL = "https://api.openaq.org/v3"
EDNDPOINT = "sensors"
MEASUREMENT_SUFFIX = "hours"
PROCESSED_SENSORS_PREFIX = "processed/dim_sensor"
SENSOR_LIST_FILE_PATTERN = "sensor_id_list"

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

    # Calculate yesterday at midnight in UTC
    yesterday_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    yesterday_midnight_utc = yesterday_utc.replace(hour=0, minute=0, second=0, microsecond=0)

    # Format into the required string
    yesterday_iso_string = yesterday_midnight_utc.isoformat().replace('+00:00', 'Z')
    URL_PARAMS = {"datetime_from": yesterday_iso_string}

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
    
    # Read sensor_id file
    sensor_ids = read_json_from_s3(
    s3_client=s3, 
    bucket_name=S3_BUCKET,
    s3_key=sensor_key
    )

    logging.info("Starting paginated, sequential data fetching...")

    num_urls = len(sensor_ids)

    logging.info(f"{num_urls} URLs to fetch")

    # Logging progress counter for sensor_ids
    i = 1

    fetched_urls = []

     # Loop through each sensor ID one by one
    for sensor_id in sensor_ids:
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
            
            i +=1

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
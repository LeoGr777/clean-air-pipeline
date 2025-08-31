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
from pathlib import Path 

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from .utils.extract_openaq_utils import fetch_all_pages_new, get_yesterday_iso_date, upload_to_s3, find_latest_s3_key, read_json_from_s3

# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT = "raw/sensors"
BASE_URL = "https://api.openaq.org/v3"
PROCESSED_LOCATIONS_PREFIX = "processed/dim_location"
ENDPOINT = "sensors"


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

    yesterday_iso_string = get_yesterday_iso_date()
    URL_PARAMS = {"datetime_from": yesterday_iso_string}

    # Dynamically find the latest sensor_location file
    location_key = find_latest_s3_key(
        s3_client=s3,
        bucket_name=S3_BUCKET,
        prefix=PROCESSED_LOCATIONS_PREFIX,
        file_pattern="sensor_to_location_map"
    )

    if not location_key:
        logging.error("Could not find sensor_ids from location task. Aborting.")
        return
    
    raw_data_from_s3 = read_json_from_s3(
    s3_client=s3, 
    bucket_name=S3_BUCKET,
    s3_key=location_key
    )

    # Extract the actual mapping dictionary from the list
    sensor_to_location_map = raw_data_from_s3[0]

    # Create the final list of sensor IDs from the dictionary keys
    sensor_ids = [int(key) for key in sensor_to_location_map.keys()]
   
    num_urls = len(sensor_ids)
    logging.info(f"Prepared {num_urls} URLs for sensor data fetching.")

    # Logging progress counter for sensor_ids
    i = 1

    fetched_urls = []

     # Loop through each sensor_id one by one
    for sensor_id in sensor_ids:
        try:
            # Build the specific URL for this ID
            url = f"{BASE_URL}/{ENDPOINT}/{sensor_id}"
            endpoint = f"{ENDPOINT}/{sensor_id}"
            logging.info(f"{i}/{num_urls} Fetching data for sensor {sensor_id} from {url} with params: {URL_PARAMS}")

            # Build file_prefix
            resource = RAW_S3_ENDPOINT.split("/")[1]
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
                    endpoint=RAW_S3_ENDPOINT,
                    data=response_data,
                    file_prefix=file_prefix,
                )
            else:
                logging.warning("No sensor data was fetched.")
        except Exception as e:
            logging.exception("Error executing meausurement extract task")

    logging.info(f"Finished extract task for sensor for {i}/{num_urls} urls")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
 
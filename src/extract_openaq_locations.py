"""
Extracts location endpoint data from openaq API and uploads it to S3.
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
from .utils.extract_openaq_utils import fetch_all_pages_new, upload_to_s3

# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
# Specific constants for this extraction script
# Filter criteria
# COUNTRY = os.getenv("COUNTRY_CODE", "DE")
# PARAMETER_ID = "2"
COORDINATES = "52.520008,13.404954" # Berlin central
RADIUS = "15000" # radius in m around central berlin

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_LOCATIONS = "raw/locations"
BASE_URL = "https://api.openaq.org/v3"
ENDPOINT = "locations"


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
    Orchestrates the fetching of locations and uploading the result to S3.
    """
    URL_PARAMS = {
        #"iso": COUNTRY, 
        # "parameters_id": PARAMETER_ID,
        "coordinates": COORDINATES,
        "radius": RADIUS,
        }

    logging.info(f"Starting locations extraction for {URL_PARAMS}")

    logging.info("Starting paginated data fetching...")

    response_data = fetch_all_pages_new(ENDPOINT, URL_PARAMS)

    # Build file_prefix
    resource = RAW_S3_ENDPOINT_LOCATIONS.split("/")[1]
    file_prefix = f"{resource}"

    # Upload to S3
    if response_data:
        logging.info(f"Successfully fetched {len(response_data)} locations.")

        # Call the generic S3 upload utility function
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_LOCATIONS,
            data=response_data,
            file_prefix=file_prefix,
        )
    else:
        logging.warning("No locations were fetched. Nothing to upload.")
        return

    logging.info("Locations extraction task finished.")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
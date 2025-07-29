# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

# 1.3 Local application modules
from utils.extract_openaq_utils import fetch_all_pages, upload_to_s3


# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
load_dotenv()

# Specific constants for this extraction script
COUNTRY = os.getenv("COUNTRY_CODE", "DE")

# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_LOCATIONS = "raw/locations"


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
    logging.info(f"Starting locations extraction for country: {COUNTRY}")

    # Step 1: Fetch all locations using the utility function
    locations = fetch_all_pages(endpoint="locations", params={"iso": COUNTRY})

    # Step 2: If data was fetched, upload it to S3
    if locations:
        logging.info(f"Successfully fetched {len(locations)} locations.")

        # Call the generic S3 upload utility function
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_LOCATIONS,
            data=locations
        )
    else:
        logging.warning("No locations were fetched. Nothing to upload.")

    logging.info("Locations extraction task finished.")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
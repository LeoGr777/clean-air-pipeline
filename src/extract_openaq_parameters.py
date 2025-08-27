"""
Extracts parameter endpoint data for a given list of parameters from openaq API and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
from pathlib import Path 

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.extract_openaq_utils import fetch_all_pages_new, upload_to_s3


# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================
# S3 specific constants
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_S3_ENDPOINT_PARAMETERS = "raw/parameters"
BASE_URL = "https://api.openaq.org/v3/parameters"
ENDPOINT = "parameters"


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
    Orchestrates the fetching of parameters and uploading the result to S3.
    """
    logging.info(f"Starting parameters extraction task")

    URL_PARAMS = {}

    response_data = fetch_all_pages_new(ENDPOINT, URL_PARAMS)

    if response_data:
        logging.info("Starting upload to S3.")

        # Call the generic S3 upload utility function
        upload_to_s3(
            s3_client=s3,
            bucket_name=S3_BUCKET,
            endpoint=RAW_S3_ENDPOINT_PARAMETERS,
            data=response_data
        )
        logging.info("parameters extraction task finished.")

    if not response_data:
        logging.warning("No parameters were fetched. Nothing to upload.")   

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    main()
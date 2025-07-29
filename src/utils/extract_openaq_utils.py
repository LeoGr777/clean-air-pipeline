# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import json
from datetime import datetime

# 1.2 Third-party libraries
import requests
import boto3
from botocore.exceptions import ClientError


# =============================================================================
# 2. CONSTANTS AND GLOBAL SETTINGS
# =============================================================================

# The script expects load_dotenv() to have been called by the entry point.
OPENAQ_BASE = os.getenv("OPENAQ_BASE", "https://api.openaq.org/v3")
LIMIT = 1000  # Default limit for paged requests


# =============================================================================
# 3. UTILITY FUNCTIONS
# =============================================================================

def fetch_all_pages(endpoint: str, params: dict) -> list[dict]:
    """
    Fetches all pages for a given API endpoint.
    Handles API key check and pagination automatically.
    """
    API_KEY = os.getenv("API_KEY")

    if not API_KEY:
        logging.error("API_KEY not found in environment.")
        raise RuntimeError("API_KEY must be set.")

    url = f"{OPENAQ_BASE}/{endpoint}"
    headers = {"accept": "application/json", "X-API-Key": API_KEY}
    all_results = []
    page = 1

    while True:
        request_params = params.copy()
        request_params["limit"] = LIMIT
        request_params["page"] = page
        
        logging.info(f"Requesting page {page} for endpoint '{endpoint}' with params {params}")
        
        try:
            response = requests.get(url, params=request_params, headers=headers, timeout=30)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            
            if not results:
                logging.info("No more results returned; ending pagination.")
                break

            all_results.extend(results)
            logging.info(f"Page {page}: fetched {len(results)} items (total: {len(all_results)})")
            
            if len(results) < LIMIT:
                logging.info("Last page reached.")
                break
                
            page += 1

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            raise

    logging.info(f"Total items fetched for endpoint '{endpoint}': {len(all_results)}")
    return all_results


def upload_to_s3(s3_client: boto3.client, bucket_name: str, endpoint: str, data: list[dict]):
    """
    Uploads a list of dictionaries as a JSON file to an S3 bucket.
    The filename includes a timestamp for uniqueness.
    """
    if not bucket_name:
        logging.error("S3_BUCKET environment variable not set.")
        raise ValueError("S3 bucket name is required.")

    # Create a unique filename with a timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{timestamp}.json"
    
    # The S3 object key is the combination of the endpoint path and the filename
    s3_key = f"{endpoint}/{file_name}"

    try:
        # Convert Python list of dicts to a JSON string
        json_data = json.dumps(data, indent=4)

        logging.info(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data
        )
        logging.info("Upload successful.")

    except ClientError as e:
        logging.error(f"S3 upload failed: {e}")
        raise
    except TypeError as e:
        logging.error(f"Data serialization to JSON failed: {e}")
        raise
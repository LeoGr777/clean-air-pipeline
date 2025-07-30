# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import json
import datetime as dt

# 1.2 Third-party libraries
import requests
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed


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

def fetch_concurrently(urls: list[str]) -> list[dict]:
    """
    Fetches data from a list of URLs concurrently using multiple threads.
    """
    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        logging.error("API_KEY not found in environment.")
        raise RuntimeError("API_KEY must be set.")

    headers = {"accept": "application/json", "X-API-Key": API_KEY}
    all_results = []
    
    # This function will be executed by each thread
    def fetch_url(url):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch {url}: {e}")
            return None # Return None on failure

    # Use a thread pool to manage concurrent requests
    with ThreadPoolExecutor(max_workers=10) as executor: # max_workers can be adjusted
        future_to_url = {executor.submit(fetch_url, url): url for url in urls}
        
        for i, future in enumerate(as_completed(future_to_url), 1):
            logging.info(f"Processing request {i}/{len(urls)}...")
            result = future.result()
            if result: # Only add successful results
                all_results.append(result)

    logging.info(f"Successfully fetched data from {len(all_results)} URLs.")
    return all_results



def read_json_from_s3(s3_client: boto3.client, bucket_name: str, s3_key: str) -> list | None:
    """
    Reads a JSON file from an S3 bucket and loads it into a Python list.

    Args:
        s3_client: The initialized Boto3 S3 client.
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The full key (path) to the JSON file in the bucket.

    Returns:
        list: The loaded data as a Python list, or None if an error occurs.
    """
    logging.info(f"Reading JSON file from s3://{bucket_name}/{s3_key}")
    try:
        # Get the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # Read the object's content and decode it from bytes to a string
        content = response["Body"].read().decode("utf-8")
        
        # Parse the JSON string into a Python list
        data = json.loads(content)
        
        logging.info(f"Successfully read and parsed file {s3_key}")
        return data

    except ClientError as e:
        # Handle cases where the file does not exist
        if e.response['Error']['Code'] == 'NoSuchKey':
            logging.error(f"File not found at s3://{bucket_name}/{s3_key}")
        else:
            logging.error(f"An S3 client error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None




def upload_to_s3(s3_client: boto3.client, bucket_name: str, endpoint: str, data: list[dict], file_prefix="data_"):
    """
    Uploads a list of dictionaries as a JSON file to an S3 bucket.
    The filename includes a timestamp for uniqueness.
    """
    if not bucket_name:
        logging.error("S3_BUCKET environment variable not set.")
        raise ValueError("S3 bucket name is required.")

    # Create a unique filename with a timestamp
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    prefix = f"{year}/{month}/{day}"
    file_name = f"{file_prefix}{ts}.json"
    
    # The S3 object key is the combination of the endpoint path and the filename
    s3_key = f"{endpoint}/{prefix}/{file_name}"

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
"""
Utility module to provide functions helping with extracting data steps.
"""

# Imports
import os
import logging
import json
import datetime as dt
import time
import requests
import boto3
from botocore.exceptions import ClientError


# Constants
# The sript requires dotenv as entrypoint
OPENAQ_BASE = os.getenv("OPENAQ_BASE", "https://api.openaq.org/v3")
LIMIT = 1000  # Default limit for paged requests
API_KEY = os.getenv("API_KEY")
# Define the delay based on the API's rate limit
DELAY_BETWEEN_REQUESTS = 2.2  # in seconds


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)


def get_yesterday_iso_date() -> str:
    """
    Generates an ISO date string for UTC time yesterday 0:00:00
    """
    # Calculate yesterday at midnight in UTC
    yesterday_utc = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    yesterday_midnight_utc = yesterday_utc.replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Format into the required string
    return yesterday_midnight_utc.isoformat().replace("+00:00", "Z")


def create_s3_key(prefix: str, file_prefix: str, file_extension: str) -> str:
    """
    Creates a unique, timestamped S3 key in a date-partitioned prefix.

    Args:
        prefix (str): The base path, e.g., 'raw/measurements'.
        file_prefix (str): A prefix for the filename, e.g., 'data_'.
        file_extension (str): The file extension, e.g., '.json' or '.parquet'.

    Returns:
        str: The full S3 key for the new file.
    """
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    date_prefix = f"{year}/{month}/{day}"
    file_name = f"{file_prefix}_{ts}{file_extension}"

    # The S3 object key is the combination of the endpoint path and the filename
    s3_key = f"{prefix.rstrip('/')}/{date_prefix}/{file_name}"
    return s3_key


def find_latest_s3_key(
    s3_client: boto3.client, bucket_name: str, prefix: str, file_pattern: str
) -> str | None:
    """
    Finds the most recently modified object in an S3 prefix that matches a pattern.

    Args:
        s3_client: The initialized Boto3 S3 client.
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The S3 prefix to search within (e.g., "processed/dim_location/").
        file_pattern (str): The pattern to match in the filename (e.g., "location_id_list_").

    Returns:
        str: The S3 key of the latest file, or None if no matching files are found.
    """
    logging.info(
        f"Searching for latest file with pattern '{file_pattern}' in s3://{bucket_name}/{prefix}"
    )
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        latest_file = None
        latest_mod_time = None

        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    key = obj["Key"]
                    if file_pattern in key:
                        mod_time = obj["LastModified"]
                        if latest_mod_time is None or mod_time > latest_mod_time:
                            latest_mod_time = mod_time
                            latest_file = key

        if latest_file:
            logging.info(f"Found latest file: {latest_file}")
            return latest_file
        else:
            logging.warning(
                f"No files matching pattern '{file_pattern}' found in prefix."
            )
            return None

    except Exception as e:
        logging.error(f"Failed to search for latest S3 key: {e}")
        return None


def read_json_from_s3(
    s3_client: boto3.client, bucket_name: str, s3_key: str
) -> list | None:
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
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.error(f"File not found at s3://{bucket_name}/{s3_key}")
        else:
            logging.error(f"An S3 client error occurred: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None


def fetch_all_pages_new(endpoint: str, params: dict = None) -> list[dict]:
    """
    Fetches paginated responses from api endpoint within list of dicts.
    Handles API key check and pagination automatically.
    """
    if not API_KEY:
        logging.error("API_KEY not found in environment.")
        raise RuntimeError("API_KEY must be set.")

    url = f"{OPENAQ_BASE}/{endpoint}"
    headers = {"accept": "application/json", "X-API-Key": API_KEY}
    all_results = []
    num_all_items = 0
    page = 1

    while True:
        request_params = params.copy()
        request_params["limit"] = LIMIT
        request_params["page"] = page

        logging.info(
            f"Requesting page {page} for endpoint '{endpoint}' with params {params}"
        )

        try:
            response = requests.get(
                url, params=request_params, headers=headers, timeout=30
            )

            response.raise_for_status()

            results = response.json()

            payload = results["results"]

            num_items = len(payload)

            # Pause here to stay within API rate limit
            logging.info(
                f"Waiting for {DELAY_BETWEEN_REQUESTS} second(s) before next request..."
            )
            time.sleep(DELAY_BETWEEN_REQUESTS)

            if not payload:
                logging.info("No more results returned; ending pagination.")
                break

            # Append raw responses
            all_results.append(results)

            num_all_items += num_items
            logging.info(
                f"Page {page}: fetched {num_items} items (total: {num_all_items})"
            )

            if num_items < LIMIT:
                logging.info("Last page reached.")
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            raise

    logging.info(f"Total items fetched for endpoint '{endpoint}': {num_all_items}")
    return all_results


def upload_to_s3(
    s3_client: boto3.client,
    bucket_name: str,
    endpoint: str,
    data: list[dict],
    file_prefix="data",
):
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
    file_name = f"{file_prefix}_{ts}.json"

    # The S3 object key is the combination of the endpoint path and the filename
    s3_key = f"{endpoint}/{prefix}/{file_name}"

    try:
        # Convert Python list of dicts to a JSON string
        json_data = json.dumps(data, indent=4)

        logging.info(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=json_data)
        logging.info("Upload successful.")

    except ClientError as e:
        logging.error(f"S3 upload failed: {e}")
        raise
    except TypeError as e:
        logging.error(f"Data serialization to JSON failed: {e}")
        raise


def upload_bytes_to_s3(
    s3_client: boto3.client, bucket_name: str, s3_key: str, data_bytes: bytes
):
    """
    Uploads a bytes object to a specific key in an S3 bucket.
    """
    try:
        logging.info(f"Uploading to s3://{bucket_name}/{s3_key}")
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=data_bytes)
        logging.info("Upload successful.")
    except ClientError as e:
        logging.error(f"S3 upload failed for key {s3_key}: {e}")
        raise

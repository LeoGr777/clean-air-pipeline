#!/usr/bin/env python3
"""
Fetch air-quality data from OpenAQ and upload raw JSON to AWS S3.
"""
import os
import requests
import json
import datetime as dt
from dateutil import tz
import boto3
import logging

# -------------------- Configuration --------------------
# API endpoint and parameters (default city/country can be overridden via env vars)
BASE_URL    = os.getenv("OPENAQ_URL", "https://api.openaq.org/v2/measurements")  # OpenAQ measurements endpoint
COUNTRY     = os.getenv("COUNTRY", "DE")                                      # ISO country code
CITY        = os.getenv("CITY", "Berlin")                                      # City name for query
PARAMETER   = os.getenv("PARAMETER", "pm25")                                   # Air-quality parameter
LIMIT       = int(os.getenv("LIMIT", "1000"))                                  # Max records per request

# S3 configuration
BUCKET_NAME = os.getenv("BUCKET", "clean-air")                                 # Target S3 bucket
RAW_PREFIX  = os.getenv("RAW_PREFIX", "raw")                                   # S3 key prefix for raw data

# Initialize S3 client; boto3 picks up AWS_PROFILE or default credentials automatically
s3 = boto3.client("s3")

# Setup logging
tlogging = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_last_hour():
    """
    Call the OpenAQ API for the past hour of measurements.
    Returns the JSON payload as a Python dict.
    """
    now_utc = dt.datetime.utcnow().replace(tzinfo=tz.tzutc())    # current UTC time
    since   = now_utc - dt.timedelta(hours=1)                     # one hour ago

    params = {
        "country": COUNTRY,
        "city": CITY,
        "parameter": PARAMETER,
        "date_from": since.isoformat(timespec="seconds"),      # start time
        "date_to": now_utc.isoformat(timespec="seconds"),      # end time
        "limit": LIMIT,
        "page": 1,
        "sort": "desc",
        "order_by": "datetime"
    }
    response = requests.get(BASE_URL, params=params, timeout=30)  # HTTP GET
    response.raise_for_status()                                   # raise on error
    return response.json()


def upload_to_s3(payload):
    """
    Upload the JSON payload to S3 under a timestamped key.
    """
    ts = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")         # timestamp for filename
    year = ts[:4]
    month = ts[4:6]
    key = f"{RAW_PREFIX}/{year}/{month}/{ts}.json"              # e.g., raw/2025/05/20250506123045.json

    body = json.dumps(payload).encode("utf-8")                  # JSON bytes
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)         # upload
    logging.info(f"Uploaded raw data to s3://{BUCKET_NAME}/{key}")
    return key


if __name__ == "__main__":
    logging.info("Starting OpenAQ extract task")
    data = fetch_last_hour()                                       # fetch data
    count = len(data.get("results", []))                         # count records
    logging.info(f"Fetched {count} records from OpenAQ")
    key = upload_to_s3(data)                                       # upload to S3
    logging.info(f"Task complete: {count} records stored at {key}")

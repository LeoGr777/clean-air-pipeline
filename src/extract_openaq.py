#!/usr/bin/env python3
"""
Fetch air-quality data from OpenAQ (last 3 hours) and upload raw JSON to AWS S3.
"""
import os
import requests
import json
import datetime as dt
from dateutil import tz
import boto3
import logging

# -------------------- Configuration --------------------
BASE_URL    = os.getenv("OPENAQ_URL", "https://api.openaq.org/v2/measurements")
COUNTRY     = os.getenv("COUNTRY", "DE")
CITY        = os.getenv("CITY", "Berlin")
PARAMETER   = os.getenv("PARAMETER", "pm25")
LIMIT       = int(os.getenv("LIMIT", "100"))

BUCKET_NAME = os.getenv("BUCKET", "clean-air-pipeline")
RAW_PREFIX  = os.getenv("RAW_PREFIX", "raw")

# Initialize S3 client; boto3 picks up AWS_PROFILE or default credentials automatically
s3 = boto3.client("s3")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def fetch_last_three_hours():
    """
    Call the OpenAQ API for the past three hours of measurements.
    Returns the JSON payload as a Python dict.
    """
    now_utc = dt.datetime.now(tz=tz.tzutc())           # current UTC time
    since   = now_utc - dt.timedelta(hours=3)           # three hours ago

    params = {
        "country":   COUNTRY,
        "city":      CITY,
        "parameter": PARAMETER,
        "date_from": since.isoformat(timespec="seconds"),
        "date_to":   now_utc.isoformat(timespec="seconds"),
        "limit":     LIMIT,
        "page":      1,
        "sort":      "desc",
        "order_by":  "datetime"
    }

    logging.info(f"Requesting data from {since.isoformat()} to {now_utc.isoformat()}")
    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        if err.response.status_code == 410:
            logging.warning(
                "OpenAQ measurements endpoint returned 410 Gone – "
                "no data available for the given window, returning empty payload"
            )
            return {"results": []}
        raise


def upload_to_s3(payload):
    """
    Upload the JSON payload to S3 under a timestamped key.
    """
    ts = dt.datetime.now(tz=tz.tzutc()).strftime("%Y%m%d%H%M%S")  # timestamp YYYYMMDDHHMMSS
    year, month = ts[:4], ts[4:6]
    key = f"{RAW_PREFIX}/{year}/{month}/{ts}.json"

    body = json.dumps(payload).encode("utf-8")                   # serialize to JSON bytes
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body)        # upload to S3
    logging.info(f"Uploaded raw data to s3://{BUCKET_NAME}/{key}")
    return key


if __name__ == "__main__":
    logging.info("Starting OpenAQ extract task (last 3h)")
    data = fetch_last_three_hours()                              # fetch data
    count = len(data.get("results", []))                         # count records
    logging.info(f"Fetched {count} records from OpenAQ")
    key = upload_to_s3(data)                                     # upload to S3
    logging.info(f"Task complete: {count} records stored at {key}")

# src/extract_openaq_locations.py
#!/usr/bin/env python3
"""
Extract task: fetch all OpenAQ locations for a given country,
upload the raw JSON list to S3.
"""
import os
import logging
import requests
import json
import datetime as dt
import boto3
from typing import cast
from dateutil import tz
from dotenv import load_dotenv
from mypy_boto3_s3.client import S3Client


# ─── Load environment variables from .env ────────────────────────────────────────
# Make sure you have a file `keys.env` or `.env` with:
#   OPENAQ_BASE=https://api.openaq.org/v3
#   COUNTRY=DE
#   LIMIT=100
#   API_KEY=your_actual_openaq_api_key
#   BUCKET=clean-air-pipeline
#   RAW_PREFIX=raw/locations
load_dotenv(dotenv_path="keys.env")

# ─── Configuration ────────────────────────────────────────────────────────────────
OPENAQ_BASE = os.getenv("OPENAQ_BASE", "https://api.openaq.org/v3")
COUNTRY     = os.getenv("COUNTRY", "DE")
LIMIT       = int(os.getenv("LIMIT", "100"))
API_KEY     = os.getenv("API_KEY")
BUCKET      = os.getenv("BUCKET", "clean-air-pipeline")
RAW_PREFIX  = os.getenv("RAW_PREFIX", "raw/locations")
# ──────────────────────────────────────────────────────────────────────────────────

# configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# initialize S3 client (credentials via AWS_PROFILE or instance role)
s3: S3Client = cast(S3Client, boto3.client("s3")) # type: ignore[reportUnknownMemberType]


def fetch_locations() -> list[dict]:
    """
    Fetch every page of /locations?iso={COUNTRY} from the OpenAQ v3 API.
    Returns a combined list of all location dicts.
    """
    if not API_KEY:
        logging.error("Missing API_KEY in environment (check your .env or keys.env)")
        raise RuntimeError("API_KEY must be set")

    url     = f"{OPENAQ_BASE}/locations"
    headers = {"X-API-Key": API_KEY}
    all_locations = []
    page = 1

    while True:
        params = {"iso": COUNTRY, "limit": LIMIT, "page": page}
        logging.info(f"Requesting page {page} of locations for country={COUNTRY}")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        logging.info(f"Received HTTP {response.status_code} from {response.url}")
        response.raise_for_status()

        data    = response.json()
        results = data.get("results", [])
        if not results:
            logging.info("No more locations returned; ending pagination loop")
            break

        all_locations.extend(results)
        logging.info(f"Page {page}: fetched {len(results)} locations (total so far: {len(all_locations)})")

        if len(results) < LIMIT:
            break

        page += 1

    logging.info(f"Total locations fetched: {len(all_locations)}")
    return all_locations


def upload_locations(locations: list[dict]) -> str:
    """
    Serialize the list of locations to JSON and upload to S3 under a timestamped key.
    """
    ts    = dt.datetime.now(tz=tz.tzutc()).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    key   = f"{RAW_PREFIX}/{year}/{month}/{day}/{ts}.json"

    body = json.dumps(locations).encode("utf-8")   # serialize list -> JSON bytes
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)
    logging.info(f"Uploaded {len(locations)} locations to s3://{BUCKET}/{key}")
    return key


if __name__ == "__main__":
    # 1) fetch all pages of locations for COUNTRY
    locations = fetch_locations()
    # 2) upload the aggregated list to S3
    upload_locations(locations)


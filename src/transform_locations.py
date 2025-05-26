#!/usr/bin/env python3
"""
Transform raw OpenAQ location JSON dumps into Parquet and upload to S3.
"""
import os
import io
import json
import logging
import datetime as dt
from dotenv import load_dotenv
import boto3
import pandas as pd
import pyarrow  # ensure the Parquet engine is present


# ─── Load env vars from .env───────────────────────────────────────────────────────
load_dotenv()

# ─── Configuration ────────────────────────────────────────────────────────────────
BUCKET            = os.getenv("BUCKET", "clean-air-pipeline")
RAW_PREFIX        = os.getenv("RAW_PREFIX", "raw/locations")
PROCESSED_PREFIX  = os.getenv("PROCESSED_PREFIX", "processed/locations")
# ──────────────────────────────────────────────────────────────────────────────────

# init logger
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# init S3 client (Credentials via AWS_PROFILE oder EC2-Role)
s3 = boto3.client("s3")


def list_raw_keys() -> list[str]:
    """
    List all JSON keys under the raw locations prefix in S3.
    """
    paginator = s3.get_paginator("list_objects_v2")
    prefix = RAW_PREFIX.rstrip("/") + "/"
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    logging.info(f"Discovered {len(keys)} raw files under {prefix}")
    return keys


def transform_and_upload(key: str) -> str:
    """
    1) Download the raw JSON list from the given S3 key.
    2) Normalize into a pandas DataFrame.
    3) Rename & select columns, add ingest timestamp.
    4) Write DataFrame to Parquet in-memory.
    5) Upload Parquet bytes to S3 under the processed prefix.
    Returns the new S3 key.
    """
    logging.info(f"Downloading raw JSON: s3://{BUCKET}/{key}")
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    raw_list = json.loads(obj["Body"].read())  # raw_list is a Python list of dicts

    # Normalize nested JSON → flat table
    df = pd.json_normalize(raw_list, sep="_")

    # Rename columns for clarity
    df = df.rename(columns={
        "country.id":   "country_id",
        "country.code": "country_code",
        "country.name": "country_name",
        "coordinates.latitude":  "latitude",
        "coordinates.longitude": "longitude",
    })

    # Add an ingestion timestamp column
    df["ingest_ts"] = dt.datetime.now(dt.timezone.utc)

    # Write to Parquet in-memory
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Derive partition path from current date
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    parquet_key = f"{PROCESSED_PREFIX}/{year}/{month}/{day}/{ts}.parquet"

    # Upload Parquet bytes
    s3.put_object(
        Bucket=BUCKET,
        Key=parquet_key,
        Body=buffer.read(),
        ContentType="application/octet-stream"
    )
    logging.info(f"Uploaded Parquet → s3://{BUCKET}/{parquet_key}")
    return parquet_key


if __name__ == "__main__":
    # 1) Discover raw JSON keys
    raw_keys = list_raw_keys()

    # 2) Process each file
    for rk in raw_keys:
        try:
            transform_and_upload(rk)
        except Exception as e:
            logging.error(f"Failed to transform {rk}: {e}", exc_info=True)

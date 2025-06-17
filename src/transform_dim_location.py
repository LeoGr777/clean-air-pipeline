#!/usr/bin/env python3
"""
Transform raw OpenAQ location JSON dumps into Parquet for DIM_LOCATION and upload to S3.
The generation of the surrogate key (location_key) will happen in Snowflake.
"""
import os
import io
import json
import logging
import datetime as dt
from dotenv import load_dotenv
import boto3
import pandas as pd
import pyarrow # ensure the Parquet engine is present

# --- Load env vars from .env-------------------------------------------------------
load_dotenv()

# --- Configuration ----------------------------------------------------------------
BUCKET = os.getenv("BUCKET", "clean-air-pipeline")
RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/locations")
PROCESSED_PREFIX_DIM_LOCATION = os.getenv("PROCESSED_PREFIX_DIM_LOCATION", "processed/dim_location")
# ----------------------------------------------------------------------------------

# Init logger
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")

# Init S3 client (Credentials via AWS_PROFILE or EC2-Role)
s3 = boto3.client("s3")

def transform_dim_location_data():
    """
    Airflow-callable function to transform raw OpenAQ location data into DIM_LOCATION
    Parquet files and upload to S3. This function prepares the data to be loaded
    into a staging table in Snowflake, from which the final DIM_LOCATION table
    will be populated (including surrogate key generation).
    """
    logging.info("Starting DIM_LOCATION transformation (without surrogate key generation in Python).")

    paginator = s3.get_paginator("list_objects_v2")
    prefix = RAW_PREFIX.rstrip("/") + "/"
    all_raw_keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            all_raw_keys.append(obj["Key"])
    logging.info(f"Discovered {len(all_raw_keys)} raw location files under {prefix}")

    if not all_raw_keys:
        logging.warning("No raw location files found to process. Exiting.")
        return

    # List to collect all processed location data
    all_locations_df = pd.DataFrame()

    for key in all_raw_keys:
        logging.info(f"Downloading raw JSON for DIM_LOCATION: s3://{BUCKET}/{key}")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw_list = json.loads(obj["Body"].read()) # raw_list is a Python list of dicts

            # Normalize nested JSON -> flat table
            df_batch = pd.json_normalize(raw_list, sep="_")

            # Rename columns for clarity and align with DIM_LOCATION schema
            df_batch = df_batch.rename(columns={
                "id":                  "openaq_location_id", # Original ID from OpenAQ
                "name":                "location_name",
                "coordinates_latitude":"latitude",
                "coordinates_longitude":"longitude",
                "city":                "city",
                "country_code":        "country_code",
            })

            # Select and order columns according to DIM_LOCATION schema
            required_cols = [
                "openaq_location_id",
                "location_name",
                "latitude",
                "longitude",
                "city",
                "country_code"
            ]

            # Add missing columns if they are not in the raw dataset
            for col in required_cols:
                if col not in df_batch.columns:
                    df_batch[col] = None # Set to None or a suitable default

            df_batch = df_batch[required_cols]

            # Append to the collected DataFrame
            all_locations_df = pd.concat([all_locations_df, df_batch], ignore_index=True)

        except Exception as e:
            logging.error(f"Failed to process raw file {key} for DIM_LOCATION: {e}", exc_info=True)
            continue # Skip to the next file

    if all_locations_df.empty:
        logging.warning("No data processed for DIM_LOCATION. Exiting.")
        return

    # **Remove duplicates based on the natural key (openaq_location_id)**
    # This ensures that each unique OpenAQ location is represented only once
    # in the processed data before loading into Snowflake.
    all_locations_df = all_locations_df.drop_duplicates(subset=['openaq_location_id'], keep='first')

    # The final DataFrame now has the correct structure for the staging table
    # in Snowflake, which will then handle surrogate key generation.
    final_dim_location_df = all_locations_df

    # **Step 3: Upload as Parquet**
    # Write the entire DataFrame to a Parquet file
    buffer = io.BytesIO()
    final_dim_location_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Derive partition path from current date
    # This ensures new files are written with each run, making the S3 processed layer append-only.
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    parquet_key = f"{PROCESSED_PREFIX_DIM_LOCATION}/{year}/{month}/{day}/dim_location_{ts}.parquet"

    s3.put_object(
        Bucket=BUCKET,
        Key=parquet_key,
        Body=buffer.read(),
        ContentType="application/octet-stream"
    )
    logging.info(f"Successfully transformed and uploaded DIM_LOCATION data to s3://{BUCKET}/{parquet_key}")
    return parquet_key

# For local testing outside of Airflow
if __name__ == "__main__":
    transform_dim_location_data()
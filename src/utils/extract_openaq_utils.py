# ### IMPORTS ###

# 1.1 Standard Libraries
import io
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional

# 1.2 Third-party libraries
import pandas as pd


def list_s3_keys_by_prefix(
    s3_client: Any, bucket_name: str, prefix: str
) -> List[str]:
    """Lists all object keys in S3 under a given prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    logging.info(f"Discovered {len(keys)} raw files under s3://{bucket_name}/{prefix}")
    return keys


def transform_json_to_parquet(
    s3_client: Any,
    bucket_name: str,
    source_key: str,
    processed_prefix: str,
    column_rename_map: Optional[Dict[str, str]] = None,
) -> str:
    """
    Generic transformation task for a single JSON file.
    
    1. Downloads a raw JSON list from S3.
    2. Normalizes it into a pandas DataFrame.
    3. Optionally renames columns based on a provided map.
    4. Writes the DataFrame to Parquet in-memory.
    5. Uploads the Parquet bytes to a processed prefix in S3.
    """
    logging.info(f"Downloading: s3://{bucket_name}/{source_key}")
    obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
    raw_list = json.loads(obj["Body"].read())

    df = pd.json_normalize(raw_list, sep="_")

    if column_rename_map:
        df = df.rename(columns=column_rename_map)

    df["ingest_ts"] = dt.datetime.now(dt.timezone.utc)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    year, month, day = ts[:4], ts[4:6], ts[6:8]
    parquet_key = f"{processed_prefix.rstrip('/')}/{year}/{month}/{day}/{ts}.parquet"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=parquet_key,
        Body=buffer.read(),
        ContentType="application/octet-stream",
    )
    logging.info(f"Uploaded Parquet -> s3://{bucket_name}/{parquet_key}")
    return parquet_key
# ### IMPORTS ###

# 1.1 Standard Libraries
import io
import json
import logging
import datetime as dt
from typing import Any, Dict, List, Optional

# 1.2 Third-party libraries
import pandas as pd

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

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
    deduplication_subset: Optional[List[str]] = None,
    final_columns: Optional[List[str]] = None,
) -> str:
    """
    Generic transformation task for a single JSON file with optional steps.
    """
    logging.info(f"Transforming: s3://{bucket_name}/{source_key}")
    
    # 1. Download and Normalize
    obj = s3_client.get_object(Bucket=bucket_name, Key=source_key)
    raw_list = json.loads(obj["Body"].read())
    df = pd.json_normalize(raw_list, sep="_")

    # 2. Rename columns if a map is provided
    if column_rename_map:
        df = df.rename(columns=column_rename_map)

    # 3. Deduplicate if a subset of columns is provided
    if deduplication_subset:
        # Ensure all columns for deduplication exist in the DataFrame
        if all(col in df.columns for col in deduplication_subset):
            original_rows = len(df)
            df = df.drop_duplicates(subset=deduplication_subset, keep='first')
            logging.info(f"Removed {original_rows - len(df)} duplicate records.")
        else:
            logging.warning("Skipping deduplication: a key column is missing.")

    # 4. Add ingestion timestamp
    df["ingest_ts"] = dt.datetime.now(dt.timezone.utc)
    
    # 5. Enforce final schema if a list of columns is provided
    if final_columns:
        # This ensures the output always has the same columns in the same order
        final_df = pd.DataFrame()
        for col in final_columns:
            if col in df:
                final_df[col] = df[col]
            else:
                final_df[col] = None # Add column with nulls if it was missing
        df = final_df
            
    # 6. Upload to S3
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

    # 7. Archive processed files and delete old file
    try:
        archive_key = source_key.replace("raw/", "archive/", 1)
        
        # 1. Copy the object to the archive location
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": source_key},
            Key=archive_key
        )
        
        # 2. Delete the original object from the raw location
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=source_key
        )
        logging.info(f"Archived {source_key} to {archive_key}")

    except Exception as e:
        logging.error(f"Failed to archive {source_key}: {e}")


    return parquet_key
"""
Utility function to transform openaq raw json data to parquet files and upload to S3.
"""

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

def transform_records_to_df(
    records: List[Dict[str, Any]],
    column_rename_map: Optional[Dict[str, str]] = None,
    deduplication_subset: Optional[List[str]] = None,
    final_columns: Optional[List[str]] = None,
    schema: dict | None = None,
) -> pd.DataFrame:
    """
    Transforms a list of records into a clean, processed Pandas DataFrame.
    """
    if not records:
        logging.warning("Received an empty list of records for transformation.")
        return pd.DataFrame()

    logging.info(f"Transforming {len(records)} records into a DataFrame.")

    # Flatten the list of dictionaries into a DataFrame
    df = pd.json_normalize(records, sep="_")

    # Rename columns if a map is provided
    if column_rename_map:
        df = df.rename(columns=column_rename_map)

    # Deduplicate if a subset of columns is provided
    if deduplication_subset:
        # Ensure all columns for deduplication exist in the DataFrame
        if all(col in df.columns for col in deduplication_subset):
            original_rows = len(df)
            df = df.drop_duplicates(subset=deduplication_subset, keep='first')
            logging.info(f"Removed {original_rows - len(df)} duplicate records.")
        else:
            logging.warning("Skipping deduplication: one or more key columns are missing from the DataFrame.")

    # Add ingestion timestamp
    df["ingest_ts"] = dt.datetime.now(dt.timezone.utc)

    # Apply schema to enforce data types
    
    # Enforce final schema if a list of columns is provided
    if final_columns:
        # This ensures the output always has the same columns in the same order
        final_df = pd.DataFrame()
        for col in final_columns:
            if col in df.columns:
                final_df[col] = df[col]
            else:
                # Add column with nulls if it was missing in the source
                final_df[col] = None 
        df = final_df
            
    logging.info(f"Transformation successful. Final DataFrame has {len(df)} rows and {len(df.columns)} columns.")
    
    return df

def df_to_parquet(df: pd.DataFrame) -> bytes:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        return buffer.read()   

def archive_s3_file(s3_client, bucket_name: str, source_key: str):
    """Copies a file to an archive location and deletes the original."""
    try:
        # Define archive path by replacing 'raw/' with 'archive/'
        archive_key = source_key.replace("raw/", "archive/", 1)
        
        # Copy the object
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=archive_key
        )
        
        # Delete the original object
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        logging.info(f"Successfully archived {source_key} to {archive_key}")

    except Exception as e:
        logging.error(f"Failed to archive {source_key}: {e}")
        raise
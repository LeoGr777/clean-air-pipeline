"""
Load transformed .parquet files from S3 to analytics.dim_location table to Snowflake.
"""

# Imports
from .utils.snowflake_connector import get_snowflake_connection
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)

# Constants
S3_BUCKET = os.getenv("S3_BUCKET")
SNOWFLAKE_TABLE = "dim_location"
S3_STAGE_NAME = "@CLEAN_AIR_DB.PROCESSED.dim_location_stage"
PROCESSED_PREFIX = "processed/dim_location"
FILE_FORMAT = "CLEAN_AIR_DB.PROCESSED.PARQUET_FMT"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)


def load_dim_location():
    truncate_table = f"TRUNCATE table {SNOWFLAKE_TABLE};"
    list_stage_content = f"LIST {S3_STAGE_NAME};"
    get_most_recent_s3_key = rf"""
        SELECT REPLACE("name", 's3://{S3_BUCKET}/{PROCESSED_PREFIX}/', '') AS s3_key 
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "name" LIKE '%locations_%'
            ORDER BY "last_modified" DESC
            LIMIT 1;
    """
    preview_table = f"Select * from {SNOWFLAKE_TABLE} Limit 5"

    try:
        with get_snowflake_connection() as conn:
            with conn.cursor() as cur:
                # Truncate table
                logging.info(f"Truncating {SNOWFLAKE_TABLE}")
                cur.execute(truncate_table)

                # List stage content
                logging.info(f"Executing: List content of {S3_STAGE_NAME}")
                cur.execute(list_stage_content)
                print("--- Content of stage ---")
                for row in cur:
                    print(row)

                # Get newest file
                cur.execute(get_most_recent_s3_key)
                s3_key = cur.fetchone()[0]
                print(f"--- Most recent S3 key: {s3_key} ---")

                # Copy into
                copy_into = f"""
                    COPY INTO {SNOWFLAKE_TABLE} (
                        OPENAQ_LOCATION_ID, 
                        LOCATION_NAME, 
                        LOCALITY, 
                        LOCATION_LATITUDE, 
                        LOCATION_LONGITUDE, 
                        COUNTRY_CODE, 
                        TIMEZONE, 
                        INGEST_TS 
                    )
                    FROM (
                        SELECT
                            $1:openaq_location_id, 
                            $1:location_name, 
                            $1:locality, 
                            $1:location_latitude, 
                            $1:location_longitude, 
                            $1:country_code, 
                            $1:timezone, 
                            $1:ingest_ts
                        FROM {S3_STAGE_NAME}
                    )
                    FILE_FORMAT = {FILE_FORMAT}
                    -- specific S3 key:
                    FILES = ('{s3_key}')
                    FORCE = TRUE;
                """

                logging.info(f"Loading data into {SNOWFLAKE_TABLE}")
                cur.execute(copy_into)

                # Show table preview
                logging.info(f"Previewing first 5 lines of {SNOWFLAKE_TABLE}")
                cur.execute(preview_table)
                for row in cur:
                    print(row)

    except Exception as e:
        print(f"The task failed: {e}")


if __name__ == "__main__":
    load_dim_location()

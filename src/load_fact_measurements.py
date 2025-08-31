"""
Merge transformed .parquet files from S3 to analytics.fact_measurements table to Snowflake.
"""
# ### IMPORTS ###
from .utils.snowflake_connector import get_snowflake_connection
import logging
import os
from dotenv import load_dotenv
from pathlib import Path 

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Constants
S3_BUCKET = os.getenv("S3_BUCKET")
SNOWFLAKE_TABLE = "fact_measurements"
S3_STAGE_NAME = "@CLEAN_AIR_DB.PROCESSED.fact_measurements_stage"
PROCESSED_PREFIX = "processed/fact_measurements"
FILE_FORMAT = "CLEAN_AIR_DB.PROCESSED.PARQUET_FMT"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

def load_fact_measurements():
    truncate_table = f"TRUNCATE table {SNOWFLAKE_TABLE};"
    list_stage_content = f"LIST {S3_STAGE_NAME};"
    get_most_recent_s3_key = rf"""
        SELECT REPLACE("name", 's3://{S3_BUCKET}/{PROCESSED_PREFIX}/', '') AS s3_key 
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "name" LIKE '%/measurements_%'
            ORDER BY "last_modified" DESC
            LIMIT 1;
    """
    preview_table = f"Select * from {SNOWFLAKE_TABLE} Limit 5"
  
    try:
        with get_snowflake_connection() as conn:
            with conn.cursor() as cur:
                # Truncate table
                #logging.info(f"Truncating {SNOWFLAKE_TABLE}")
                #cur.execute(truncate_table)

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

                # Copy into (NOT used as the table is loaded incrementally)
                copy_into = f"""
                    COPY INTO {SNOWFLAKE_TABLE} (
                        date_id,
                        time_id,
                        location_id,
                        sensor_id,
                        parameter_id,
                        timestamp_utc,
                        value
                    )
                    FROM (
                        SELECT
                            $1:date_id,
                            $1:time_id,
                            $1:location_id,
                            $1:openaq_sensor_id as sensor_id,
                            $1:openaq_parameter_id as parameter_id,
                            $1:utc_timestamp as timestamp_utc,
                            $1:value

                        FROM {S3_STAGE_NAME}
                    )
                    FILE_FORMAT = {FILE_FORMAT}
                    -- specific S3 key:
                    FILES = ('{s3_key}')
                    FORCE = TRUE;
                """

                merge_stmt = f"""
                    MERGE INTO {SNOWFLAKE_TABLE} AS target
                    USING (
                        SELECT
                            $1:date_id as date_id,
                            $1:time_id as time_id,
                            $1:location_id as location_id,
                            $1:openaq_sensor_id as sensor_id,
                            $1:openaq_parameter_id as parameter_id,
                            ($1:utc_timestamp / 1000)::INTEGER::TIMESTAMP_NTZ as timestamp_utc,
                            $1:value as value
                        FROM {S3_STAGE_NAME}/{s3_key} 
                        (FILE_FORMAT => {FILE_FORMAT})
                    ) AS source
                    ON (
                        target.sensor_id = source.sensor_id AND
                        target.timestamp_utc = source.timestamp_utc AND
                        target.parameter_id = source.parameter_id AND
                        target.location_id = source.location_id
                    )
                    WHEN NOT MATCHED THEN
                        INSERT (date_id, time_id, location_id, sensor_id, parameter_id, timestamp_utc, value)
                        VALUES (source.date_id, source.time_id, source.location_id, source.sensor_id, source.parameter_id, source.timestamp_utc, source.value)
                    WHEN MATCHED THEN
                        UPDATE SET value = source.value;
                """
                # Copy into
                #logging.info(f"Loading data into {SNOWFLAKE_TABLE}")
                #cur.execute(copy_into)

                # Merge into
                logging.info(f"Merging data into {SNOWFLAKE_TABLE}")
                cur.execute(merge_stmt)

                # Show table preview
                logging.info(f"Previewing first 5 lines of {SNOWFLAKE_TABLE}")
                cur.execute(preview_table)
                for row in cur:
                    print(row)


    except Exception as e:
        print(f"The task failed: {e}")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    load_fact_measurements()


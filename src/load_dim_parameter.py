"""
Load transformed .parquet files from S3 to analytics.dim_parameter table to Snowflake.
"""
# ### IMPORTS ###
from utils.snowflake_connector import get_snowflake_connection
import logging

# Constants
SNOWFLAKE_TABLE = "dim_parameter"
S3_STAGE_NAME = "@CLEAN_AIR_DB.PROCESSED.dim_parameter_stage"
PROCESSED_PREFIX = "processed/dim_parameter"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

def load_dim_parameter():
    truncate_table = f"TRUNCATE table {SNOWFLAKE_TABLE};"
    list_stage_content = f"LIST {S3_STAGE_NAME};"
    get_most_recent_stage_element = """
        SELECT "name" AS latest_file
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            ORDER BY "last_modified" DESC
            LIMIT 1;
    """
    
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
                cur.execute(get_most_recent_stage_element)
                print(f"Most recent file: {cur.fetchone()[0]}")

    except Exception as e:
        print(f"The task failed: {e}")

# =============================================================================
# 4. SCRIPT EXECUTION
# =============================================================================
if __name__ == "__main__":
    load_dim_parameter()
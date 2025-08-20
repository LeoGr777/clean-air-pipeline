"""
Transforms raw meausurement data and uploads it to S3.
"""

# ### IMPORTS ###

# 1.1 Standard Libraries
import os
import logging
import datetime as dt

# 1.2 Third-party libraries
from dotenv import load_dotenv
import boto3
import pandas as pd

dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# 1.3 Local application modules
from utils.transform_utils import list_s3_keys_by_prefix, transform_json_to_parquet, archive_s3_file
from utils.extract_openaq_utils import upload_to_s3

# ─── Load env vars and set up logging ──────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─── Configuration Specific to Locations ───────────────────────────────────────
BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = "raw/sensors"
PROCESSED_PREFIX = "processed/dim_sensor"
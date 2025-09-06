"""
Utility script to connect to Snowflake.
"""

# Imports
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.connector import SnowflakeConnection
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import Iterator
from contextlib import contextmanager


dotenv_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=dotenv_path)


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)


@contextmanager
def get_snowflake_connection() -> Iterator[SnowflakeConnection]:
    """
    Creates and manages a connection to Snowflake using a context manager.

    This function is a generator that yields a Snowflake connection object.

    Yields:
        Iterator[SnowflakeConnection]: The active Snowflake connection object.

    Raises:
        snowflake.connector.Error: If the connection fails.
    """

    conn = None
    try:
        private_key_str = os.getenv("SNOWFLAKE_PRIVATE_KEY")
        if not private_key_str:
            raise ValueError("SNOWFLAKE_PRIVATE_KEY environment variable not set.")

        passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRISE")
        encoded_passphrase = passphrase.encode() if passphrase else None

        private_key = serialization.load_pem_private_key(
            private_key_str.encode(), password=encoded_passphrase
        )

        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=pkb,
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        logging.info("Snowflake connection successful")
        yield conn
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Snowflake connection closed.")

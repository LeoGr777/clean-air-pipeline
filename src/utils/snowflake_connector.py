"""
Utility script to connect to Snowflake.
"""
# ### IMPORTS ###
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from snowflake.connector import SnowflakeConnection
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from typing import Iterator
from contextlib import contextmanager


# --- Configuration ---
# Load environment variables
dotenv_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=dotenv_path)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

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
        # Load the private key from the file
        with open("key/rsa_key.p8", "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").encode()
        )

        # Get the key in bytes format
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            private_key=pkb,
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        ) 
        logging.info("Snowflake connection successful")
        # Pass the connection object to the 'with' block
        yield conn
    except Exception as e:
        logging.error(f"Snowflake connection failed: {e}", exc_info=True)
        # Re-raise the exception to let the calling script know something went wrong
        raise
    finally:
        # This block is executed when the 'with' statement is exited
        if conn: 
            conn.close()
            logging.info("Snowflake connection closed.")
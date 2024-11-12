# Test connection to snowflake

import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = None

try:
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    print("Connection successful!")
except snowflake.connector.errors.OperationalError as e:
    print(f"Failed to connect: {e}")
finally:
    if conn:
        conn.close()

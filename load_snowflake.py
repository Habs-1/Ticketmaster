import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


def load_snowflake(df: pd.DataFrame, table_name: str):

    load_dotenv()
    # Snowflake login from .env
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

    engine = create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}')

    try:
        df.to_sql(table_name, con=engine, index=False, if_exists='replace')
        num_events = len(df)
        print(f"Loaded {num_events} events successfully into Snowflake: {table_name}.")
    except Exception as e:
        print(f"Failed to load data into Snowflake: {e}")
    finally:
        # Close connection
        engine.dispose()
        
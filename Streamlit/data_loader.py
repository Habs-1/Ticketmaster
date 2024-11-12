import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import warnings 

warnings.filterwarnings("ignore", message="Bad owner or permissions on .*connections.toml")

load_dotenv()

def get_snowflake_engine():
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    
    return create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}')

def query_snowflake(query: str) -> pd.DataFrame:
    engine = get_snowflake_engine()
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df

def get_unique_values(column_name: str) -> list:
    query = f"SELECT DISTINCT {column_name} FROM Events;"
    engine = get_snowflake_engine()
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result[column_name].dropna().unique().tolist()

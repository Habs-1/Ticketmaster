from dotenv import load_dotenv
import streamlit as st
import snowflake.connector
import pandas as pd
import os
import warnings


warnings.filterwarnings("ignore", message="Bad owner or permissions on .*connections.toml")

# Write directly to the app
st.title("Ticketmaster Event Visualization")
st.write("Brandon Habschied bjh3420@rit.edu")
st.write("[docs.streamlit.io](https://docs.streamlit.io)")

# Get the current credentials    
load_dotenv()

conn = snowflake.connector.connect(
    user = os.getenv('SNOWFLAKE_USER'),
    password = os.getenv('SNOWFLAKE_PASSWORD'),
    account = os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE'),
    database = os.getenv('SNOWFLAKE_DATABASE'),
    schema = os.getenv('SNOWFLAKE_SCHEMA')
)

def query_snowflake(query: str) -> pd.DataFrame:

    df = pd.read_sql(query, conn)
    conn.close()
    return df


# Use an interactive slider to get user input
hifives_val = st.slider(
    "Number of anything from 0-90",
    min_value=0,
    max_value=90,
    value=60,
    help="Use this slider to slide   to different values",
)

query = "SELECT * FROM Events"  
data_df = query_snowflake(query)

st.write("Sample Query from Snowflake:")
st.dataframe(data_df)
from load_snowflake import load_snowflake
from pull_data import fetch_events
from pull_data import events_to_dataframe_allCol
from transform import flatten_nested_json
from transform import init_drop
from transform import transform_data
from pull_data import get_existing_events
from pull_data import filter_new_events
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import warnings

load_dotenv()

warnings.filterwarnings("ignore", message="Bad owner or permissions on .*connections.toml")
warnings.filterwarnings("ignore", message="The GenericFunction 'flatten' is already registered and is going to be overridden.")

def run_pipeline():
    print("### Fetching Events ###")
    events = fetch_events()
    print("### Events Fetched ###")

    # Changed mind on removing existing events so updates are reflected. Will load and replace any existing event IDs
    #
    # print('### Checking existing Events ###')
    # engine = create_engine(f'snowflake://{os.getenv("SNOWFLAKE_USER")}:{os.getenv("SNOWFLAKE_PASSWORD")}@{os.getenv("SNOWFLAKE_ACCOUNT")}/{os.getenv("SNOWFLAKE_DATABASE")}/{os.getenv("SNOWFLAKE_SCHEMA")}?warehouse={os.getenv("SNOWFLAKE_WAREHOUSE")}')
    # existing_events_df = get_existing_events(engine)
    # engine.dispose()
    # existing_events_id = existing_events_df['id']
    # if not existing_events_df.empty:
    #     print('### Existing Events Found ###')
    # else:
    #     print("### No Existing Events Found ###")

    print("### Creating DF to Load Events ###")
    df = events_to_dataframe_allCol(events)
    print("### DF Created ###")

    # Changed mind on removing existing events so updates are reflected. Will load and replace any existing event IDs
    # print("### Removing Existing Events ###")
    # new_events_df = filter_new_events(df, existing_events_id)
    # new_events_len = len(new_events_df)
    # print(f"### {new_events_len} New Events to Add ###")

    print("### Cleaning DF of Events ###")
    df_clean1 = init_drop(df)
    print("### DF Cleaned ###")

    print("### Flattening Nested DF ###")
    flat_df = flatten_nested_json(df_clean1)
    print("### DF Flattened ###")

    print("### Cleaning DF Again ###")
    df_clean = transform_data(flat_df)
    print("### DF Cleaned ###")
    
    print("### Loading Events ###")
    load_snowflake(df_clean, "Events")
    # df_clean.to_csv("test.csv")
    num_events = len(df_clean)
    print(f"### {num_events} Events Loaded ###")


if __name__ == "__main__":
    run_pipeline()

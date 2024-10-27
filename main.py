from load_snowflake import load_snowflake
from pull_data import fetch_events
from pull_data import events_to_dataframe_allCol
from transform import flatten_nested_json
from transform import init_drop
from transform import transform_data
import warnings

warnings.filterwarnings("ignore", message="Bad owner or permissions on .*connections.toml")
warnings.filterwarnings("ignore", message="The GenericFunction 'flatten' is already registered and is going to be overridden.")

def run_pipeline():
    print("###Fetching Events###")
    events = fetch_events()
    print("###Events Fetched###")

    print("###Creating DF###")
    df = events_to_dataframe_allCol(events)
    print("###DF Created###")

    print("###Inital Cleaning DF###")
    df_clean1 = init_drop(df)
    print("###DF Cleaned###")

    print("###Flattening DF###")
    flat_df = flatten_nested_json(df_clean1)
    print("###DF Flattened###")

    print("###Cleaning DF Again###")
    df_clean = transform_data(flat_df)
    print("###DF Cleaned###")
    
    print("###Loading Events###")
    load_snowflake(df_clean, "Events")
    df_clean.to_csv("test.csv")
    num_events = len(df_clean)
    print(f"{num_events} events")
    print("###Events Loaded###")

if __name__ == "__main__":
    run_pipeline()

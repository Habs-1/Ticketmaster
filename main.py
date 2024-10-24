from load_snowflake import load_snowflake
from pull_data import fetch_events
from pull_data import events_to_dataframe_allCol

def run_pipeline():

    events = fetch_events()

    df = events_to_dataframe_allCol(events)

    load_snowflake(df, "Events")


if __name__ == "__main__":
    run_pipeline()

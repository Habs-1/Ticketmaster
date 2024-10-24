from load_snowflake import load_snowflake
from pull_data import fetch_events
from pull_data import events_to_dataframe_allCol
from transform import transform_data

def run_pipeline():

    events = fetch_events()

    df = events_to_dataframe_allCol(events)

    test_df = ['name','classifications']
    test_df = df[test_df]
    # df.to_csv('test.csv', index=False)

    # df_clean = transform_data(df)

    load_snowflake(test_df, "Events")
    # load_snowflake(df_clean, "Events")


if __name__ == "__main__":
    run_pipeline()

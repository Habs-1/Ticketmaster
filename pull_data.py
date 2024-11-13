import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from load_snowflake import load_snowflake
from urllib.parse import quote_plus 

load_dotenv()

def fetch_events():
    api_key = os.getenv('API_KEY')
    
    url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    
    params = {
        'apikey': api_key,
        'countryCode': 'US',                      # Limit results to the US
        'startDateTime': '2024-12-01T00:00:00Z',  # Start date
        ### Deep paging issues over 1000 items, limiting to 1 month for now. will need to add batching
        'endDateTime': '2024-12-31T23:59:59Z',    # End date
        'size': 200,                              # Max events per page
        'page': 0,                                # Start at page 0
        'sort': 'date,asc'                        # Sort by date ascending
    }
    
    all_events = []  # To store all events data
    
    while True:
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code != 200:
            print(f"Error: Unable to fetch data (status code {response.status_code})")
            break
        
        data = response.json()

        # Check if there are any events in the response
        if '_embedded' not in data or 'events' not in data['_embedded']:
            print("No more events found.")
            break
        
        # Extract events from the current page
        events = data['_embedded']['events']
        all_events.extend(events)
        
        # Print the number of events fetched on this page
        print(f"Fetched {len(events)} events on page {params['page']}")
        
        # Check if there are more pages
        if params['page'] >= data['page']['totalPages'] - 1:
            print("All pages have been fetched.")
            break # breaks the while True when out of pages
        
        # Increment to the next page
        params['page'] += 1
    
    return all_events

# def events_to_dataframe(events):
#     # Create a list to store processed event data
#     event_list = []

#     for event in events:
#         # Extract fields from each event
#         event_name = event.get('name')
#         event_date = event['dates']['start'].get('localDate')
#         event_time = event['dates']['start'].get('localTime', 'N/A')  
#         venue_name = event['_embedded']['venues'][0].get('name')

#         event_list.append([event_name, event_date, event_time, venue_name])

#     # Convert the list to a DataFrame
#     df = pd.DataFrame(event_list, columns=['Event Name', 'Date', 'Time', 'Venue'])
#     return df


# Puts all fields into DF from JSON
def events_to_dataframe_allCol(events):
    df = pd.json_normalize(events, sep='_')

    return df

# Below for checking existing events so we only load new events. 
# I am deciding to not do this incase there are changes. Will continue to load all data and use replace for existing IDs
#
# def get_existing_events(engine) -> pd.DataFrame:
#     query = f"""SELECT id FROM "Events";"""
#     with engine.connect() as connection:
#         existing_events = pd.read_sql(query, connection)
#     return existing_events

# def filter_new_events(df: pd.DataFrame, existing_ids: pd.Series) -> pd.DataFrame:
#     return df[~df['id'].isin(existing_ids)]

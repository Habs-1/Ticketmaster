import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from load_snowflake import load_snowflake
from urllib.parse import quote_plus 

load_dotenv()
# Snowflake login from .env
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = quote_plus(os.getenv('SNOWFLAKE_PASSWORD'))  # Use urllib to parse special characters in PW
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')  
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

engine = create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}')

def fetch_events():
    # Your Ticketmaster API key
    api_key = os.getenv('API_KEY')
    
    # Base URL for Ticketmaster API
    url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    
    # Initial API parameters
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
        # Fetch the events for the current page
        response = requests.get(url, params=params)
        
        # Check if the request was successful
        if response.status_code != 200:
            print(f"Error: Unable to fetch data (status code {response.status_code})")
            break
        
        # Get the JSON response
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
            break
        
        # Increment to the next page
        params['page'] += 1
    
    
    return all_events

def events_to_dataframe(events):
    # Create a list to store processed event data
    event_list = []

    for event in events:
        # Extract relevant fields from each event
        event_name = event.get('name')
        event_date = event['dates']['start'].get('localDate')
        event_time = event['dates']['start'].get('localTime', 'N/A')  
        venue_name = event['_embedded']['venues'][0].get('name')
    #    venue_city = event['_embedded']['venues'][0]['city'].get('name', 'N/A')
     #   venue_state = event['_embedded']['venues'][0]['state'].get('name', 'N/A')  
        
        # Append event data to the list
        event_list.append([event_name, event_date, event_time, venue_name #, venue_city, venue_state
                           ])

    # Convert the list to a DataFrame
    df = pd.DataFrame(event_list, columns=['Event Name', 'Date', 'Time', 'Venue' #, 'City', 'State'
                                           ])
    return df


if __name__ == "__main__":
    events_data = fetch_events()
    events_df = events_to_dataframe(events_data)

    load_snowflake(events_df, "Test Table")

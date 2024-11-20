import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from load_snowflake import load_snowflake
from urllib.parse import quote_plus
from datetime import datetime, timedelta

load_dotenv()

num_days = 4
num_hours = num_days * 24

def date_range(start_date, days=num_days):
    # Helper function to generate the start and end dates in ISO format for the time period input
    end_date = start_date + timedelta(days=days)
    return start_date.strftime('%Y-%m-%dT00:00:00Z'), end_date.strftime('%Y-%m-%dT23:59:59Z')


def fetch_events():
    api_key = os.getenv('API_KEY')
    url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    all_events = []  # To store all events data
    start_date = datetime(2024, 12, 1)  # Start date
    end_limit = datetime(2024, 12, 31)  # End date limit
       
    while start_date < end_limit:
        start_date_str, end_date_str = date_range(start_date, days=num_days)
    
        initial_params = {
            'apikey': api_key,
            'countryCode': 'US',                      # Limit results to the US
            'startDateTime': start_date_str,          # Start of the range in UTC
            'endDateTime': end_date_str,              # End of the range in UTC
            'size': 200,                              # Max events per page
            'page': 0,                                
            'sort': 'date,asc',                       # Sort by date ascending
            '&source=': 'Ticketmaster'                # Ticketmaster source only. no resale  
        }

        response = requests.get(url, params=initial_params)
        print(f"Request URL: {response.url}")

        # Check if the request was successful
        if response.status_code != 200:
            print(f"Error: Unable to fetch data (status code {response.status_code})")
            break
        
        data = response.json()
        total_events = data.get('page', {}).get('totalElements', 0)
        print(f"{total_events} total events for {start_date_str} thru {end_date_str}")

        num_splits = max((total_events // 1000) + 2, 1)
        print(f"Total events for {start_date.date()} to {(start_date + timedelta(days=num_days)).date()}: {total_events}. Splitting into {num_splits} periods.")

        for i in range(num_splits):
            split_start = start_date + timedelta(hours=i * (num_hours // num_splits))  
            split_end = split_start + timedelta(hours=(num_hours // num_splits) - 1, minutes=59, seconds=59)
            split_start_str = split_start.strftime('%Y-%m-%dT%H:%M:%SZ')
            split_end_str = split_end.strftime('%Y-%m-%dT%H:%M:%SZ')

            page = 0
            while True:
                params = {
                    'apikey': api_key,
                    'countryCode': 'US',
                    'startDateTime': split_start_str,
                    'endDateTime': split_end_str,
                    'size': 200,
                    'page': page,
                    'sort': 'date,asc',
                    '&source=': 'Ticketmaster'
                }

                response = requests.get(url, params=params)
                if response.status_code != 200:
                    print(f"Error: Unable to fetch data (status code {response.status_code})")
                    break

                data = response.json()
                if '_embedded' not in data or 'events' not in data['_embedded']:
                    print("No more events found.")
                    break

                events = data['_embedded']['events']
                all_events.extend(events)
                print(f"Fetched {len(events)} events from {split_start_str} to {split_end_str} on page {page}")

                if page >= data['page']['totalPages'] - 1:
                    print(f"All pages for {split_start_str} to {split_end_str} have been fetched.")
                    break

                page += 1

        start_date += timedelta(days=num_days)

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

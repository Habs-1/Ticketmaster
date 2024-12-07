import requests
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


def fetch_events():
    api_key = os.getenv('API_KEY')
    url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    all_events = []
    start_date = datetime(2024, 12, 10)
    end_limit = datetime(2025, 1, 10)

    while start_date < end_limit:
        end_date = start_date + timedelta(days = 3)  # Adjust the date chunk interval here
        if end_date > end_limit:
            end_date = end_limit

        print(f"Fetching events from {start_date} to {end_date}")
        all_events.extend(fetch_split_events(api_key, url, start_date, end_date))

        start_date = end_date + timedelta(days=1)

    return all_events


def fetch_split_events(api_key, url, start_date, end_date, max_events = 1000):
    all_events = []
    params = {
        'apikey': api_key,
        'countryCode': 'US',
        'startDateTime': start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'endDateTime': end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'size': 200,
        'page': 0,
        'sort': 'date,asc',
        '&source=': 'Ticketmaster'
    }
    min_granularity = timedelta(seconds = 1) # timestamps within 1s of eachother are treated as the same to try to stop infinite loops in recursion

    response = requests.get(url, params=params)
    print(f"Request URL: {response.url}")

    if response.status_code != 200:
        print(f"Error: Unable to fetch data (status code {response.status_code})")
        return []

    data = response.json()
    total_events = data.get('page', {}).get('totalElements', 0)

    if total_events > max_events:
        print(f"{total_events} total events for {start_date} to {end_date}. Splitting the period.")
        
        if end_date - start_date <= min_granularity:
            print(f"Cannot split further: {start_date} to {end_date} has reached minimum granularity.")
            return []

        # Split the range into two and recurse
        midpoint = start_date + (end_date - start_date) / 2
        print(f"Splitting period {start_date} to {end_date} into {start_date} to {midpoint} and {midpoint} to {end_date}")

        if midpoint <= start_date or midpoint >= end_date:
            print(f"Cannot split further: {start_date} to {end_date} has reached minimum granularity.")
            return []

        all_events.extend(fetch_split_events(api_key, url, start_date, midpoint, max_events))
        all_events.extend(fetch_split_events(api_key, url, midpoint, end_date, max_events))
    else:
        print(f"{total_events} total events for {start_date} to {end_date}")
        # Fetch events for this period
        page = 0
        while True:
            params['page'] = page
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
            print(f"Fetched {len(events)} events from {start_date} to {end_date} on page {page}")

            if page >= data['page']['totalPages'] - 1:
                print(f"All pages for {start_date} to {end_date} have been fetched.")
                break

            page += 1

    return all_events



# Puts all fields into DF from JSON
def events_to_dataframe_allCol(events):
    df = pd.json_normalize(events, sep='_')

    return df

import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv

load_dotenv()

def fetch_events():
    # Your Ticketmaster API key
    api_key = os.getenv('API_KEY')
    
    # Base URL for Ticketmaster API
    url = 'https://app.ticketmaster.com/discovery/v2/events.json'
    
    # Initial API parameters
    params = {
        'apikey': api_key,
        'countryCode': 'US',                      # Limit results to the US
        'startDateTime': '2024-01-01T00:00:00Z',  # Start date
        ### Deep paging issues over 1000 items, limiting to 1 month for now. will need to add batching
        'endDateTime': '2024-01-31T23:59:59Z',    # End date
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

        ### RAW JSON TEST PRINTS
       #print('###JSON PRETTY###')
       #print(json.dumps(data, indent=4))
       #print('###END JSON PRETTY###')
        print(data.keys())
        print('##JSON KEYS##')
        if '_embedded' in data:
            print(data['_embedded'].keys())
            # Check the keys within each event
            first_event = data['_embedded']['events'][0]
            print(first_event.keys())
        print('##first event##')
        first_event = data['_embedded']['events'][0]
        print(json.dumps(first_event, indent=4))
        print('###Normalized###')
        df_flattened = pd.json_normalize(data, sep='_')
        print(df_flattened)
        print(df_flattened.columns)
        print('###END###')

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
    # Fetch all events for the US with pagination
    events_data = fetch_events()
    
    # Convert the events to a Pandas DataFrame
    events_df = events_to_dataframe(events_data)
    
    # Display the first few rows of the DataFrame
    print(events_df.head())
    print(events_df)
    
    
    # Optionally, save the DataFrame to a CSV file
#    events_df.to_csv("us_events.csv", index=False)
 #   print("Events saved to us_events.csv")

import requests

# Your Ticketmaster API key (replace with your actual key)
api_key = "ad8GbFvZQrIcPIxARQ1KO5oDJMMXs4ty"

# Base URL for the Ticketmaster Discovery API
base_url = "https://app.ticketmaster.com/discovery/v2/events.json"

# Define parameters for the API request
params = {
    "apikey": api_key,
    "keyword": "music",  # Example search term
    "city": "New York",
    "countryCode": "US"
}

# Make the API request
response = requests.get(base_url, params=params)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    print("Success! Event data received:")
    print(data)  # Display the raw JSON data
else:
    print(f"Error: Unable to fetch data (status code {response.status_code})")

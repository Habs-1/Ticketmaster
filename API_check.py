import os
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def check_api_usage():
    url = "https://app.ticketmaster.com/discovery/v2/events.json"

    api_key = os.getenv("API_KEY")
    if not api_key:
        print("Error: API key not found. Make sure it's set in the .env file.")
        return

    params = {
        "apikey": api_key,
        "countryCode": "US",
        "size": 1
    }

    try:
        response = requests.get(url, params=params)
        
        remaining_requests = response.headers.get("Rate-Limit-Available", "Unknown")

        print(f"API Usage Details:")
        print(f"  Total Requests Remaining: {remaining_requests}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    check_api_usage()

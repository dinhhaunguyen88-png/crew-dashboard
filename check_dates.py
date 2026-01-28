import requests
import re
import json

def verify_dates():
    url = "http://localhost:5000/?source=aims"
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            match = re.search(r'const dashboardData = (\{.*?\});', resp.text, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
                dates = data.get('available_dates', [])
                print(f"URL: {url}")
                print(f"Total Flights: {data.get('summary', {}).get('total_flights')}")
                print(f"Available Dates Count: {len(dates)}")
                if dates:
                    print(f"First 5 dates: {dates[:5]}")
                else:
                    print("ERROR: available_dates is EMPTY!")
            else:
                print("Could not find dashboardData")
        else:
            print(f"Error {resp.status_code}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    verify_dates()

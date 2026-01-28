import requests
import re
import json

def verify_live():
    url = "http://localhost:5000/?source=aims"
    try:
        print(f"Fetching {url}...")
        resp = requests.get(url)
        print(f"Status Code: {resp.status_code}")
        
        if resp.status_code == 200:
            content = resp.text
            # Look for const dashboardData = ...
            match = re.search(r'const dashboardData = (\{.*?\});', content, re.DOTALL)
            if match:
                js_obj = match.group(1)
                # It might be using 'tojson', so it should be valid JSON
                try:
                    data = json.loads(js_obj)
                    summary = data.get('summary', {})
                    total_flights = summary.get('total_flights', 0)
                    available_dates = data.get('available_dates', [])
                    print(f"LIVE Dashboard Total Flights: {total_flights}")
                    print(f"LIVE Dashboard Available Dates Count: {len(available_dates)}")
                    
                    if total_flights > 0 and len(available_dates) > 0:
                        print("SUCCESS: Live server is returning AIMS data with dates.")
                    elif total_flights > 0:
                        print("PARTIAL SUCCESS: Flights found but dates are still empty.")
                    else:
                        print("FAILURE: Live server returned 0 flights.")
                        
                    # Also check is_aims_source flag if I added it
                    print(f"Is AIMS Source: {data.get('is_aims_source', False)}")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON Parse Error: {e}")
            else:
                print("Could not find dashboardData in response.")
        else:
            print(f"Server Error: {resp.text[:500]}")
            
    except Exception as e:
        print(f"Connection Error: {e}")

if __name__ == '__main__':
    verify_live()

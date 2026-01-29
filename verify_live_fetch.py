from data_processor import get_processor
import json
from datetime import datetime

print("Initializing processor...")
processor = get_processor()

# Create a dummy filter date (Today)
today_str = datetime.now().strftime('%d/%m/%y')
print(f"Fetching dashboard data for {today_str}...")

data = processor.get_dashboard_data(filter_date=today_str)

print("\n--- Dashboard Data Summary ---")
print(json.dumps(data['summary'], indent=2))

print(f"\nData Source Crew: {data.get('data_source_crew', 'CSV/Local')}")

if data.get('data_source_crew') == 'AIMS Live':
    print("SUCCESS: Live data integrated!")
else:
    print("WARNING: Live data NOT used (check AIMS_ENABLED or connection).")

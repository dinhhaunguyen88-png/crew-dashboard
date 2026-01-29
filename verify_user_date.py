from data_processor import get_processor
import json

print("Initializing processor...")
processor = get_processor()

# Target date from user's screenshot
target_date = "14/01/26"
print(f"Testing dashboard data for Source='aims', Date={target_date}...")

data = processor.get_dashboard_data(filter_date=target_date, source='aims')

print("\n--- Dashboard Data Summary ---")
print(json.dumps(data['summary'], indent=2))

print(f"\nData Source Crew: {data.get('data_source_crew', 'N/A')}")
print(f"Is AIMS Source: {data.get('is_aims_source', False)}")
print(f"Operating Crew Count: {len(data.get('operating_crew', []))}")

if data['summary']['total_crew'] > 0:
    print("\nSUCCESS: Live data integrated for 14/01/26!")
else:
    print("\nWARNING: Live data still 0 for 14/01/26. Check AIMS response or date range.")

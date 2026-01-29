from data_processor import get_processor
import json

processor = get_processor()
flights = processor.aims_flights
print(f"Total AIMS flights: {len(flights)}")

if flights:
    sample = flights[0]
    print("\n--- Sample Flight ---")
    print(json.dumps(sample, indent=2))
    
    # Let's see some dates
    dates = sorted(list(set(f.get('flight_date') or f.get('date') for f in flights[:100] if f.get('flight_date') or f.get('date'))))
    print(f"\nSample Dates: {dates[:10]}")

from data_processor import get_processor

processor = get_processor()
flights = processor.aims_flights
dates = sorted(list(set(f.get('flight_date') or f.get('date') for f in flights if f.get('flight_date') or f.get('date'))))

print(f"Total dates in AIMS flights: {len(dates)}")
print(f"Dates sample: {dates[:20]}")

target = "14/01/2026"
target_short = "14/01/26"
if target in dates:
    print(f"FOUND {target}!")
elif target_short in dates:
    print(f"FOUND {target_short}!")
else:
    print(f"NOT FOUND {target} or {target_short} in database flights.")

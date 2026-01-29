from data_processor import get_processor
from datetime import datetime

processor = get_processor()
flights = processor.aims_flights

target_date_str = "14/01/26"
day, month, year_short = target_date_str.split('/')
y_int = int(year_short)
year = y_int + 2000 if y_int < 100 else y_int
target_date = datetime(year, int(month), int(day))

target_d_full = target_date.strftime('%d/%m/%Y')
target_d_short = target_date.strftime('%d/%m/%y')

print(f"Targets: {[target_d_full, target_d_short]}")

flight_reg_map = {}
matched_count = 0
for f in flights:
    f_date = str(f.get('flight_date') or f.get('date') or '').strip()
    
    # Debug a few flights from Jan 14
    if "14/01" in f_date:
        matched_count += 1
        if matched_count < 5:
            print(f"  Found potential date match: '{f_date}' against targets")
    
    if f_date == target_d_full or f_date == target_d_short or f_date.replace('/0', '/') == target_d_full.replace('/0', '/'):
        f_no = str(f.get('flight_no') or f.get('flt') or '')
        reg = f.get('reg') or f.get('ac_reg')
        if f_no and reg:
            flight_reg_map[f_no] = reg

print(f"\nFinal Map Size: {len(flight_reg_map)}")

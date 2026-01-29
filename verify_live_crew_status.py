from aims_soap_client import get_aims_client
from datetime import datetime
import json

client = get_aims_client()
client._init_client()

target_date = datetime(2026, 1, 14)
base = 'SGN'

print(f"--- Testing Bulk Crew Status for {base} on {target_date.strftime('%d/%m/%Y')} ---")
try:
    result = client.get_bulk_crew_status(target_date, base=base)
    if result.get('success'):
        print("\nSUCCESS!")
        print(f"Total Crew: {result.get('total_crew')}")
        print(f"Sampled: {result.get('sampled_crew')}")
        print("\nSummary Counts:")
        print(json.dumps(result.get('summary'), indent=2))
        
        counts = result.get('summary')
        if counts.get('SBY', 0) > 0:
            print(f"\n✅ Standby found: {counts['SBY']}")
        else:
            print("\n⚠️ No Standby found in sample.")
            
        if counts.get('SL', 0) > 0 or counts.get('CSL', 0) > 0:
            print(f"✅ Sick leave found: SL={counts['SL']}, CSL={counts['CSL']}")
        else:
            print("⚠️ No Sick leave found in sample.")
    else:
        print(f"\nFAILED: {result.get('error')}")
except Exception as e:
    print(f"\nERROR: {e}")

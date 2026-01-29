from aims_soap_client import get_aims_client
from datetime import datetime, timedelta
import collections

client = get_aims_client()
client._init_client()

target_date = datetime(2026, 1, 14)
from_parts = client._format_date_parts(target_date)

print("--- Inspecting FetchCrewQuals Items ---")
try:
    response = client._service.FetchCrewQuals(
        UN=client.username,
        PSW=client.password,
        FmDD="01", FmMM="01", FmYYYY="2026",
        ToDD="31", ToMM="01", ToYYYY="2026",
        CrewID=0,
        PrimaryQualify=True,
        GetAllQualsInPeriod=False
    )
    
    quals_list = getattr(response, 'QualsList', None)
    if quals_list:
        items = quals_list.TAIMSCrewQual if hasattr(quals_list, 'TAIMSCrewQual') else []
        if items:
            sample = items[0]
            print(f"\nItem Type: {type(sample)}")
            attrs = [a for a in dir(sample) if not a.startswith('_')]
            print(f"Attributes: {attrs}")
            # Specifically check for SBY-related fields
            for attr in attrs:
                if 'sby' in attr.lower() or 'exempt' in attr.lower():
                    print(f"  FOUND POTENTIAL SBY FIELD: {attr} = {getattr(sample, attr)}")
except Exception as e:
    print(f"FetchCrewQuals ERROR: {e}")

print("\n--- Fetching Roster for 50 crew members ---")
codes = collections.Counter()
# We need some valid IDs. Let's try to get them from FetchCrewQuals if it worked
try:
    if items:
        valid_ids = [getattr(i, 'ID', None) for i in items[:50] if getattr(i, 'ID', None)]
        print(f"Testing with {len(valid_ids)} crew IDs...")
        for cid in valid_ids:
            try:
                response = client._service.CrewMemberRosterDetailsForPeriod(
                    UN=client.username,
                    PSW=client.password,
                    ID=cid,
                    FmDD="01", FmMM="01", FmYY="2026",
                    ToDD="31", ToMM="01", ToYY="2026"
                )
                if hasattr(response, 'TAIMSCrewRostDetailList') and response.TAIMSCrewRostDetailList:
                    for item in response.TAIMSCrewRostDetailList.TAIMSCrewRostItm:
                        code = getattr(item, 'Flt', None)
                        if code:
                            codes[code] += 1
            except:
                pass
except:
    pass

print("\nMost common duty codes:")
for code, count in codes.most_common(30):
    if not code.isdigit(): # Focus on non-flight codes
        print(f"  {code}: {count}")
    elif count > 5:
        print(f"  {code}: {count}")

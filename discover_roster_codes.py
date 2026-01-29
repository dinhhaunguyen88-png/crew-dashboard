from aims_soap_client import get_aims_client
from datetime import datetime, timedelta
import collections

client = get_aims_client()
client._init_client()

target_date = datetime(2026, 1, 14)
from_parts = client._format_date_parts(target_date)

print("--- Testing FetchCrewQuals with full parameters ---")
try:
    response = client._service.FetchCrewQuals(
        UN=client.username,
        PSW=client.password,
        FmDD=from_parts['DD'],
        FmMM=from_parts['MM'],
        FmYYYY=from_parts['YYYY'],
        ToDD=from_parts['DD'],
        ToMM=from_parts['MM'],
        ToYYYY=from_parts['YYYY'],
        CrewID=0,
        PrimaryQualify=True,
        GetAllQualsInPeriod=False
    )
    print("FetchCrewQuals SUCCESS")
    # Check if any objects have ExemptSBY
    if hasattr(response, 'QualsList') and response.QualsList:
        items = response.QualsList.TAIMSCrewQual if hasattr(response.QualsList, 'TAIMSCrewQual') else []
        if items:
            sample = items[0]
            print("\nQual Item Sample:")
            for attr in dir(sample):
                if not attr.startswith('_'):
                    print(f"  {attr}: {getattr(sample, attr)}")
except Exception as e:
    print(f"FetchCrewQuals FAILED: {e}")

print("\n--- Fetching Roster for multiple crew to find SL/CSL codes ---")
# Let's fetch roster for a few crew members to see variety
crew_ids = [16195, 100, 200, 300, 400, 500]
codes = collections.Counter()

for cid in crew_ids:
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
                code = getattr(item, 'Flt', None) # Flt is often used for duty code in rosters
                if code:
                    codes[code] += 1
    except:
        pass

print("\nTop Duty Codes found in Jan 2026:")
for code, count in codes.most_common(20):
    print(f"  {code}: {count}")

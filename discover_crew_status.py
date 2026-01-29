from aims_soap_client import get_aims_client
from datetime import datetime, timedelta

client = get_aims_client()
client._init_client()

target_date = datetime(2026, 1, 14) # A date we know has data
from_parts = client._format_date_parts(target_date)

print("--- Testing GetCrewList ---")
response = client._service.GetCrewList(
    UN=client.username,
    PSW=client.password,
    ID=0,
    PrimaryQualify=True,
    FmDD=from_parts['DD'],
    FmMM=from_parts['MM'],
    FmYY=from_parts['YY'],
    ToDD=from_parts['DD'],
    ToMM=from_parts['MM'],
    ToYY=from_parts['YY'],
    BaseStr=''
)

if hasattr(response, 'CrewList') and response.CrewList:
    items = response.CrewList.TAIMSGetCrewItm
    if items:
        sample = items[0]
        print("\nGetCrewList Item Structure:")
        for attr in dir(sample):
            if not attr.startswith('_') and not callable(getattr(sample, attr)):
                print(f"  {attr}: {getattr(sample, attr)}")

print("\n--- Testing FetchCrewQuals ---")
response = client._service.FetchCrewQuals(
    UN=client.username,
    PSW=client.password,
    ID=0
)

items = getattr(response, 'QualsList', None) or getattr(response, 'CrewQuals', None)
if items:
    q_items = items if isinstance(items, list) else getattr(items, 'TAIMSCrewQual', [])
    if q_items:
        sample = q_items[0]
        print("\nFetchCrewQuals Item Structure:")
        for attr in dir(sample):
            if not attr.startswith('_') and not callable(getattr(sample, attr)):
                print(f"  {attr}: {getattr(sample, attr)}")

print("\n--- Testing CrewMemberRosterDetailsForPeriod ---")
# Pick a random crew ID or just try a common one
crew_id = 16195 # From previous debugs
response = client._service.CrewMemberRosterDetailsForPeriod(
    UN=client.username,
    PSW=client.password,
    ID=crew_id,
    FmDD=from_parts['DD'],
    FmMM=from_parts['MM'],
    FmYY=from_parts['YY'],
    ToDD=from_parts['DD'],
    ToMM=from_parts['MM'],
    ToYY=from_parts['YY']
)

if hasattr(response, 'TAIMSCrewRostDetailList') and response.TAIMSCrewRostDetailList:
    items = response.TAIMSCrewRostDetailList.TAIMSCrewRostItm
    if items:
        sample = items[0]
        print("\nCrewMemberRosterDetailsForPeriod Item Structure:")
        for attr in dir(sample):
            if not attr.startswith('_') and not callable(getattr(sample, attr)):
                print(f"  {attr}: {getattr(sample, attr)}")

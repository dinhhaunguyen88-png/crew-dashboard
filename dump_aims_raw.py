from aims_soap_client import get_aims_client
from datetime import datetime
import zeep

client = get_aims_client()
client._init_client()

target_dt = datetime(2026, 1, 14)
p = client._format_date_parts(target_dt)

print("--- RAW FetchCrewQuals (ID=16195) ---")
try:
    response = client._service.FetchCrewQuals(
        UN=client.username,
        PSW=client.password,
        FmDD="01", FmMM="01", FmYYYY="2026",
        ToDD="31", ToMM="01", ToYYYY="2026",
        CrewID=16195,
        PrimaryQualify=True,
        GetAllQualsInPeriod=False
    )
    print(zeep.helpers.serialize_object(response))
except Exception as e:
    print(f"Error: {e}")

print("\n--- RAW Roster (ID=16195, Jan 2026) ---")
try:
    response = client._service.CrewMemberRosterDetailsForPeriod(
        UN=client.username,
        PSW=client.password,
        ID=16195,
        FmDD="01", FmMM="01", FmYY="2026",
        ToDD="31", ToMM="01", ToYY="2026"
    )
    print(zeep.helpers.serialize_object(response))
except Exception as e:
    print(f"Error: {e}")

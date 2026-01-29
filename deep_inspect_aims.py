from aims_soap_client import get_aims_client
from datetime import datetime
import zeep

client = get_aims_client()
client._init_client()

print("--- Deep Inspection of FetchCrewQuals (ID=16195) ---")
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
    
    serialized = zeep.helpers.serialize_object(response)
    print("Serialized Response:")
    print(serialized)
    
    # Check for QualsList
    quals = getattr(response, 'QualsList', None)
    if quals:
        items = quals.TAIMSCrewQualsItm if hasattr(quals, 'TAIMSCrewQualsItm') else []
        if items:
            item = items[0]
            print("\nQual Item Dir:")
            print(dir(item))
            print("\nQual Item Data:")
            print(zeep.helpers.serialize_object(item))

except Exception as e:
    print(f"Error: {e}")

print("\n--- Deep Inspection of Roster (ID=16195) ---")
try:
    response = client._service.CrewMemberRosterDetailsForPeriod(
        UN=client.username,
        PSW=client.password,
        ID=16195,
        FmDD="14", FmMM="01", FmYY="2026", # One day
        ToDD="14", ToMM="01", ToYY="2026"
    )
    serialized = zeep.helpers.serialize_object(response)
    print(serialized)
except Exception as e:
    print(f"Error: {e}")

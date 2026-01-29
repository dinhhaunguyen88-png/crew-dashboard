from aims_soap_client import get_aims_client
from datetime import datetime
import zeep
import sys

# Set output to UTF-8
sys.stdout.reconfigure(encoding='utf-8')

client = get_aims_client()
client._init_client()

target_ids = [16195, 16196, 16197, 16198, 16199]

print("--- Searching for SL/CSL/SBY in Roster for 5 crew ---")
for cid in target_ids:
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
                code = str(getattr(item, 'Flt', '')).strip()
                if any(k in code.upper() for k in ['SBY', 'SL', 'CSL', 'SICK', 'BN', 'OM']):
                    print(f"Crew {cid} Date {getattr(item, 'Day', '')} Code: '{code}'")
    except:
        pass

print("\n--- Testing FetchCrewQuals for SBY fields ---")
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
        items = quals_list.TAIMSCrewQualsItm if hasattr(quals_list, 'TAIMSCrewQualsItm') else []
        for item in items[:10]:
            print(f"CrewID: {getattr(item, 'ID', '??')} Base: {getattr(item, 'QualBase', '??')} Pos: {getattr(item, 'QualPos', '??')}")
except Exception as e:
    print(f"Error: {e}")

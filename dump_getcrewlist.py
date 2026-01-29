from aims_soap_client import get_aims_client
from datetime import datetime
import zeep

client = get_aims_client()
client._init_client()

target_dt = datetime(2026, 1, 14)
p = client._format_date_parts(target_dt)

print("--- RAW GetCrewList (ID=16195) ---")
try:
    response = client._service.GetCrewList(
        UN=client.username,
        PSW=client.password,
        ID=16195,
        PrimaryQualify=True,
        FmDD=p['DD'], FmMM=p['MM'], FmYY=p['YY'].replace('20', ''), # YY is 2 bits
        ToDD=p['DD'], ToMM=p['MM'], ToYY=p['YY'].replace('20', ''),
        BaseStr=''
    )
    print(zeep.helpers.serialize_object(response))
except Exception as e:
    print(f"Error: {e}")

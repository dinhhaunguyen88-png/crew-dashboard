from aims_soap_client import get_aims_client
from datetime import datetime

client = get_aims_client()
client._init_client()

target_date = datetime(2026, 1, 14)
p = client._format_date_parts(target_date)

print(f"Testing GetCrewList with 2-digit YEAR ({p['YY']})...")
try:
    response = client._service.GetCrewList(
        UN=client.username,
        PSW=client.password,
        ID=0,
        PrimaryQualify=True,
        FmDD=p['DD'], FmMM=p['MM'], FmYY=p['YY'],
        ToDD=p['DD'], ToMM=p['MM'], ToYY=p['YY'],
        BaseStr='', ACStr='', PosStr=''
    )
    print("SUCCESS with 2-digit")
except Exception as e:
    print(f"FAILED with 2-digit: {e}")

print(f"\nTesting GetCrewList with 4-digit YEAR ({p['YYYY']})...")
try:
    response = client._service.GetCrewList(
        UN=client.username,
        PSW=client.password,
        ID=0,
        PrimaryQualify=True,
        FmDD=p['DD'], FmMM=p['MM'], FmYY=p['YYYY'],
        ToDD=p['DD'], ToMM=p['MM'], ToYY=p['YYYY'],
        BaseStr='', ACStr='', PosStr=''
    )
    print("SUCCESS with 4-digit")
except Exception as e:
    print(f"FAILED with 4-digit: {e}")

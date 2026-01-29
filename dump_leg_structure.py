from aims_soap_client import get_aims_client
from datetime import datetime

client = get_aims_client()
target_date = datetime(2026, 1, 14)

print(f"Fetching response for {target_date.strftime('%Y-%m-%d')}...")
# We'll call the service directly to see the raw zeep objects
client._init_client()
response = client._service.FetchLegMembersPerDay(
    UN=client.username,
    PSW=client.password,
    DD="14",
    MM="01",
    YY="2026"
)

day_member = getattr(response, 'DayMember', None)
if day_member:
    legs = getattr(day_member, 'TAIMSGetLegMembers', [])
    if legs:
        first_leg = legs[0] if isinstance(legs, list) else legs
        print("\n--- Leg Structure ---")
        for attr in dir(first_leg):
            if not attr.startswith('_'):
                val = getattr(first_leg, attr)
                # Skip methods
                if not callable(val):
                    print(f"{attr}: {val}")

        # Check FMember too
        if hasattr(first_leg, 'FMember'):
            members = getattr(first_leg.FMember, 'TAIMSMember', [])
            if members:
                first_member = members[0] if isinstance(members, list) else members
                print("\n--- Member Structure ---")
                for attr in dir(first_member):
                    if not attr.startswith('_'):
                        val = getattr(first_member, attr)
                        if not callable(val):
                            print(f"{attr}: {val}")
else:
    print("No DayMember found in response.")

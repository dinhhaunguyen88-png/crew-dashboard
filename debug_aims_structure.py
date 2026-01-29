import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from aims_soap_client import AIMSSoapClient

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)

client = AIMSSoapClient()

if not client.is_configured():
    print("Error: AIMS not configured.")
    exit(1)

print("Connecting to AIMS...")
client._init_client()

# Prepare date parameters (Current Day)
now = datetime.now()
print(f"Fetching data for: {now.strftime('%d/%m/%Y')}")

from_parts = client._format_date_parts(now)

try:
    # Inspect signature
    print("\n--- Method Signature ---")
    try:
        # Get operation object to see input signature
        op = client._service._binding._operations['FetchLegMembersPerDay']
        print(str(op.input.signature()))
    except:
        print("Could not get signature")

    print(f"\nParameters:")
    print(f"UN: {client.username}")
    print(f"PSW: {'*' * len(client.password) if client.password else 'None'}")
    print(f"DD: {from_parts['DD']}")
    print(f"MM: {from_parts['MM']}")
    print(f"YY (sending YYYY value): {from_parts['YYYY']}")

    # Call the API
    response = client._service.FetchLegMembersPerDay(
        UN=client.username,
        PSW=client.password,
        DD=from_parts['DD'],
        MM=from_parts['MM'],
        YY=from_parts['YYYY']  # Try sending 2026 to YY parameter
    )
    
    print("\n--- Raw Response Structure ---")
    print(dir(response))
    
    if hasattr(response, 'DayMember'):
        print("\n--- DayMember ---")
        day_member = response.DayMember
        print(dir(day_member))
        
        items = []
        if hasattr(day_member, 'TAIMSGetLegMembers'):
             print("\nFound TAIMSGetLegMembers inside DayMember")
             inner = day_member.TAIMSGetLegMembers
             
             # Check if this is the list or if it has another layer
             if isinstance(inner, list):
                 items = inner
             elif hasattr(inner, 'TAIMSLegMember'):
                 items = inner.TAIMSLegMember
             elif hasattr(inner, 'LegMember'):
                  items = inner.LegMember
             else:
                 print("Structure of TAIMSGetLegMembers:")
                 print(dir(inner))
                 # Try to assume it's iterable directly
                 try:
                     items = list(inner)
                 except:
                     pass
        
        print(f"\nFound {len(items)} items.")
            
        if len(items) > 0:
            first_leg = items[0]
            print("\n--- First Leg Item Fields ---")
            print(dir(first_leg))
            
            # Check for 'crte'
            if hasattr(first_leg, 'crte'):
                print(f"Found 'crte': {first_leg.crte}")
            else:
                print("'crte' NOT found.")

            # Check for FMember (capital M)
            print("\nChecking for FMember...")
            if hasattr(first_leg, 'FMember'):
                print("Found 'FMember'")
                members = first_leg.FMember
                
                member_items = []
                # Check what FMember contains
                print(f"FMember contains attrs: {dir(members)}")
                
                if hasattr(members, 'TAIMSMember'):
                    member_items = members.TAIMSMember
                elif isinstance(members, list):
                    member_items = members
                
                print(f"Found {len(member_items)} members in first leg.")
                if len(member_items) > 0:
                    first_member = member_items[0]
                    print("First member fields:")
                    print(dir(first_member))
                    
                    if hasattr(first_member, 'crte'):
                        print(f"Found 'crte' in member: {first_member.crte}")
                    else:
                        print(f"'crte' NOT found in member. Checking other fields...")
            else:
                 print("'FMember' NOT found.")


except Exception as e:
    print(f"Error: {e}")

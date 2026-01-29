from aims_soap_client import get_aims_client
from datetime import datetime
from collections import defaultdict
import json

client = get_aims_client()
target_date = datetime(2026, 1, 14)

print(f"Fetching live data for {target_date.strftime('%Y-%m-%d')}...")
result = client.fetch_leg_members_per_day(target_date)

if not result['success']:
    print(f"Error: {result['error']}")
    exit()

legs = result['legs']
print(f"Total legs: {len(legs)}")

# Group legs by CRTE
crte_groups = defaultdict(list)
for leg in legs:
    for crew in leg['crew']:
        crte = crew.get('rotation') # This is 'crte'
        if crte:
            # Check if this leg is already in the group
            if leg['flt'] not in [l['flt'] for l in crte_groups[crte]]:
                crte_groups[crte].append(leg)

print(f"Total unique CRTEs: {len(crte_groups)}")

multi_reg_count = 0
for crte, group_legs in crte_groups.items():
    regs = sorted(list(set(l['reg'] for l in group_legs if l['reg'])))
    if len(regs) > 1:
        multi_reg_count += 1
        if multi_reg_count <= 5:
            print(f"CRTE {crte}: Regs {regs}, Flights {[l['flt'] for l in group_legs]}")

print(f"Total multi-reg CRTEs: {multi_reg_count}")

# Check my "leg_crew_str" logic too
group_rotations = defaultdict(list)
for leg in legs:
    crew_ids = sorted([c['id'] for c in leg['crew'] if c.get('id')])
    crew_key = ",".join(crew_ids)
    if crew_key:
        if leg['reg'] not in group_rotations[crew_key]:
            group_rotations[crew_key].append(leg['reg'])

multi_group_count = 0
for key, regs in group_rotations.items():
    if len(regs) > 1:
        multi_group_count += 1

print(f"Total multi-reg Groups (by IDs): {multi_group_count}")

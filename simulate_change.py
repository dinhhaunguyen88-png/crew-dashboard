import os
import sys
from datetime import datetime
from supabase_client import get_client

def simulate():
    client = get_client()
    if not client:
        print("Failed to initialize Supabase client")
        return

    # Use today's date in YYYY-MM-DD format (standard for PG/Supabase)
    today = datetime.now().strftime('%Y-%m-%d')
    
    test_flight = {
        'flight_date': today,
        'flight_no': 'VIBE-CHECK-777',
        'departure': 'SGN',
        'arrival': 'HAN',
        'std': '10:00',
        'sta': '12:00',
        'atd': '10:05',
        'ata': '',
        'status': 'DELAYED (SIM)',
        'ac_reg': 'TEST-JET',
        'block_minutes': 120,
        'source': 'SIMULATION',
        'synced_at': datetime.now().isoformat()
    }

    try:
        print(f"Inserting simulation flight: {test_flight['flight_no']} for {today}...")
        result = client.table('fact_actuals').upsert(
            [test_flight],
            on_conflict='flight_date,flight_no'
        ).execute()
        
        if result.data:
            print("Successfully inserted simulation flight into Supabase!")
        else:
            print("Successfully executed upsert (no data returned)")
            
    except Exception as e:
        print(f"Error during simulation: {e}")

if __name__ == "__main__":
    simulate()

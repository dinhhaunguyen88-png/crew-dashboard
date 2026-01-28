import os
import csv
from data_processor import DataProcessor
from pathlib import Path

def test_process():
    # Initialize processor
    processor = DataProcessor()
    
    # Path to the large file
    file_path = Path("Crew schedule 01-28Feb(standby,callsick, fatigue).csv")
    
    if not file_path.exists():
        print(f"File not found at {file_path}")
        return

    # Read content as bytes
    with open(file_path, 'rb') as f:
        content_bytes = f.read()

    # Process file
    result = processor.process_crew_schedule_csv(file_content=content_bytes, file_path=file_path, sync_db=False)
    
    summary = processor.crew_schedule.get('summary', {})
    
    # Extract unique IDs from standby_records
    unique_crew_from_records = set()
    for rec in processor.standby_records:
        if rec.get('crew_id'):
            unique_crew_from_records.add(rec['crew_id'])
    
    # Let's also count all IDs in rows starting from index 4
    all_ids_in_csv = set()
    try:
        content_str = content_bytes.decode('utf-8', errors='ignore')
        rows = list(csv.reader(content_str.splitlines()))
        # Find ID column in Row 4 (index 3)
        if len(rows) > 3:
            id_idx = -1
            for idx, col in enumerate(rows[3]):
                if col.strip().upper() == 'ID':
                    id_idx = idx
                    break
            
            if id_idx != -1:
                for row in rows[4:]: # Data starts row 5
                    if len(row) > id_idx:
                        cid = row[id_idx].strip()
                        if cid and cid[0].isdigit():
                            all_ids_ids_in_csv = all_ids_in_csv.add(cid)
    except Exception as e:
        print(f"Error manual counting: {e}")

    print("-" * 30)
    print(f"TOTAL CREW MEMBERS (ALL): {len(all_ids_in_csv)}")
    print(f"TOTAL CREW WITH DUTIES: {len(unique_crew_from_records)}")
    print(f"TOTAL SBY FOUND: {summary.get('SBY', 0)}")
    print(f"TOTAL OSBY FOUND: {summary.get('OSBY', 0)}")
    print(f"TOTAL CS (Sick) FOUND: {summary.get('CSL', 0)}")
    print(f"TOTAL FGT (Fatigue) FOUND: {summary.get('FGT', 0)}")
    print(f"TOTAL OFF FOUND: {summary.get('OFF', 0)}")
    print(f"TOTAL NO_DUTY: {summary.get('NO_DUTY', 0)}")
    print("-" * 30)

if __name__ == "__main__":
    test_process()

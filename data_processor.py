"""
Data Processor Module for Crew Dashboard
Handles CSV parsing and KPI calculations
"""

import csv
import re
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
import supabase_client as db

class DataProcessor:
    def __init__(self, data_dir=None):
        self.data_dir = Path(data_dir) if data_dir else Path(".")
        self.flights = []
        self.flights_by_date = defaultdict(list)  # Store flights grouped by date
        self.available_dates = []  # List of available dates
        self.current_filter_date = None  # Current date filter (None = all dates)
        self.crew_to_regs = defaultdict(set)
        self.crew_to_regs_by_date = defaultdict(lambda: defaultdict(set))  # Crew regs by date
        self.crew_roles = {}
        self.reg_flight_hours = defaultdict(float)
        self.reg_flight_hours_by_date = defaultdict(lambda: defaultdict(float))  # By date
        self.reg_flight_count = defaultdict(int)
        self.reg_flight_count_by_date = defaultdict(lambda: defaultdict(int))  # By date
        self.ac_utilization = {}
        self.ac_utilization_by_date = defaultdict(dict)  # date -> {ac_type -> stats}
        # New data structures for Rolling hours and Crew schedule
        self.rolling_hours = []  # Rolling 28-day/365-day block hours
        self.crew_schedule = {   # Standby, sick-call, fatigue status
            'standby': [],
            'sick_call': [],
            'fatigue': [],
            'office_standby': [],
            'summary': {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
        }
        self.crew_schedule_by_date = defaultdict(lambda: {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0})
        # Individual standby records for date filtering
        self.standby_records = []  # List of {crew_id, crew_name, base, ac_type, position, duty_type, duty_date}
        # Crew group rotations tracking
        self.crew_group_rotations = defaultdict(list)  # crew_set -> list of REGs
        self.crew_group_rotations_by_date = defaultdict(lambda: defaultdict(list))  # date -> crew_set_key -> list of REGs
        
        # AIMS specific data
        self.aims_flights = []
        self.aims_flights_by_date = defaultdict(list)
        self.aims_available_dates = []
        
        # Upload date context for dynamic date filtering
        self.upload_date_context = {'min_date': None, 'max_date': None, 'default_date': None}
        
        # Existing flight keys for incremental updates
        self._existing_flight_keys = set()
        
        # Try to load from Supabase first
        if db.is_connected():
            print("Connected to Supabase. Loading data...")
            self.load_from_supabase()
        else:
            print("Supabase not connected. Using local/empty state.")

    def load_from_supabase(self):
        """Load all data from Supabase"""
        # 1. Flights
        db_flights = db.get_flights()
        if db_flights:
            self.flights = db_flights
            self.flights_by_date = defaultdict(list)
            self.available_dates = []
            self.crew_to_regs = defaultdict(set)
            self.crew_to_regs_by_date = defaultdict(lambda: defaultdict(set))
            self.reg_flight_hours = defaultdict(float)
            self.reg_flight_hours_by_date = defaultdict(lambda: defaultdict(float))
            self.reg_flight_count = defaultdict(int)
            self.reg_flight_count_by_date = defaultdict(lambda: defaultdict(int))
            self.crew_group_rotations = defaultdict(list)
            self.crew_group_rotations_by_date = defaultdict(lambda: defaultdict(list))
            
            unique_dates = set()
            
            for flight in self.flights:
                 # Reconstruct internal structures from flight objects
                 op_date = flight.get('date')
                 if op_date:
                     unique_dates.add(op_date)
                     self.flights_by_date[op_date].append(flight)
                     
                     reg = flight.get('reg', '')
                     if reg:
                         # Recalculate duration
                         std = self.parse_time(flight.get('std', ''))
                         sta = self.parse_time(flight.get('sta', ''))
                         if std is not None and sta is not None:
                             duration = sta - std
                             if duration < 0: duration += 24 * 60
                             hours = duration / 60
                             self.reg_flight_hours[reg] += hours
                             self.reg_flight_count[reg] += 1
                             self.reg_flight_hours_by_date[op_date][reg] += hours
                             self.reg_flight_count_by_date[op_date][reg] += 1
                     
                     crew_str = flight.get('crew', '')
                     if crew_str:
                         crew_list = self.extract_crew_ids(crew_str)
                         for role, crew_id in crew_list:
                             self.crew_to_regs[crew_id].add(reg)
                             self.crew_to_regs_by_date[op_date][crew_id].add(reg)
                             self.crew_roles[crew_id] = role
                         
                         if crew_list:
                             key = self.get_crew_set_key(crew_str)
                             if key:
                                 self.crew_group_rotations[key].append(reg)
                                 self.crew_group_rotations_by_date[op_date][key].append(reg)
            
            self.available_dates = sorted(list(unique_dates), key=lambda d: self._parse_date_for_sort(d))
            print(f"Loaded {len(self.flights)} flights from Supabase")
        
        # 6. AIMS Fact Actuals
        db_aims = db.get_fact_actuals()
        if db_aims:
            self.aims_flights = db_aims
            # Normalize and prepare AIMS flights
            for flight in self.aims_flights:
                f_date = flight.get('flight_date')
                if f_date:
                    # FIX: Explicitly handle YYYY-MM-DD format from AIMS
                    if '-' in f_date and len(f_date) == 10:
                        try:
                            parts = f_date.split('-')
                            # YYYY-MM-DD -> DD/MM/YY
                            norm_date = f"{parts[2]}/{parts[1]}/{parts[0][2:]}"
                        except:
                            norm_date = self.normalize_date(f_date)
                    else:
                        norm_date = self.normalize_date(f_date.replace('-', '/'))
                        
                    flight['date'] = norm_date
                    flight['reg'] = flight.get('ac_reg')
                    flight['flt'] = flight.get('flight_no')
                    flight['dep'] = flight.get('departure')
                    flight['arr'] = flight.get('arrival')
            
            # Pre-calculate maps for AIMS
            self.aims_flights_by_date, self.aims_available_dates, self.aims_reg_flight_hours, \
            self.aims_reg_flight_count, self.aims_crew_to_regs, self.aims_crew_to_regs_by_date, \
            self.aims_reg_flight_hours_by_date, self.aims_reg_flight_count_by_date, \
            self.aims_crew_group_rotations, self.aims_crew_group_rotations_by_date = \
                self._calculate_kpi_maps(self.aims_flights)
            
            print(f"DEBUG: Loaded {len(self.aims_flights)} AIMS flights from Supabase. Available dates: {len(self.aims_available_dates)}")
        else:
            print("DEBUG: db.get_fact_actuals() returned empty/None.")

        # 2. AC Utilization
        db_util = db.get_ac_utilization()
        if db_util:
            self.ac_utilization = {}
            self.ac_utilization_by_date = defaultdict(dict)
            for item in db_util:
                date_str = item.get('date')
                ac_type = item.get('ac_type') # Note: db might use different keys if I inserted differently, but let's assume consistent
                # Actually get_ac_utilization returns flat list. 
                # Need to check how I insert it. I insert flat list.
                # So here I reconstruct the nested dict.
                if date_str and ac_type:
                     self.ac_utilization_by_date[date_str][ac_type] = {
                         'dom_block': item.get('dom_block'),
                         'int_block': item.get('int_block'),
                         'total_block': item.get('total_block'),
                         'dom_cycles': str(item.get('dom_cycles')),
                         'int_cycles': str(item.get('int_cycles')),
                         'total_cycles': str(item.get('total_cycles')),
                         'avg_util': item.get('avg_util')
                     }
            # Re-summarize totals? Or just trust what's there?
            # Creating self.ac_utilization (total) from daily? 
            # Or is 'Total' stored as a separate date? usually not.
            # I will trust the getter logic later if needed.
            print(f"Loaded AC Util for {len(self.ac_utilization_by_date)} dates")

        # 3. Rolling Hours
        # Changed: Safety & Compliance should default to 0 on startup/refresh
        # Logic: Only show data when a file is uploaded in the current session
        # db_rolling = db.get_rolling_hours()
        # if db_rolling:
        #     self.rolling_hours = db_rolling
        #     print(f"Loaded {len(self.rolling_hours)} rolling hour records")
        pass

        # 4. Crew Schedule
        db_schedule = db.get_crew_schedule()
        if db_schedule:
             self.crew_schedule_by_date = defaultdict(lambda: {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0})
             self.crew_schedule['summary'] = {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
             for item in db_schedule:
                 d = item.get('date')
                 s = item.get('status_type')
                 if d and s:
                     self.crew_schedule_by_date[d][s] += 1
                 if s:
                     self.crew_schedule['summary'][s] += 1
             print(f"Loaded Crew Schedule from Supabase")
        
        # 5. Standby Records (new table with individual crew data)
        db_standby = db.get_standby_records()
        if db_standby:
            self.standby_records = db_standby
            print(f"Loaded {len(self.standby_records)} standby records from Supabase")
        else:
            # FALLBACK: If standby_records table doesn't exist or is empty,
            # load from local CSV to populate standby_records in memory
            print("Standby records table empty/missing. Loading from local CSV...")
            self.process_crew_schedule_csv(sync_db=False)  # Don't sync back to DB
            print(f"Loaded {len(self.standby_records)} standby records from local CSV")
        
        # 6. AIMS Fact Actuals
        db_aims = db.get_fact_actuals()
        if db_aims:
            self.aims_flights = db_aims
            self.aims_flights_by_date = defaultdict(list)
            aims_unique_dates = set()
            for flight in self.aims_flights:
                # Map AIMS schema to internal schema
                # Internal schema: date, reg, flt, dep, arr, std, sta, crew
                # AIMS schema: flight_date, ac_reg, flight_no, departure, arrival, std, sta, status, ...
                f_date = flight.get('flight_date')
                if f_date:
                    # Convert YYYY-MM-DD to DD/MM/YY for consistency in UI if needed
                    # Actually let's keep it as is or normalize
                    norm_date = self.normalize_date(f_date.replace('-', '/'))
                    flight['date'] = norm_date
                    flight['reg'] = flight.get('ac_reg')
                    flight['flt'] = flight.get('flight_no')
                    flight['dep'] = flight.get('departure')
                    flight['arr'] = flight.get('arrival')
                    
                    self.aims_flights_by_date[norm_date].append(flight)
                    aims_unique_dates.add(norm_date)
            
            self.aims_available_dates = sorted(list(aims_unique_dates), key=lambda d: self._parse_date_for_sort(d))
            print(f"Loaded {len(self.aims_flights)} AIMS flights from Supabase")
        
    def _read_file_safe(self, file_path):
        """Read file with encoding fallback (utf-8 -> cp1252 -> latin1)"""
        if not file_path or not file_path.exists():
            return None
            
        encodings = ['utf-8', 'cp1252', 'latin1']
        for enc in encodings:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                return None
        return None

    def _decode_content_safe(self, content_bytes):
        """Decode bytes with fallback (utf-8 -> cp1252 -> latin1)"""
        if not content_bytes:
            return ""
            
        encodings = ['utf-8', 'cp1252', 'latin1']
        for enc in encodings:
            try:
                return content_bytes.decode(enc)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                print(f"Error decoding raw content: {e}")
                return ""
        return ""
        
    def parse_time(self, time_str):
        """Parse time string HH:MM to minutes from midnight"""
        if not time_str or ':' not in time_str:
            return None
        try:
            parts = time_str.split(':')
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, TypeError, IndexError):
            return None
    
    def get_operating_date(self, calendar_date, time_str):
        """
        Determine operating date based on flight departure time.
        Operating day: 04:00 to 03:59 next day
        - Flights departing 04:00-23:59 belong to that calendar date
        - Flights departing 00:00-03:59 belong to previous calendar date
        """
        if not time_str:
            return calendar_date
        
        time_minutes = self.parse_time(time_str)
        if time_minutes is None:
            return calendar_date
        
        # If departure time is 00:00-03:59 (0-239 minutes), it belongs to previous day
        if time_minutes < 240:  # 04:00 = 240 minutes
            # Adjust to previous day
            try:
                parts = calendar_date.split('/')
                day = int(parts[0])
                month = int(parts[1])
                year = int(parts[2]) + 2000 if int(parts[2]) < 100 else int(parts[2])
                
                from datetime import date, timedelta
                current_date = date(year, month, day)
                prev_date = current_date - timedelta(days=1)
                
                return f"{prev_date.day:02d}/{prev_date.month:02d}/{str(prev_date.year)[-2:]}"
            except (ValueError, TypeError, IndexError):
                return calendar_date
        
        return calendar_date
    
    def extract_crew_ids(self, crew_string):
        """Extract crew IDs from crew string like '-NAME(ROLE) ID'"""
        pattern = r'\(([A-Z]{2})\)\s*(\d+)'
        matches = re.findall(pattern, crew_string)
        return [(role, id) for role, id in matches]
    
    def get_crew_set_key(self, crew_string):
        """Get a unique key for a crew set (sorted crew IDs)"""
        crew_list = self.extract_crew_ids(crew_string)
        crew_ids = sorted([cid for _, cid in crew_list])
        return tuple(crew_ids)
    
    def normalize_date(self, date_str):
        """Normalize date string to DD/MM/YY format (force 2-digit year)"""
        if not date_str:
            return None
        # Remove leading/trailing spaces
        date_str = date_str.strip()
        # Handle format like "15/01/26" or "15/01/2026"
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) >= 2:
                day = parts[0].zfill(2)
                month = parts[1].zfill(2)
                if len(parts) > 2:
                    # Take last 2 digits of year
                    year = parts[2][-2:]
                else:
                    year = '26'
                return f"{day}/{month}/{year}"
        return date_str
    
    def detect_csv_format(self, header_row):
        """Detect CSV format based on header row and return column indices"""
        # Default column mapping for format: DATE,REG,FLT,DEP,ARR,STD,STA,...,Crew #,Crew
        col_map = {
            'date': 0,
            'reg': 1,
            'flt': 2,
            'dep': 3,
            'arr': 4,
            'std': 5,
            'sta': 6,
            'crew': 14
        }
        
        # Check header to detect format
        header_lower = [h.lower().strip() for h in header_row]
        
        # Try to find column indices from header
        for i, h in enumerate(header_lower):
            if h == 'date':
                col_map['date'] = i
            elif h == 'reg':
                col_map['reg'] = i
            elif h == 'flt':
                col_map['flt'] = i
            elif h == 'dep':
                col_map['dep'] = i
            elif h == 'arr':
                col_map['arr'] = i
            elif h == 'std':
                col_map['std'] = i
            elif h == 'sta':
                col_map['sta'] = i
            elif h == 'crew':
                col_map['crew'] = i
        
        # Check if crew column exists
        col_map['has_crew'] = 'crew' in header_lower or len(header_row) > 14
        
        return col_map
    
    def _infer_ac_type(self, reg):
        """Infer aircraft type from registration code"""
        if not reg:
            return 'A320'
        reg_upper = reg.upper()
        if 'A6' in reg_upper or 'A321' in reg_upper or '321' in reg_upper:
            return 'A321'
        elif 'A33' in reg_upper or 'A330' in reg_upper or '330' in reg_upper:
            return 'A330'
        elif '32W' in reg_upper or 'C90W' in reg_upper:
            return 'A320neo'
        return 'A320'
    
    def _get_flight_key(self, flight):
        """Generate unique key for a flight record (for incremental updates)"""
        return f"{flight.get('date', '')}_{flight.get('flt', '')}_{flight.get('reg', '')}_{flight.get('std', '')}"
    
    def process_dayrep_csv(self, file_path=None, file_content=None, sync_db=True):
        """Process DayRepReport CSV file with operating day logic (04:00-03:59)
        Supports multiple CSV formats with auto-detection based on header"""
        self.flights = []
        self.flights_by_date = defaultdict(list)
        self.available_dates = []
        self.crew_to_regs = defaultdict(set)
        self.crew_to_regs_by_date = defaultdict(lambda: defaultdict(set))
        self.crew_roles = {}
        self.reg_flight_hours = defaultdict(float)
        self.reg_flight_hours_by_date = defaultdict(lambda: defaultdict(float))
        self.reg_flight_count = defaultdict(int)
        self.reg_flight_count_by_date = defaultdict(lambda: defaultdict(int))
        
        # New: Track crew rotations at group level
        self.crew_group_rotations = defaultdict(list)  # crew_set -> list of REGs
        self.crew_group_rotations_by_date = defaultdict(lambda: defaultdict(list))
        
        unique_dates = set()
        
        rows = []
        if file_content:
            lines = self._decode_content_safe(file_content).split('\n')
            rows = list(csv.reader(lines))
        else:
            # Check if any user uploads exist (User Mode vs Demo Mode)
            uploads_dir = self.data_dir / 'uploads'
            has_uploads = any(uploads_dir.glob('*.csv')) if uploads_dir.exists() else False
            
            uploaded_path = uploads_dir / 'DayRepReport.csv'
            
            if uploaded_path.exists():
                file_path = uploaded_path
            elif has_uploads:
                # User Mode active but this file missing -> Empty data
                return 0
            else:
                # Demo Mode
                file_path = self.data_dir / 'DayRepReport15Jan2026.csv'
                
            if file_path and file_path.exists():
                with open(file_path, 'r', encoding='utf-8') as f:
                    rows = list(csv.reader(f))
        
        # Auto-detect format from header row (usually row 2 or 3)
        col_map = None
        header_row_idx = None
        for i, row in enumerate(rows[:5]):  # Check first 5 rows for header
            if len(row) >= 6:
                row_lower = [c.lower().strip() for c in row]
                if 'date' in row_lower and ('reg' in row_lower or 'flt' in row_lower):
                    col_map = self.detect_csv_format(row)
                    header_row_idx = i
                    break
        
        # If no header found, use default mapping based on content detection
        if col_map is None:
            col_map = {
                'date': 0, 'reg': 1, 'flt': 2, 'dep': 3, 'arr': 4,
                'std': 5, 'sta': 6, 'crew': 14, 'has_crew': True
            }
            header_row_idx = -1
        
        # Process data rows
        for row in rows[header_row_idx + 1:]:
            # Need at least the basic columns to process
            min_cols = max(col_map['date'], col_map['reg'], col_map['flt'], 
                          col_map['dep'], col_map['arr'], col_map['std'], col_map['sta']) + 1
            
            if len(row) >= min_cols and row[col_map['date']]:
                date_str = row[col_map['date']].strip()
                # Check if first column looks like a date (contains / and digits)
                if '/' in date_str and any(c.isdigit() for c in date_str):
                    reg = row[col_map['reg']].strip() if col_map['reg'] < len(row) else ''
                    
                    # Skip rows without REG (some dates may not have aircraft assigned yet)
                    if not reg:
                        continue
                    
                    calendar_date = self.normalize_date(date_str)
                    std_time = row[col_map['std']].strip() if col_map['std'] < len(row) and row[col_map['std']] else ''
                    sta_time = row[col_map['sta']].strip() if col_map['sta'] < len(row) and row[col_map['sta']] else ''
                    
                    # Apply operating day logic (04:00-03:59)
                    operating_date = self.get_operating_date(calendar_date, std_time)
                    unique_dates.add(operating_date)
                    
                    # Get crew string if available
                    crew_string = ''
                    if col_map['has_crew'] and col_map['crew'] < len(row):
                        crew_string = row[col_map['crew']]
                    
                    flight = {
                        'date': operating_date,
                        'calendar_date': calendar_date,
                        'reg': reg,
                        'ac_type': self._infer_ac_type(reg),  # A/C Type mapping from REG
                        'flt': row[col_map['flt']].strip() if col_map['flt'] < len(row) else '',
                        'dep': row[col_map['dep']].strip() if col_map['dep'] < len(row) else '',
                        'arr': row[col_map['arr']].strip() if col_map['arr'] < len(row) else '',
                        'std': std_time,
                        'sta': sta_time,
                        'crew': crew_string
                    }
                    self.flights.append(flight)
                    self.flights_by_date[operating_date].append(flight)
                    
                    # Calculate flight hours (both total and by date)
                    std = self.parse_time(std_time)
                    sta = self.parse_time(sta_time)
                    if std is not None and sta is not None:
                        duration = sta - std
                        if duration < 0:
                            duration += 24 * 60
                        hours = duration / 60
                        self.reg_flight_hours[reg] += hours
                        self.reg_flight_count[reg] += 1
                        self.reg_flight_hours_by_date[operating_date][reg] += hours
                        self.reg_flight_count_by_date[operating_date][reg] += 1
                    
                    # Extract crew (both total and by date) - only if crew data exists
                    if crew_string:
                        crew_list = self.extract_crew_ids(crew_string)
                        for role, crew_id in crew_list:
                            self.crew_to_regs[crew_id].add(reg)
                            self.crew_to_regs_by_date[operating_date][crew_id].add(reg)
                            self.crew_roles[crew_id] = role
                        
                        # Track crew group rotations
                        if crew_list:
                            crew_set_key = self.get_crew_set_key(crew_string)
                            if crew_set_key:
                                self.crew_group_rotations[crew_set_key].append(reg)
                                self.crew_group_rotations_by_date[operating_date][crew_set_key].append(reg)
        
        # Sort dates chronologically
        self.available_dates = sorted(list(unique_dates), key=lambda d: self._parse_date_for_sort(d))
        
        # Update upload_date_context with the date range from this upload
        if self.available_dates:
            self.upload_date_context = {
                'min_date': self.available_dates[0],
                'max_date': self.available_dates[-1],
                'default_date': self.available_dates[-1]  # Default to most recent date
            }
            print(f"Upload date context: {self.upload_date_context['min_date']} to {self.upload_date_context['max_date']}")
        
        # Track existing flight keys for incremental updates
        self._existing_flight_keys = set(self._get_flight_key(f) for f in self.flights)
        
        # INSERT TO SUPABASE
        if sync_db and db.is_connected() and len(self.flights) > 0:
            print("syncing flights to supabase...")
            flights_payload = []
            for flight in self.flights:
                flights_payload.append({
                    'date': flight.get('date', ''),
                    'calendar_date': flight.get('calendar_date', ''),
                    'reg': flight.get('reg', ''),
                    'ac_type': flight.get('ac_type', 'A320'),  # Include A/C Type
                    'flt': flight.get('flt', ''),
                    'dep': flight.get('dep', ''),
                    'arr': flight.get('arr', ''),
                    'std': flight.get('std', ''),
                    'sta': flight.get('sta', ''),
                    'crew': flight.get('crew', '')
                })
            db.insert_flights(flights_payload)
        
        return len(self.flights)
    
    def _parse_date_for_sort(self, date_str):
        """Parse date string for sorting purposes"""
        try:
            parts = date_str.split('/')
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2]) + 2000 if int(parts[2]) < 100 else int(parts[2])
            return (year, month, day)
        except (ValueError, TypeError, IndexError, AttributeError):
            return (9999, 99, 99)
    
    def process_sacutil_csv(self, file_path=None, file_content=None, sync_db=True):
        """Process SacutilReport CSV file"""
        self.ac_utilization = {}
        self.ac_utilization_by_date.clear()
        
        if file_content:
            content = self._decode_content_safe(file_content)
        else:
            # check uploads
            uploads_dir = self.data_dir / 'uploads'
            has_uploads = any(uploads_dir.glob('*.csv')) if uploads_dir.exists() else False
            
            uploaded_path = uploads_dir / 'SacutilReport.csv'
            
            if uploaded_path.exists():
                file_path = uploaded_path
            elif has_uploads:
                return {}
            else:
                file_path = self.data_dir / 'SacutilReport1.csv'
            
            if file_path and file_path.exists():
                content = self._read_file_safe(file_path)
                if content is None:
                    return {}
            else:
                return {}
        
        # Parse CSV properly
        rows = list(csv.reader(content.splitlines()))
        
        # Helper functions
        def parse_time_to_min(time_str):
            try:
                if ':' in time_str:
                    h, m = time_str.split(':')
                    return int(h) * 60 + int(m)
                return 0
            except (ValueError, TypeError, AttributeError):
                return 0
        
        def parse_int(val):
            try:
                return int(val)
            except (ValueError, TypeError):
                return 0
        
        def min_to_time(minutes):
            return f"{minutes // 60:02d}:{minutes % 60:02d}"
        
        # Try to detect year from file content (e.g., "20/01/2026-31/01/2026")
        report_year = 2026  # Default
        for row in rows[:5]:
            row_str = ','.join(row)
            import re
            year_match = re.search(r'20(\d{2})', row_str)
            if year_match:
                report_year = 2000 + int(year_match.group(1))
                break
        
        # Aggregate data by date and by aircraft type
        # Structure: ac_stats_by_date[date_str][ac_type] = {stats}
        ac_stats_by_date = defaultdict(lambda: defaultdict(lambda: {
            'dom_block_min': 0, 'int_block_min': 0, 'total_block_min': 0,
            'dom_cycles': 0, 'int_cycles': 0, 'total_cycles': 0,
            'count': 0, 'last_avg_util': ''
        }))
        
        # Also keep totals for "All Dates"
        ac_stats_total = defaultdict(lambda: {
            'dom_block_min': 0, 'int_block_min': 0, 'total_block_min': 0,
            'dom_cycles': 0, 'int_cycles': 0, 'total_cycles': 0,
            'count': 0, 'last_avg_util': ''
        })
        
        for row in rows:
            if len(row) < 8:
                continue
            
            first_col = row[0].strip()
            # Skip header rows, totals, and non-data rows
            if not first_col or 'Totals' in first_col or 'Period' in first_col or 'Generated' in first_col:
                continue
            
            # Check if first column is a date (DD.MM or DD/MM format)
            if not ('.' in first_col or '/' in first_col):
                continue
            if not any(c.isdigit() for c in first_col):
                continue
            
            # Parse date from first column (formats: DD.MM or DD/MM)
            try:
                if '.' in first_col:
                    day, month = first_col.split('.')
                else:
                    day, month = first_col.split('/')
                day = int(day)
                month = int(month)
                # Construct date string in DD/MM/YY format (same as DayRep/CrewSchedule)
                date_str = f"{day:02d}/{month:02d}/{str(report_year)[-2:]}"
            except (ValueError, TypeError, IndexError):
                continue
                
            # Get AC type - handle formats like "320", "321", "330", "A320", etc.
            ac_type = row[1].strip()
            if not ac_type or ac_type in ['AC', 'ACTYPE', 'Aircraft', 'Date']:
                continue
            
            # Normalize AC type (strip leading 'A' if present for consistency)
            if ac_type.startswith('A') and len(ac_type) > 1 and ac_type[1:].isdigit():
                ac_type = ac_type[1:]
            
            dom_block = parse_time_to_min(row[2].strip()) if len(row) > 2 else 0
            int_block = parse_time_to_min(row[3].strip()) if len(row) > 3 else 0
            total_block = parse_time_to_min(row[4].strip()) if len(row) > 4 else 0
            
            dom_cycles = parse_int(row[5].strip()) if len(row) > 5 else 0
            int_cycles = parse_int(row[6].strip()) if len(row) > 6 else 0
            total_cycles = parse_int(row[7].strip()) if len(row) > 7 else 0
            
            # Get avg util from last column (index 11 in new format)
            avg_util = row[11].strip() if len(row) > 11 else ''
            
            # Aggregate by date and AC type
            stats = ac_stats_by_date[date_str][ac_type]
            stats['dom_block_min'] += dom_block
            stats['int_block_min'] += int_block
            stats['total_block_min'] += total_block
            stats['dom_cycles'] += dom_cycles
            stats['int_cycles'] += int_cycles
            stats['total_cycles'] += total_cycles
            stats['count'] += 1
            stats['last_avg_util'] = avg_util
            
            # Also aggregate totals
            total_stats = ac_stats_total[ac_type]
            total_stats['dom_block_min'] += dom_block
            total_stats['int_block_min'] += int_block
            total_stats['total_block_min'] += total_block
            total_stats['dom_cycles'] += dom_cycles
            total_stats['int_cycles'] += int_cycles
            total_stats['total_cycles'] += total_cycles
            total_stats['count'] += 1
            total_stats['last_avg_util'] = avg_util
        
        # Convert to display format and store by date
        for date_str, ac_types in ac_stats_by_date.items():
            self.ac_utilization_by_date[date_str] = {}
            for ac_type, stats in ac_types.items():
                self.ac_utilization_by_date[date_str][ac_type] = {
                    'dom_block': min_to_time(stats['dom_block_min']),
                    'int_block': min_to_time(stats['int_block_min']),
                    'total_block': min_to_time(stats['total_block_min']),
                    'dom_cycles': str(stats['dom_cycles']),
                    'int_cycles': str(stats['int_cycles']),
                    'total_cycles': str(stats['total_cycles']),
                    'avg_util': stats['last_avg_util']
                }
        
        # Store totals (for "All Dates" view)
        for ac_type, stats in ac_stats_total.items():
            self.ac_utilization[ac_type] = {
                'dom_block': min_to_time(stats['dom_block_min']),
                'int_block': min_to_time(stats['int_block_min']),
                'total_block': min_to_time(stats['total_block_min']),
                'dom_cycles': str(stats['dom_cycles']),
                'int_cycles': str(stats['int_cycles']),
                'total_cycles': str(stats['total_cycles']),
                'avg_util': stats['last_avg_util']
            }
        
        # INSERT TO SUPABASE
        if sync_db and db.is_connected() and len(self.ac_utilization_by_date) > 0:
            print("syncing ac_utilization to supabase...")
            util_data = []
            for date_str, ac_types in self.ac_utilization_by_date.items():
                for ac_type, stats in ac_types.items():
                    util_data.append({
                        'date': date_str,
                        'ac_type': ac_type,
                        'dom_block': stats.get('dom_block', '00:00') if isinstance(stats.get('dom_block'), str) else self.min_to_time(stats.get('dom_block_min', 0)), # Handle if stats are mixed, but usually formatted strings by now
                        'int_block': stats.get('int_block', '00:00'),
                        'total_block': stats.get('total_block', '00:00'),
                        'dom_cycles': int(stats.get('dom_cycles', 0)),
                        'int_cycles': int(stats.get('int_cycles', 0)),
                        'total_cycles': int(stats.get('total_cycles', 0)),
                        'avg_util': stats.get('avg_util', '')
                    })
            # Ensure safe string formatting if not already
            # Actually process_sacutil_csv earlier converts to strings in self.ac_utilization_by_date
            # So I should just take the strings.
            # But wait, self.ac_utilization_by_date values are DICTS of strings (lines 471-479).
            # So stats['dom_block'] is "HH:MM".
            
            db.insert_ac_utilization(util_data)

        return len(self.ac_utilization)
    
    def process_rolcrtot_csv(self, file_path=None, file_content=None, sync_db=True):
        """Process RolCrTotReport CSV file - Rolling crew hours totals"""
        self.rolling_hours = []
        
        if file_content:
            content = self._decode_content_safe(file_content)
        else:
            # check uploads
            uploads_dir = self.data_dir / 'uploads'
            has_uploads = any(uploads_dir.glob('*.csv')) if uploads_dir.exists() else False
            
            uploaded_path = uploads_dir / 'RolCrTotReport.csv'
            
            if uploaded_path.exists():
                file_path = uploaded_path
            elif has_uploads:
                return 0
            else:
                file_path = self.data_dir / 'RolCrTotReport 28Feb26.csv'
            
            try:
                if file_path and file_path.exists():
                    content = self._read_file_safe(file_path)
                    if content is None: return 0
                else:
                    return 0
            except Exception:
                return 0
        
        # Read CSV with header detection
        rows = list(csv.reader(content.splitlines()))
        if not rows:
            return 0
            
        # Detect header - RolCrTotReport has multi-row header format:
        # Row 0-1: Title/Date
        # Row 2: ID, Name, Seniority, Last, Last
        # Row 3: '', '', '', 28-Day(s), 12-Month(s)
        # Row 4: '', '', '', Block Time, Block Time
        # Data starts at row 5
        
        # Find the row with 'ID' and 'Name' columns
        data_start_idx = 0
        for i, row in enumerate(rows[:10]):
            row_lower = [c.lower().strip() for c in row]
            if 'id' in row_lower and 'name' in row_lower:
                # For this specific CSV format, data starts several rows after ID/Name header
                # Skip the additional header rows (28-Day(s), Block Time)
                data_start_idx = i + 1
                # Skip any rows that don't start with a digit (more header rows)
                while data_start_idx < len(rows) and len(rows[data_start_idx]) > 0:
                    first_cell = rows[data_start_idx][0].strip()
                    if first_cell and first_cell[0].isdigit():
                        break
                    data_start_idx += 1
                break
        
        # Use fixed column mapping for RolCrTotReport format:
        # Column 0: ID, Column 1: Name, Column 2: Seniority, Column 3: 28-Day Block, Column 4: 12-Month Block
        header_map = {'id': 0, 'name': 1, 'seniority': 2, 'block_28day': 3, 'block_12month': 4}

        for row in rows[data_start_idx:]:
            if len(row) < 4: continue
            
            # Safe extraction
            try:
                crew_id = row[header_map['id']].strip()
                if not crew_id or not crew_id[0].isdigit(): continue
                
                name = row[header_map['name']].strip()
                seniority = row[header_map['seniority']].strip() if len(row) > 2 else '0'
                
                # Get block hours from fixed column positions
                b28 = row[header_map['block_28day']].strip() if len(row) > 3 else '0:00'
                b12m = row[header_map['block_12month']].strip() if len(row) > 4 else '0:00'
                
                # Parse hours from HH:MM format
                def parse_hours(time_str):
                    try:
                        if ':' in time_str:
                            h, m = time_str.split(':')
                            return float(h) + float(m) / 60
                        return 0.0
                    except (ValueError, TypeError, AttributeError):
                        return 0.0
                
                hours_28day = parse_hours(b28)
                hours_12month = parse_hours(b12m)
                
                # Determine status based on 28-day limit (100 hours)
                percentage = (hours_28day / 100) * 100
                if percentage >= 95:
                    status = 'critical'
                elif percentage >= 85:
                    status = 'warning'
                else:
                    status = 'normal'
                
                self.rolling_hours.append({
                    'id': crew_id,
                    'name': name,
                    'seniority': seniority,
                    'block_28day': b28,
                    'block_12month': b12m,
                    'hours_28day': round(hours_28day, 2),
                    'hours_12month': round(hours_12month, 2),
                    'percentage': round(percentage, 1),
                    'status': status
                })
            except Exception:
                continue
        
        # Sort by 28-day hours descending
        self.rolling_hours.sort(key=lambda x: x['hours_28day'], reverse=True)
        
        # INSERT TO SUPABASE
        if sync_db and db.is_connected() and len(self.rolling_hours) > 0:
            print("syncing rolling_hours to supabase...")
            hours_data = []
            for item in self.rolling_hours:
                hours_data.append({
                    'crew_id': item.get('id', ''),
                    'name': item.get('name', ''),
                    'seniority': item.get('seniority', ''),
                    'block_28day': item.get('block_28day', '0:00'),
                    'block_12month': item.get('block_12month', '0:00'),
                    'hours_28day': item.get('hours_28day', 0),
                    'hours_12month': item.get('hours_12month', 0),
                    'percentage': item.get('percentage', 0),
                    'status': item.get('status', 'normal')
                })
            db.insert_rolling_hours(hours_data)
            
        return len(self.rolling_hours)
    
    def process_crew_schedule_csv(self, file_path=None, file_content=None, sync_db=True):
        """Process Crew schedule CSV file - Standby, sick-call, fatigue status
        
        Now stores individual crew records with crew_id, name, base, position for
        proper date-based filtering.
        """
        self.crew_schedule = {
            'standby': [],
            'sick_call': [],
            'fatigue': [],
            'office_standby': [],
            'summary': {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0, 'FGT': 0, 'OFF': 0, 'NO_DUTY': 0}
        }
        
        # New: Store individual crew records for standby
        self.standby_records = []  # List of {crew_id, crew_name, base, ac_type, position, duty_type, duty_date}
        
        if file_content:
            content = self._decode_content_safe(file_content)
        else:
            # check uploads
            uploads_dir = self.data_dir / 'uploads'
            has_uploads = any(uploads_dir.glob('*.csv')) if uploads_dir.exists() else False
            
            uploaded_path = uploads_dir / 'CrewSchedule.csv'
            
            if uploaded_path.exists():
                file_path = uploaded_path
            elif has_uploads and (uploads_dir / 'CrewSchedule.csv').exists():
                 file_path = uploads_dir / 'CrewSchedule.csv'
            else:
                # Look for Crew Schedule files in root data dir
                # Prioritize Feb2026 specific file, otherwise find any "Crew schedule*.csv"
                feb_file = self.data_dir / 'Crew schedule Feb2026.csv'
                
                if feb_file.exists():
                    file_path = feb_file
                    print(f"DEBUG: Selected specific file: {file_path}")
                else:
                    # Find all matching files
                    candidates = list(self.data_dir.glob('Crew schedule*.csv'))
                    if candidates:
                        # Sort by modification time (newest first) or name
                        # Let's verify if we have the Jan file and maybe others
                        candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                        file_path = candidates[0]
                        print(f"DEBUG: Selected latest file: {file_path}")
                    else:
                        file_path = self.data_dir / 'Crew schedule 15Jan(standby,callsick, fatigue).csv'
            
            try:
                if file_path and file_path.exists():
                    print(f"Loading Crew Schedule from: {file_path.name}")
                    content = self._read_file_safe(file_path)
                    if content is None: return 0
                else:
                    print("No Crew Schedule file found.")
                    return 0
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                return 0
        
        # Reset data
        self.crew_schedule['summary'] = {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0, 'FGT': 0, 'OFF': 0, 'NO_DUTY': 0}
        self.crew_schedule_by_date.clear()
        self.standby_records = []
        
        # Read CSV with header detection
        rows = list(csv.reader(content.splitlines()))
        if not rows:
            return 0
            
        header_map = {}
        data_start_idx = 0
        date_cols = {}  # col_idx -> date_str (DD/MM/YY)
        report_month = datetime.now().month
        report_year = datetime.now().year
        
        # 1. Try to detect Report Month/Year from first few lines and Filename
        
        # Strategy A: Check Filename first (often more reliable for manual uploads)
        if file_path:
            filename = file_path.name
            # Pattern: "Feb2026", "Jan 2026", "02-2026", etc.
            name_match = re.search(r'([A-Za-z]{3})[-_ ]?(\d{4})', filename, re.IGNORECASE)
            if name_match:
                try:
                    d_month_str, d_year = name_match.groups()
                    d_month = datetime.strptime(d_month_str, "%b").month
                    report_month = int(d_month)
                    report_year = int(d_year)
                    print(f"DEBUG: Parsed Date from Filename ({filename}) - Month={report_month}, Year={report_year}")
                except Exception as e:
                    print(f"DEBUG: Filename parse error: {e}")

        # Strategy B: Check File Header (Period) - Overwrites filename if found valid
        found_header_date = False
        for i in range(min(10, len(rows))):
            line_str = ",".join(rows[i])
            
            # Try Pattern: "Period: DD/MM/YYYY-DD/MM/YYYY" (e.g., "01/02/2025-28/02/2025")
            # Also handles dot/dash separator: 01.02.2025 or 01-02-2025
            period_match = re.search(r'Period[:\s]+(\d{1,2})[/\.\-](\d{1,2})[/\.\-](\d{4})', line_str, re.IGNORECASE)
            if period_match:
                try:
                    d_day, d_month, d_year = period_match.groups()
                    report_month = int(d_month)
                    report_year = int(d_year)
                    found_header_date = True
                    print(f"DEBUG: Parsed Period Header - Month={report_month}, Year={report_year}")
                    break
                except (ValueError, TypeError):
                    pass
            
            # Try Pattern: "DD Mon YYYY" (e.g., "19 Jan 2026")
            if not found_header_date:
                date_match = re.search(r'(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})', line_str)
                if date_match:
                    try:
                        d_day, d_month_str, d_year = date_match.groups()
                        d_month = datetime.strptime(d_month_str, "%b").month
                        report_year = int(d_year)
                        report_month = int(d_month)
                        found_header_date = True
                        print(f"DEBUG: Parsed Date Line - Month={report_month}, Year={report_year}")
                        break
                    except (ValueError, TypeError):
                        pass


        # 2. Detect columns (Standard vs Matrix)
        is_matrix = False
        
        # Check specific row 5 (index 4) as per Professional Specification
        if len(rows) > 4:
            row_4 = [c.upper().strip() for c in rows[4]]
            if 'ID' in row_4 and ('NAME' in row_4 or 'BASE' in str(row_4)):
                is_matrix = True
                data_start_idx = 5 # Start from row 6
                print("DEBUG: Professional Specification Header detected at Row 5")
                
                # Map standard columns
                for idx, col in enumerate(row_4):
                    if col == 'ID': header_map['id'] = idx
                    elif 'NAM' in col: header_map['name'] = idx
                    elif 'BASE' in col or 'AC' in col or 'POS' in col: header_map['base_ac_pos'] = idx
                    elif 'DAY' in col: header_map['days_total'] = idx
                
                # Map date columns
                for idx, col in enumerate(rows[4]):
                    val = col.strip()
                    if val.isdigit() and 1 <= int(val) <= 31:
                        day_num = int(val)
                        
                        # Fix: If report_year is 2025 but current date is 2026, 
                        # it's likely a typo in the report header/file. Force current year.
                        actual_year = report_year
                        if report_year == 2025 and datetime.now().year == 2026:
                            actual_year = 2026
                            
                        date_str = f"{day_num:02d}/{report_month:02d}/{str(actual_year)[-2:]}"
                        date_cols[idx] = date_str
        
        # Fallback to search if Row 5 didn't match
        if not header_map:
            for i, row in enumerate(rows[:10]):
                row_upper = [c.upper().strip() for c in row]
                
                # Check for Matrix headers (ID and Day Numbers like '20', '21')
                day_cols = [idx for idx, c in enumerate(row) if c.strip().isdigit() and 1 <= int(c.strip()) <= 31]
                
                if 'ID' in row_upper and len(day_cols) > 3:
                    is_matrix = True
                    data_start_idx = i + 1
                    
                    # Map standard columns
                    for idx, col in enumerate(row_upper):
                        if col == 'ID': header_map['id'] = idx
                        elif 'NAM' in col: header_map['name'] = idx
                        elif 'BASE' in col or 'AC' in col or 'POS' in col: header_map['base_ac_pos'] = idx
                    
                    # Map date columns
                    for idx in day_cols:
                        day_num = int(row[idx].strip())
                        
                        actual_year = report_year
                        if report_year == 2025 and datetime.now().year == 2026:
                            actual_year = 2026
                            
                        date_str = f"{day_num:02d}/{report_month:02d}/{str(actual_year)[-2:]}"
                        date_cols[idx] = date_str
                    break
                    
                # Check for Standard headers (with SL, SBY columns)
                elif 'ID' in row_upper and ('SL' in row_upper or 'SBY' in row_upper or 'FDUT' in row_upper or 'CREW' in str(row_upper)):
                    for idx, col in enumerate(row_upper):
                        if col == 'ID': header_map['id'] = idx
                        elif 'NAM' in col: header_map['name'] = idx
                        elif 'BASE' in col or 'AC' in col: header_map['base_ac_pos'] = idx
                        elif col == 'SL': header_map['sl'] = idx
                        elif col == 'CSL': header_map['csl'] = idx
                        elif col == 'SBY': header_map['sby'] = idx
                        elif col == 'OSBY': header_map['osby'] = idx
                    data_start_idx = i + 1
                    break
        
        # Default mapping fallback (Standard)
        if not header_map and not is_matrix:
             header_map = {'id': 1, 'name': 2, 'base_ac_pos': 3, 'sl': 5, 'csl': 6, 'sby': 7, 'osby': 8}
             for i, row in enumerate(rows):
                 if len(row) > 1 and row[1].strip().isdigit():
                     data_start_idx = i
                     break
        
        # Helper to parse Base/AC/Pos string like "SGN 320 CP"
        def parse_base_ac_pos(val):
            parts = val.strip().split()
            base = parts[0] if len(parts) > 0 else ''
            ac_type = parts[1] if len(parts) > 1 else ''
            position = parts[2] if len(parts) > 2 else ''
            return base, ac_type, position
        
        # Determine default date for Standard format (use report date from filename or today)
        default_date = f"{15:02d}/{report_month:02d}/{str(report_year)[-2:]}"
        
        # Process Rows
        for row in rows[data_start_idx:]:
            if len(row) < 2: continue
            
            # Skip totals/empty key rows
            if 'id' in header_map and header_map['id'] < len(row):
                 crew_id = row[header_map['id']].strip()
                 if not crew_id or not crew_id[0].isdigit(): continue
            else:
                 continue
            
            # Get crew info
            crew_name = row[header_map.get('name', 2)].strip() if header_map.get('name', 2) < len(row) else ''
            base_ac_pos = row[header_map.get('base_ac_pos', 3)].strip() if header_map.get('base_ac_pos', 3) < len(row) else ''
            base, ac_type, position = parse_base_ac_pos(base_ac_pos)

            if is_matrix:
                # MATRIX MODE - iterate over date columns
                row_has_duty = False
                try:
                     for col_idx, date_str in date_cols.items():
                         if col_idx < len(row):
                             val = row[col_idx].strip().upper()
                             if not val: continue
                             
                             duty_type = None
                             if 'SBY' in val and 'OSBY' not in val:
                                 duty_type = 'SBY'
                             elif 'OSBY' in val:
                                 duty_type = 'OSBY'
                             elif 'CS' in val: # Handles CS and CSL
                                 duty_type = 'CSL'
                             elif 'SL' in val:
                                 duty_type = 'SL'
                             elif 'FGT' in val:
                                 duty_type = 'FGT'
                             elif 'OFF' in val:
                                 duty_type = 'OFF'
                             
                             if duty_type:
                                 row_has_duty = True
                                 self.crew_schedule_by_date[date_str][duty_type] = self.crew_schedule_by_date[date_str].get(duty_type, 0) + 1
                                 self.crew_schedule['summary'][duty_type] += 1
                                 
                                 # Store individual record
                                 self.standby_records.append({
                                     'crew_id': crew_id,
                                     'crew_name': crew_name,
                                     'base': base,
                                     'ac_type': ac_type,
                                     'position': position,
                                     'duty_type': duty_type,
                                     'duty_date': date_str,
                                     'long_format': True # To confirm it follows normalization
                                 })
                     if not row_has_duty:
                        self.crew_schedule['summary']['NO_DUTY'] += 1
                        # Mark specifically as NO_DUTY for metadata if needed
                except (KeyError, IndexError, ValueError, TypeError) as e:
                    print(f"DEBUG: Matrix row parse error: {e}")
                    continue
            else:
                # STANDARD LIST MODE - create records for each duty type marked
                try:
                    def get_value(key):
                        if key in header_map and header_map[key] < len(row):
                            val = row[header_map[key]].strip()
                            if val.isdigit(): return int(val)
                        return 0

                    sl_val = get_value('sl')
                    csl_val = get_value('csl')
                    sby_val = get_value('sby')
                    osby_val = get_value('osby')

                    # Process each status type
                    for status_type, count in [('SL', sl_val), ('CSL', csl_val), ('SBY', sby_val), ('OSBY', osby_val)]:
                        if count > 0:
                            self.crew_schedule['summary'][status_type] += count
                            self.crew_schedule_by_date[default_date][status_type] += count
                            
                            # Store individual record (count times)
                            for _ in range(count):
                                self.standby_records.append({
                                    'crew_id': crew_id,
                                    'crew_name': crew_name,
                                    'base': base,
                                    'ac_type': ac_type,
                                    'position': position,
                                    'duty_type': status_type,
                                    'duty_date': default_date
                                })
                except Exception:
                    continue

        # INSERT TO SUPABASE (both legacy crew_schedule and new standby_records)
        if sync_db and db.is_connected():
            print("syncing crew_schedule to supabase...")
            
            # Legacy crew_schedule table
            schedule_data = []
            for date_str, counts in self.crew_schedule_by_date.items():
                for status_type in ['SL', 'CSL', 'SBY', 'OSBY']:
                    count = counts.get(status_type, 0)
                    for _ in range(count):
                        schedule_data.append({
                            'date': date_str,
                            'status_type': status_type
                        })
            
            if schedule_data:
                db.insert_crew_schedule(schedule_data)
            
            # New standby_records table
            if self.standby_records:
                print(f"syncing {len(self.standby_records)} standby_records to supabase...")
                db.upsert_standby_records(self.standby_records)

        # After processing, update the global upload_date_context
        # to ensure the dashboard picks up the new date range immediately
        record_dates = sorted(list(set(r['duty_date'] for r in self.standby_records)), 
                             key=lambda d: self._parse_date_for_sort(d))
        
        if record_dates:
            self.upload_date_context = {
                'min_date': record_dates[0],
                'max_date': record_dates[-1],
                'default_date': record_dates[0] # Default to start of the schedule
            }
            print(f"Updated upload date context from Crew Schedule: {self.upload_date_context}")

        return sum(self.crew_schedule['summary'].values())



    
    def calculate_metrics(self, filter_date=None, date_context=None):
        """Calculate all dashboard KPIs, optionally filtered by date"""
        # Determine which data to use based on filter
        if filter_date:
            if filter_date in self.flights_by_date:
                flights = self.flights_by_date[filter_date]
                crew_to_regs = self.crew_to_regs_by_date[filter_date]
                reg_flight_hours = self.reg_flight_hours_by_date[filter_date]
                reg_flight_count = self.reg_flight_count_by_date[filter_date]
                crew_group_rotations = self.crew_group_rotations_by_date[filter_date]
            else:
                # Specified date has no flights in DayRep - show empty for flight cards
                flights = []
                crew_to_regs = {}
                reg_flight_hours = {}
                reg_flight_count = {}
                crew_group_rotations = {}
        else:
            # No filter_date - use all data
            flights = self.flights
            crew_to_regs = self.crew_to_regs
            reg_flight_hours = self.reg_flight_hours
            reg_flight_count = self.reg_flight_count
            crew_group_rotations = self.crew_group_rotations
        
        unique_regs = set(f['reg'] for f in flights if f['reg'])
        
        # Calculate Utilization - use SacutilReport data if available
        utilization_data = {}
        
        # Priority 1: Use SacutilReport data for filtered date
        if filter_date and filter_date in self.ac_utilization_by_date:
            utilization_data = self.ac_utilization_by_date[filter_date]
        # Priority 2: Use SacutilReport total data for "All Dates"
        elif self.ac_utilization:
            utilization_data = self.ac_utilization
        # Fallback: Calculate from DayRep data if no SacutilReport
        else:
            stats_by_type = defaultdict(lambda: {'block': 0.0, 'cycles': 0})
            
            for reg, hours in reg_flight_hours.items():
                # Default to '320' as catch-all for consistency with UI
                ac_type = '320'
                if 'A321' in reg or reg.startswith('A6'): ac_type = '321'
                elif 'A330' in reg: ac_type = '330'
                elif 'A320' in reg: ac_type = '320'
                
                stats_by_type[ac_type]['block'] += hours
                stats_by_type[ac_type]['cycles'] += reg_flight_count.get(reg, 0)

            # Format for UI
            for ac_type, stats in stats_by_type.items():
                total_minutes = int(stats['block'] * 60)
                hours_str = f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"
                
                utilization_data[ac_type] = {
                    'total_block': hours_str,
                    'total_cycles': str(stats['cycles']),
                    'dom_block': hours_str,
                    'int_block': '00:00',
                    'avg_util': '-'
                }


        # Calculate Crew & Base counts
        total_crew = len(crew_to_regs)
        
        # Base distribution
        # DayRep doesn't have Base. CrewSchedule does.
        # We need to map active crew (from DayRep) to their base (from CrewSchedule)
        
        base_counts = {'SGN': {'count': 0, 'sby': 0, 'sl': 0}, 
                       'HAN': {'count': 0, 'sby': 0, 'sl': 0}, 
                       'DAD': {'count': 0, 'sby': 0, 'sl': 0}}
                       
        # Build map of Crew ID -> Base from CrewSchedule content?
        # We process CrewSchedule into `crew_schedule` (lists of dicts).
        # We can build a lookup map.
        crew_base_map = {}
        # Iterate all categories to find base info (limited to those with status)
        # Ideally we process the full list. 
        # For now, let's just count based on the 'standby/sick' lists which HAVE base info.
        # But for 'Active Crew' (heatmap main number), we need the base of the flying crew.
        # If we can't get it, we might have to show 0 or global stats.
        
        # Let's try to infer base from `crew_to_regs`? No.
        # Let's just use the `crew_schedule` data to populate SBY/SL counts per base.
        # For Active Crew count, we might just leave it as is or try to approximate.
        
        # Recalculate SBY/Sick per base based on filtered data if available?
        # `crew_schedule_by_date` only stores counts, not base info.
        # So breaking down SBY by Base for a specific DATE is hard with current structure.
        # We need `crew_schedule_by_date` to be `date -> base -> counts`.
        
        # Quick Fix: Just use the SBY/SL counts from `crew_schedule_by_date` and distribute them 
        # proportional to global base distribution? Or just show Global SBY?
        # The user complained about "Active Crew" (Heatmap).
        # Active Crew = Crew flying today.
        # We have `total_crew` (flying today). We need their bases.
        # Since we don't have base info in DayRep, we can't split Active Crew by Base accurately without a master roster.
        pass

        total_flights = len(flights)
        total_crew = len(crew_to_regs)
        
        # Average flight hours per aircraft
        avg_flight_hours = 0
        if reg_flight_hours:
            avg_flight_hours = sum(reg_flight_hours.values()) / len(reg_flight_hours)
        
        # Calculate Crew Rotations (group-based)
        # A rotation is when a crew GROUP flies on multiple different aircraft
        # Count rotations as: (number of unique REGs - 1) for each group that has 2+ REGs
        rotation_count = 0
        rotation_details = []
        
        for crew_set_key, regs_list in crew_group_rotations.items():
            unique_regs_for_group = list(set(regs_list))
            if len(unique_regs_for_group) >= 2:
                # This group had a rotation (changed aircraft)
                rotation_count += 1  # Count as 1 rotation event per group
                
                # Get role info from first crew member
                if crew_set_key and len(crew_set_key) > 0:
                    first_crew_id = crew_set_key[0]
                    role = self.crew_roles.get(first_crew_id, 'UNK')
                    
                    # Find flight numbers for this crew group
                    group_flights = []
                    for f in flights:
                        # Check if this flight was flown by this exact crew group
                        # We need to re-generate the key from the flight's crew string
                        flight_crew_key = self.get_crew_set_key(f.get('crew', ''))
                        if flight_crew_key == crew_set_key:
                             group_flights.append(f.get('flt', ''))
                    
                    rotation_details.append({
                        'crew_ids': list(crew_set_key),
                        'crew_count': len(crew_set_key),
                        'role': role,
                        'regs': sorted(unique_regs_for_group),
                        'flights': sorted(list(set(group_flights))),
                        'rotations': len(unique_regs_for_group) - 1
                    })
        
        # Sort rotation details by number of rotations (descending)
        rotation_details.sort(key=lambda x: (-x['rotations'], -x['crew_count']))
        
        # Role counts (recalculate based on filtered data)
        role_counts = defaultdict(int)
        counted_crew = set()
        operating_crew = []
        
        for f in flights:
            crew_list = self.extract_crew_ids(f.get('crew', ''))
            for role, crew_id in crew_list:
                if crew_id not in counted_crew:
                    role_counts[role] += 1
                    counted_crew.add(crew_id)
                    
                    # Find name if possible (from rolling hours or other sources?)
                    # For now we only have ID and Role from DayRep string: "-NAME(ROLE) ID"
                    # Wait, extract_crew_ids logic in line 83: pattern = r'\(([A-Z]{2})\)\s*(\d+)'
                    # It captures Role and ID. The NAME is before the parens.
                    # I need to improve extraction or parse name here.
                    
                    # Re-extract name from raw string
                    # Sample: "-NGUYEN VAN A(CP) 12345"
                    # Regex to capture Name?
                    # Let's simple use ID and Role for now, or try to get name from self.rolling_hours lookup if available
                    name = "Unknown"
                    # Try lookup in rolling hours
                    for rh in self.rolling_hours:
                        if rh['id'] == crew_id:
                            name = rh['name']
                            break
                    
                    operating_crew.append({
                        'id': crew_id,
                        'role': role,
                        'name': name
                    })
        
        # Sort by Role (CP, FO, PU, FA)
        role_order = {'CP': 1, 'FO': 2, 'PU': 3, 'FA': 4}
        operating_crew.sort(key=lambda x: (role_order.get(x['role'], 99), x['name']))
        
        # Aircraft details
        aircraft_data = []
        for reg in sorted(reg_flight_hours.keys()):
            hours = reg_flight_hours[reg]
            count = reg_flight_count[reg]
            aircraft_data.append({
                'reg': reg,
                'total_hours': round(hours, 1),
                'flights': count,
                'avg_per_flight': round(hours / count, 1) if count > 0 else 0
            })
        
        # Calculate rolling hours statistics
        rolling_stats = {'normal': 0, 'warning': 0, 'critical': 0}
        compliance_rate = 0
        for crew in self.rolling_hours:
            rolling_stats[crew['status']] += 1
            
        if self.rolling_hours:
            safe_crew = rolling_stats['normal'] + rolling_stats['warning']
            compliance_rate = (safe_crew / len(self.rolling_hours)) * 100
        
        # Calculate total block hours
        total_block_hours = sum(reg_flight_hours.values()) if reg_flight_hours else 0
        
        # Merge standby dates into available_dates (so user can filter by standby dates too)
        standby_dates = set(r.get('duty_date') for r in self.standby_records if r.get('duty_date'))
        all_dates = set(self.available_dates) | standby_dates
        # Sort dates by parsing DD/MM/YY
        def parse_date(d):
            try:
                parts = d.split('/')
                return (int(parts[2]), int(parts[1]), int(parts[0]))
            except (ValueError, TypeError, IndexError, AttributeError):
                return (0, 0, 0)
        
        # Filter dates based on context if provided
        if date_context and isinstance(date_context, dict):
            min_date = date_context.get('min_date')
            max_date = date_context.get('max_date')
            
            if min_date and max_date:
                min_dt = parse_date(min_date)
                max_dt = parse_date(max_date)
                
                filtered_dates = []
                for d in all_dates:
                    curr_dt = parse_date(d)
                    if min_dt <= curr_dt <= max_dt:
                        filtered_dates.append(d)
                all_dates = set(filtered_dates)

        merged_available_dates = sorted(list(all_dates), key=parse_date)
        
        # Calculate flight trend vs yesterday
        flight_trend = 0
        flight_trend_direction = 'neutral'  # 'up', 'down', 'neutral'
        if filter_date:
            try:
                # Parse current date
                current_dt = datetime.strptime(filter_date, "%d/%m/%y")
                # Get previous date
                from datetime import timedelta
                prev_dt = current_dt - timedelta(days=1)
                prev_date_str = prev_dt.strftime("%d/%m/%y")
                
                if prev_date_str in self.flights_by_date:
                    prev_count = len(self.flights_by_date[prev_date_str])
                    if prev_count > 0:
                        flight_trend = ((total_flights - prev_count) / prev_count) * 100
                        if flight_trend > 0: flight_trend_direction = 'up'
                        elif flight_trend < 0: flight_trend_direction = 'down'
            except Exception as e:
                print(f"Error calculating flight trend: {e}")

        # Build the data dictionary
        data = {
            'summary': {
                'total_aircraft': len(set(f['reg'] for f in flights if f['reg'])),
                'total_flights': total_flights,
                'flight_trend': flight_trend,
                'flight_trend_direction': flight_trend_direction,
                'total_crew': total_crew,
                'total_crew': total_crew,
                'crew_rotation_count': rotation_count,  # Renamed from multi_reg_count
                'avg_flight_hours': round(avg_flight_hours, 1),
                'total_block_hours': round(total_block_hours, 1)
            },
            'available_dates': merged_available_dates,
            'current_filter_date': filter_date,
            'compliance_rate': compliance_rate,

            'crew_roles': dict(role_counts),
            'operating_crew': operating_crew,
            'aircraft': aircraft_data,
            'crew_rotations': rotation_details[:20],  # Top 20 rotation groups
            'utilization': utilization_data,
            'rolling_hours': self.rolling_hours[:50],  # Top 50
            'rolling_stats': rolling_stats,
            'crew_schedule': self.crew_schedule.copy() if isinstance(self.crew_schedule, dict) else {'summary': {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}},
            'last_updated': datetime.now().isoformat()
        }
        
        # Override crew schedule summary if filtered by date
        if filter_date:
            print(f"DEBUG: Filter date = {filter_date}")
            
        # NEW: Filter standby_records by date and recalculate summary
        if filter_date:
            filtered_standby = [r for r in self.standby_records if r.get('duty_date') == filter_date]
            
            # Recalculate summary from filtered records
            filtered_summary = {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0, 'FGT': 0, 'OFF': 0}
            for record in filtered_standby:
                duty_type = record.get('duty_type', '')
                if duty_type in filtered_summary:
                    filtered_summary[duty_type] += 1
            
            data['crew_schedule']['summary'] = filtered_summary
            data['standby_records'] = filtered_standby
            print(f"DEBUG: Filtered standby records: {len(filtered_standby)}, Summary: {filtered_summary}")
        else:
            # No filter - use totals
            data['standby_records'] = self.standby_records
        
        # FINAL SAFETY CHECK: Ensure summary exists
        if 'summary' not in data['crew_schedule']:
            data['crew_schedule']['summary'] = {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
        
        return data
    
    def get_dashboard_data(self, filter_date=None, date_context=None, source='csv', base=None):
        """Get all data for dashboard, optionally filtered by date"""
        
        print(f"DEBUG: get_dashboard_data called. Source={source}, FilterDate={filter_date}. AIMS count={len(self.aims_flights)}")
        
        # 1. Get base data from selected source
        current_flights = self.flights
        if source == 'aims' and self.aims_flights:
            # Store original state for restoration
            originals = {
                'flights': self.flights,
                'flights_by_date': self.flights_by_date,
                'available_dates': self.available_dates,
                'reg_flight_hours': self.reg_flight_hours,
                'reg_flight_count': self.reg_flight_count,
                'crew_to_regs': self.crew_to_regs,
                'crew_to_regs_by_date': self.crew_to_regs_by_date,
                'reg_flight_hours_by_date': self.reg_flight_hours_by_date,
                'reg_flight_count_by_date': self.reg_flight_count_by_date,
                'crew_group_rotations': self.crew_group_rotations,
                'crew_group_rotations_by_date': self.crew_group_rotations_by_date
            }
            
            # Set AIMS state
            self.flights = self.aims_flights
            self.flights_by_date = self.aims_flights_by_date
            self.available_dates = self.aims_available_dates
            self.reg_flight_hours = self.aims_reg_flight_hours
            self.reg_flight_count = self.aims_reg_flight_count
            self.crew_to_regs = self.aims_crew_to_regs
            self.crew_to_regs_by_date = self.aims_crew_to_regs_by_date
            self.reg_flight_hours_by_date = self.aims_reg_flight_hours_by_date
            self.reg_flight_count_by_date = self.aims_reg_flight_count_by_date
            self.crew_group_rotations = self.aims_crew_group_rotations
            self.crew_group_rotations_by_date = self.aims_crew_group_rotations_by_date
            current_flights = self.aims_flights
            
            try:
                data = self.calculate_metrics(filter_date, date_context)
                data['is_aims_source'] = True
                
                # Apply Live Override while AIMS source is active if needed
                data = self._apply_live_crew_override(data, filter_date, current_flights, base=base)
                
            finally:
                # Restore original state
                for key, value in originals.items():
                    setattr(self, key, value)
        else:
            data = self.calculate_metrics(filter_date, date_context)
            data['is_aims_source'] = False
            # Apply Live Override for CSV source
            data = self._apply_live_crew_override(data, filter_date, self.flights, base=base)

        return data

    def _apply_live_crew_override(self, data, filter_date, current_flights, base=None):
        """Helper to apply live AIMS crew data over base metrics"""
        try:
            from aims_soap_client import is_aims_available, get_aims_client
            
            if is_aims_available():
                target_date_str = filter_date or datetime.now().strftime('%d/%m/%y')
                
                # Parse date string to datetime
                try:
                    parts = target_date_str.split('/')
                    if len(parts) == 3:
                        day, month, year_short = parts
                        y_int = int(year_short)
                        year = y_int + 2000 if y_int < 100 else y_int
                        target_dt = datetime(year, int(month), int(day))
                        
                        client = get_aims_client()
                        
                        # 1. Fetch live crew status (SBY, SL, CSL)
                        status_res = client.get_bulk_crew_status(target_dt, base=base)
                        if status_res.get('success'):
                            status_summary = status_res.get('summary', {})
                            data['crew_schedule']['summary'] = {
                                'SBY': status_summary.get('SBY', 0),
                                'OSBY': status_summary.get('OSBY', 0),
                                'SL': status_summary.get('SL', 0),
                                'CSL': status_summary.get('CSL', 0),
                                'FGT': status_summary.get('FGT', 0),
                                'OFF': status_summary.get('OFF', 0)
                            }
                            data['crew_status_source'] = f'AIMS Live (Bulk - {status_res.get("sampled_crew")}/{status_res.get("total_crew")})'

                        live_data = client.fetch_leg_members_per_day(target_dt)
                        
                        if live_data.get('success'):
                            live_crew_count = live_data.get('total_crew_operating', 0)
                            live_legs = live_data.get('legs', [])
                            
                            # Update Summary
                            data['summary']['total_crew'] = live_crew_count
                            data['data_source_crew'] = 'AIMS Live'
                            
                            # Detailed Crew List
                            new_operating_crew = []
                            new_role_counts = {'CP': 0, 'FO': 0, 'PU': 0, 'FA': 0}
                            seen_crew = set()
                            
                            for leg in live_legs:
                                for crew in leg.get('crew', []):
                                    c_id = crew.get('id')
                                    if c_id and c_id not in seen_crew:
                                        seen_crew.add(c_id)
                                        role = crew.get('role', 'FA')
                                        if role in new_role_counts: new_role_counts[role] += 1
                                        else: new_role_counts['FA'] += 1
                                        new_operating_crew.append({'id': c_id, 'name': crew.get('name', 'Unknown'), 'role': role})
                            
                            if new_operating_crew:
                                data['operating_crew'] = new_operating_crew
                                data['crew_roles'] = new_role_counts

                            # 3. Calculate Rotations mapping flights to registrations
                            flight_reg_map = {}
                            full_d = target_dt.strftime('%d/%m/%Y')
                            short_d = target_dt.strftime('%d/%m/%y')
                            
                            for f in (current_flights or []):
                                f_date = str(f.get('flight_date') or f.get('date') or '')
                                if f_date == full_d or f_date == short_d or f_date.replace('/0', '/') == full_d.replace('/0', '/'):
                                    f_no = str(f.get('flight_no') or f.get('flt') or '')
                                    reg = f.get('reg') or f.get('ac_reg')
                                    if f_no and reg:
                                        flight_reg_map[f_no] = reg
                                        if f_no.startswith('VJ'): flight_reg_map[f_no[2:]] = reg
                                        elif f_no.isdigit(): flight_reg_map['VJ' + f_no] = reg
                            
                            crte_groups = defaultdict(list)
                            for leg in live_legs:
                                f_no = str(leg.get('flight_no', ''))
                                reg = flight_reg_map.get(f_no) or leg.get('reg')
                                crte = None
                                crew_ids = []
                                for c in leg.get('crew', []):
                                    if not crte: crte = c.get('rotation')
                                    if c.get('id'): crew_ids.append(c.get('id'))
                                if crte: crte_groups[crte].append({'flt': f_no, 'reg': reg, 'crew_ids': sorted(crew_ids)})
                            
                            new_rotation_details = []
                            for crte, legs in crte_groups.items():
                                regs = []
                                for l in legs:
                                    if l['reg'] and (not regs or l['reg'] != regs[-1]): regs.append(l['reg'])
                                
                                if len(regs) > 1:
                                    main_crew = legs[0]['crew_ids']
                                    lead_role = "FA"
                                    for c_id in main_crew:
                                        for oc in new_operating_crew:
                                            if oc['id'] == c_id:
                                                if oc['role'] == 'CP': lead_role = 'CP'; break
                                                if oc['role'] == 'FO' and lead_role != 'CP': lead_role = 'FO'
                                        if lead_role == 'CP': break
                                    
                                    new_rotation_details.append({
                                        'id': crte, 'crew_count': len(main_crew), 'role': lead_role,
                                        'regs': regs, 'flights': [l['flt'] for l in legs], 'rotations': len(regs) - 1
                                    })
                            
                            if new_rotation_details:
                                new_rotation_details.sort(key=lambda x: (-x['rotations'], -x['crew_count']))
                                data['crew_rotations'] = new_rotation_details[:100]
                                data['summary']['crew_rotation_count'] = len(new_rotation_details)
                            else:
                                data['crew_rotations'] = []
                                data['summary']['crew_rotation_count'] = 0
                except Exception as e:
                    print(f"Error applying live crew override: {e}")
        except ImportError:
            pass
        return data


    def _calculate_kpi_maps(self, flights):
        """Helper to calculate all grouping and KPI maps from a list of flights"""
        flights_by_date = defaultdict(list)
        unique_dates = set()
        crew_to_regs = defaultdict(set)
        crew_to_regs_by_date = defaultdict(lambda: defaultdict(set))
        reg_flight_hours = defaultdict(float)
        reg_flight_hours_by_date = defaultdict(lambda: defaultdict(float))
        reg_flight_count = defaultdict(int)
        reg_flight_count_by_date = defaultdict(lambda: defaultdict(int))
        crew_group_rotations = defaultdict(list)
        crew_group_rotations_by_date = defaultdict(lambda: defaultdict(list))
        
        for flight in flights:
            op_date = flight.get('date')
            if op_date:
                unique_dates.add(op_date)
                flights_by_date[op_date].append(flight)
                
                reg = flight.get('reg', '')
                if reg:
                    # Recalculate duration
                    std = self.parse_time(flight.get('std', ''))
                    sta = self.parse_time(flight.get('sta', ''))
                    if std is not None and sta is not None:
                        duration = sta - std
                        if duration < 0: duration += 24 * 60
                        hours = duration / 60
                        reg_flight_hours[reg] += hours
                        reg_flight_count[reg] += 1
                        reg_flight_hours_by_date[op_date][reg] += hours
                        reg_flight_count_by_date[op_date][reg] += 1
                
                crew_str = flight.get('crew', '')
                if crew_str:
                    crew_list = self.extract_crew_ids(crew_str)
                    for role, crew_id in crew_list:
                        crew_to_regs[crew_id].add(reg)
                        crew_to_regs_by_date[op_date][crew_id].add(reg)
                        self.crew_roles[crew_id] = role
                    
                    if crew_list:
                        key = self.get_crew_set_key(crew_str)
                        if key:
                            crew_group_rotations[key].append(reg)
                            crew_group_rotations_by_date[op_date][key].append(reg)
        
        available_dates = sorted(list(unique_dates), key=lambda d: self._parse_date_for_sort(d))
        
        return flights_by_date, available_dates, reg_flight_hours, reg_flight_count, \
               crew_to_regs, crew_to_regs_by_date, reg_flight_hours_by_date, \
               reg_flight_count_by_date, crew_group_rotations, crew_group_rotations_by_date
    
    def export_to_json(self, output_file='dashboard_data.json'):
        """Export data to JSON file"""
        data = self.calculate_metrics()
        output_path = self.data_dir / output_file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return str(output_path)
    
    # ============================================================
    # AIMS Integration Methods
    # ============================================================
    
    def get_alert_status(self, block_hours: float) -> str:
        """
        Determine alert status based on Alert Matrix
        
        Alert Matrix (28-day rolling):
        - Normal: <= 85 hours (Green)
        - Warning: > 85 hours (Yellow)
        - Critical: > 95 hours (Red)
        
        Args:
            block_hours: Total block hours in 28-day period
            
        Returns:
            str: 'normal', 'warning', or 'critical'
        """
        if block_hours > 95:
            return 'critical'
        elif block_hours > 85:
            return 'warning'
        return 'normal'
    
    def convert_utc_to_gmt7(self, utc_datetime):
        """
        Convert UTC datetime to GMT+7 (Vietnam timezone)
        
        Args:
            utc_datetime: datetime object or ISO string in UTC
            
        Returns:
            datetime: Converted to GMT+7
        """
        from datetime import timedelta
        
        if isinstance(utc_datetime, str):
            try:
                # Try parsing ISO format
                utc_datetime = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
            except ValueError:
                return utc_datetime
        
        # Simple UTC to GMT+7 conversion
        if utc_datetime:
            return utc_datetime + timedelta(hours=7)
        return utc_datetime
    
    def load_from_aims(self, from_date=None, to_date=None):
        """
        Load data from AIMS API instead of CSV files
        
        This method integrates with aims_soap_client to fetch live data.
        Only active when AIMS_ENABLED=true in environment.
        
        Args:
            from_date: Start date (default: today - 30 days)
            to_date: End date (default: today + 30 days)
            
        Returns:
            dict: Summary of loaded data
        """
        try:
            from aims_soap_client import get_aims_client, is_aims_available
            
            if not is_aims_available():
                return {'success': False, 'error': 'AIMS not available or not enabled'}
            
            client = get_aims_client()
            
            # Use optimized date range if not specified
            if not from_date or not to_date:
                from_date, to_date = client.get_optimized_date_range()
            
            result = {
                'success': True,
                'flights_loaded': 0,
                'crew_loaded': 0,
                'errors': []
            }
            
            # 1. Load flight details
            flight_result = client.get_flight_details(from_date, to_date)
            if flight_result['success']:
                # Convert AIMS flight data to our internal format
                for flight in flight_result['flights']:
                    # Apply UTC to GMT+7 conversion for display
                    dep_dt = flight.get('dep_actual_dt')
                    if dep_dt:
                        flight['dep_actual_dt_local'] = self.convert_utc_to_gmt7(dep_dt)
                    
                    # Store in our data structure
                    self.flights.append(flight)
                    
                    # Group by date
                    flight_date = flight.get('flight_date', '')
                    if flight_date:
                        self.flights_by_date[flight_date].append(flight)
                
                result['flights_loaded'] = len(flight_result['flights'])
            else:
                result['errors'].append(flight_result.get('error'))
            
            # 2. Load crew list
            crew_result = client.get_crew_list(from_date, to_date)
            if crew_result['success']:
                result['crew_loaded'] = len(crew_result['crew_list'])
            else:
                result['errors'].append(crew_result.get('error'))
            
            return result
            
        except ImportError as e:
            return {'success': False, 'error': f'AIMS module not available: {e}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def calculate_rolling_28day_stats(self):
        """
        Calculate rolling 28-day statistics with Alert Matrix
        
        Returns summary of crew compliance status based on block hours.
        
        Returns:
            dict: {
                'total_crew': int,
                'normal_count': int,
                'warning_count': int,
                'critical_count': int,
                'compliance_rate': float (percentage of normal crew)
            }
        """
        stats = {
            'total_crew': 0,
            'normal_count': 0,
            'warning_count': 0,
            'critical_count': 0,
            'compliance_rate': 100.0
        }
        
        if not self.rolling_hours:
            return stats
        
        stats['total_crew'] = len(self.rolling_hours)
        
        for crew in self.rolling_hours:
            status = crew.get('status', 'normal')
            if status == 'critical':
                stats['critical_count'] += 1
            elif status == 'warning':
                stats['warning_count'] += 1
            else:
                stats['normal_count'] += 1
        
        if stats['total_crew'] > 0:
            stats['compliance_rate'] = round(
                (stats['normal_count'] / stats['total_crew']) * 100, 1
            )
        
        return stats



# Singleton instance for the API
_processor = None

def get_processor():
    global _processor
    if _processor is None:
        _processor = DataProcessor(Path(__file__).parent)
        # Load default data
        try:
            if db.is_connected():
                print("Initial load from Supabase...")
                _processor.load_from_supabase()
            else:
                _processor.process_dayrep_csv()
                _processor.process_sacutil_csv()
                _processor.process_rolcrtot_csv()
                _processor.process_crew_schedule_csv()
        except Exception as e:
            print(f"Warning: Could not load default data: {e}")
    return _processor

def refresh_data():
    """Refresh data from Supabase if available, otherwise from default CSV files"""
    processor = get_processor()
    if db.is_connected():
        print("Refreshing data from Supabase...")
        processor.load_from_supabase()
    else:
        print("Refreshing data from local CSVs...")
        processor.process_dayrep_csv()
        processor.process_sacutil_csv()
        processor.process_rolcrtot_csv()
        processor.process_crew_schedule_csv()
    return processor.get_dashboard_data()


if __name__ == '__main__':
    # Test the processor
    processor = DataProcessor()
    print("Processing DayRepReport...")
    flights = processor.process_dayrep_csv('DayRepReport15Jan2026.csv')
    print(f"Loaded {flights} flights")
    
    print("\nProcessing SacutilReport...")
    utils = processor.process_sacutil_csv('SacutilReport1.csv')
    print(f"Loaded {utils} aircraft utilization records")
    
    print("\nCalculating metrics...")
    metrics = processor.calculate_metrics()
    print(json.dumps(metrics['summary'], indent=2))
    
    print("\nExporting to JSON...")
    output = processor.export_to_json()
    print(f"Exported to: {output}")

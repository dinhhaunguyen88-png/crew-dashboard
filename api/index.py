"""
Vercel Serverless Function Handler for Crew Management Dashboard
With Supabase Database Integration and Robust Error Handling
"""

from flask import Flask, request, render_template, redirect, url_for, flash, jsonify
import os
import sys
import re
import traceback
from pathlib import Path

# Add the parent directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

# Initialize Flask with correct template folder
app = Flask(__name__, template_folder=root_dir, static_folder=os.path.join(root_dir, 'static'))
app.secret_key = os.environ.get('SECRET_KEY', 'crew-dashboard-secret-key-2026')

# ==================== SUPABASE INITIALIZATION ====================
# Check environment variables first
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Log environment status
if not SUPABASE_URL:
    print("[ERROR] SUPABASE_URL environment variable is not set!")
if not SUPABASE_KEY:
    print("[ERROR] SUPABASE_KEY environment variable is not set!")

# Try to import modules with error handling
MODULES_LOADED = False
db = None
processor = None
supabase_connected = False
supabase_msg = "Not initialized"

try:
    from data_processor import DataProcessor
    processor = DataProcessor(data_dir=root_dir)
    MODULES_LOADED = True
    print("[OK] DataProcessor loaded successfully")
except ImportError as e:
    print(f"[ERROR] Failed to import DataProcessor: {e}")
except Exception as e:
    print(f"[ERROR] DataProcessor initialization failed: {e}")

# Only try to import supabase_client if credentials are configured
if SUPABASE_URL and SUPABASE_KEY:
    try:
        import supabase_client as db
        supabase_connected, supabase_msg = db.check_connection()
        print(f"[SUPABASE] {supabase_msg}")
    except ImportError as e:
        print(f"[ERROR] Failed to import supabase_client: {e}")
        db = None
        supabase_msg = f"Import error: {e}"
    except Exception as e:
        print(f"[ERROR] Supabase connection check failed: {e}")
        supabase_connected = False
        supabase_msg = f"Connection error: {e}"
else:
    print("[INFO] Supabase credentials not configured - using local CSV files")
    supabase_msg = "Credentials not configured"


# ==================== DEFAULT DATA STRUCTURE ====================
def get_default_data():
    """Return default empty data structure to prevent template errors"""
    return {
        'summary': {
            'total_aircraft': 0,
            'total_flights': 0,
            'total_crew': 0,
            'avg_flight_hours': 0,
            'total_block_hours': 0,
            'crew_rotation_count': 0,
            'crew_by_role': {'CP': 0, 'FO': 0, 'PU': 0, 'FA': 0}
        },
        'aircraft': [],
        'crew_roles': {'CP': 0, 'FO': 0, 'PU': 0, 'FA': 0},
        'crew_rotations': [],
        'available_dates': [],
        'operating_crew': [],
        'utilization': {},
        'rolling_hours': [],
        'rolling_stats': {'normal': 0, 'warning': 0, 'critical': 0, 'total': 0},
        'crew_schedule': {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
    }


# ==================== DATA LOADING FUNCTIONS ====================
def load_data_from_supabase(filter_date=None):
    """Load all data from Supabase and process metrics - with full error handling"""
    default_data = get_default_data()
    
    if not db or not supabase_connected:
        print("[INFO] Supabase not available, returning default data")
        return default_data, []
    
    try:
        # Get flights from Supabase
        flights = db.get_flights(filter_date) or []
        available_dates = db.get_available_dates() or []
        
        if not flights:
            print("[INFO] No flights found in Supabase")
            return default_data, available_dates
        
        # Get other data with individual error handling
        try:
            rolling_hours = db.get_rolling_hours() or []
        except Exception as e:
            print(f"[WARN] Failed to get rolling_hours: {e}")
            rolling_hours = []
        
        try:
            crew_schedule_summary = db.get_crew_schedule_summary(filter_date) or {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
        except Exception as e:
            print(f"[WARN] Failed to get crew_schedule_summary: {e}")
            crew_schedule_summary = {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
        
        try:
            ac_utilization = db.get_ac_utilization(filter_date) or []
        except Exception as e:
            print(f"[WARN] Failed to get ac_utilization: {e}")
            ac_utilization = []
        
        # Process flights to calculate metrics if processor is available
        if processor:
            processor.flights = flights
            processor.available_dates = available_dates
            
            # Re-populate internal data structures from flights
            processor.crew_to_regs.clear()
            processor.reg_flight_hours.clear()
            processor.reg_flight_count.clear()
            
            for flight in flights:
                try:
                    reg = flight.get('reg', '')
                    crew_string = flight.get('crew', '')
                    std = flight.get('std', '')
                    sta = flight.get('sta', '')
                    
                    # Calculate flight hours
                    if std and sta and ':' in str(std) and ':' in str(sta):
                        std_parts = str(std).split(':')
                        sta_parts = str(sta).split(':')
                        std_min = int(std_parts[0]) * 60 + int(std_parts[1])
                        sta_min = int(sta_parts[0]) * 60 + int(sta_parts[1])
                        duration = sta_min - std_min
                        if duration < 0:
                            duration += 24 * 60
                        hours = duration / 60
                        processor.reg_flight_hours[reg] += hours
                        processor.reg_flight_count[reg] += 1
                    
                    # Extract crew
                    if crew_string:
                        pattern = r'\(([A-Z]{2})\)\s*(\d+)'
                        matches = re.findall(pattern, str(crew_string))
                        for role, crew_id in matches:
                            processor.crew_to_regs[crew_id].add(reg)
                            processor.crew_roles[crew_id] = role
                except Exception as e:
                    print(f"[WARN] Error processing flight: {e}")
                    continue
            
            # Calculate metrics
            try:
                metrics = processor.calculate_metrics(filter_date)
            except Exception as e:
                print(f"[WARN] calculate_metrics failed: {e}")
                metrics = default_data
        else:
            metrics = default_data
        
        # Override with Supabase data
        metrics['rolling_hours'] = rolling_hours[:20] if rolling_hours else []
        metrics['crew_schedule'] = crew_schedule_summary
        
        # Process rolling hours stats
        normal_count = len([r for r in rolling_hours if r.get('status') == 'normal'])
        warning_count = len([r for r in rolling_hours if r.get('status') == 'warning'])
        critical_count = len([r for r in rolling_hours if r.get('status') == 'critical'])
        metrics['rolling_stats'] = {
            'normal': normal_count,
            'warning': warning_count,
            'critical': critical_count,
            'total': len(rolling_hours)
        }
        
        # AC Utilization
        util_dict = {}
        for item in ac_utilization:
            ac_type = item.get('ac_type', '')
            util_dict[ac_type] = {
                'dom_block': item.get('dom_block', '00:00'),
                'int_block': item.get('int_block', '00:00'),
                'total_block': item.get('total_block', '00:00'),
                'dom_cycles': item.get('dom_cycles', 0),
                'int_cycles': item.get('int_cycles', 0),
                'total_cycles': item.get('total_cycles', 0),
                'avg_util': item.get('avg_util', '')
            }
        metrics['utilization'] = util_dict
        
        return metrics, available_dates
        
    except Exception as e:
        print(f"[ERROR] load_data_from_supabase failed: {e}")
        traceback.print_exc()
        return default_data, []


def fallback_to_local():
    """Fallback to local CSV files if Supabase not available"""
    if not processor:
        print("[WARN] Processor not available for local fallback")
        return False
    
    try:
        processor.process_dayrep_csv()
        processor.process_sacutil_csv()
        processor.process_rolcrtot_csv()
        processor.process_crew_schedule_csv()
        print("[OK] Local CSV files loaded successfully")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to load local CSV files: {e}")
        traceback.print_exc()
        return False


# ==================== ROUTES ====================
@app.route('/', methods=['GET'])
def index():
    """Render the dashboard with data - with comprehensive error handling"""
    filter_date = request.args.get('date', None)
    
    # Always start with default data to ensure template never crashes
    data = get_default_data()
    available_dates = []
    
    try:
        # Try Supabase first
        if supabase_connected and db:
            print("[INFO] Loading data from Supabase...")
            metrics_data, available_dates = load_data_from_supabase(filter_date)
        else:
            # Fallback to local CSV files
            print("[INFO] Falling back to local CSV files...")
            if fallback_to_local() and processor:
                try:
                    metrics_data = processor.calculate_metrics(filter_date)
                    available_dates = processor.available_dates or []
                except Exception as e:
                    print(f"[ERROR] calculate_metrics failed: {e}")
                    metrics_data = get_default_data()
            else:
                metrics_data = get_default_data()
        
        # Build final data structure with safe access
        data = {
            'summary': metrics_data.get('summary', data['summary']),
            'aircraft': list(processor.reg_flight_hours.keys()) if processor and hasattr(processor, 'reg_flight_hours') else [],
            'crew_roles': metrics_data.get('crew_roles', data['crew_roles']),
            'crew_rotations': metrics_data.get('crew_rotations', []),
            'available_dates': available_dates or [],
            'operating_crew': metrics_data.get('operating_crew', []),
            'utilization': metrics_data.get('utilization', {}),
            'rolling_hours': metrics_data.get('rolling_hours', []),
            'rolling_stats': metrics_data.get('rolling_stats', data['rolling_stats']),
            'crew_schedule': metrics_data.get('crew_schedule', data['crew_schedule'])
        }
        
    except Exception as e:
        print(f"[ERROR] Index route failed: {e}")
        traceback.print_exc()
        # Keep default data on error - template will still render
    
    try:
        return render_template('crew_dashboard.html', data=data, filter_date=filter_date)
    except Exception as e:
        print(f"[ERROR] Template render failed: {e}")
        traceback.print_exc()
        return f"<h1>Dashboard Error</h1><p>Failed to render template: {str(e)}</p><p>Please check server logs.</p>", 500


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle CSV file uploads and save to Supabase"""
    if not supabase_connected or not db:
        flash('Supabase not connected. Please configure environment variables.')
        return redirect(url_for('index'))
    
    try:
        # Process DayRepReport
        if 'dayrep' in request.files:
            file = request.files['dayrep']
            if file.filename and processor:
                content = file.read()
                processor.process_dayrep_csv(file_content=content)
                
                flights_data = []
                for flight in processor.flights:
                    flights_data.append({
                        'date': flight.get('date', ''),
                        'calendar_date': flight.get('calendar_date', ''),
                        'reg': flight.get('reg', ''),
                        'flt': flight.get('flt', ''),
                        'dep': flight.get('dep', ''),
                        'arr': flight.get('arr', ''),
                        'std': flight.get('std', ''),
                        'sta': flight.get('sta', ''),
                        'crew': flight.get('crew', '')
                    })
                db.insert_flights(flights_data)
        
        # Process SacutilReport
        if 'sacutil' in request.files:
            file = request.files['sacutil']
            if file.filename and processor:
                content = file.read()
                processor.process_sacutil_csv(file_content=content)
                
                util_data = []
                for date_str, ac_types in processor.ac_utilization_by_date.items():
                    for ac_type, stats in ac_types.items():
                        util_data.append({
                            'date': date_str,
                            'ac_type': ac_type,
                            'dom_block': stats.get('dom_block', '00:00'),
                            'int_block': stats.get('int_block', '00:00'),
                            'total_block': stats.get('total_block', '00:00'),
                            'dom_cycles': int(stats.get('dom_cycles', 0) or 0),
                            'int_cycles': int(stats.get('int_cycles', 0) or 0),
                            'total_cycles': int(stats.get('total_cycles', 0) or 0),
                            'avg_util': stats.get('avg_util', '')
                        })
                if util_data:
                    db.insert_ac_utilization(util_data)
        
        # Process RolCrTotReport
        if 'rolcrtot' in request.files:
            file = request.files['rolcrtot']
            if file.filename and processor:
                content = file.read()
                processor.process_rolcrtot_csv(file_content=content)
                
                hours_data = []
                for item in processor.rolling_hours:
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
                if hours_data:
                    db.insert_rolling_hours(hours_data)
        
        # Process Crew Schedule
        if 'crew_schedule' in request.files:
            file = request.files['crew_schedule']
            if file.filename and processor:
                content = file.read()
                processor.process_crew_schedule_csv(file_content=content)
                
                schedule_data = []
                for date_str, counts in processor.crew_schedule_by_date.items():
                    for status_type in ['SL', 'CSL', 'SBY', 'OSBY']:
                        count = counts.get(status_type, 0)
                        for _ in range(count):
                            schedule_data.append({
                                'date': date_str,
                                'status_type': status_type
                            })
                if schedule_data:
                    db.insert_crew_schedule(schedule_data)
        
        flash('Data uploaded successfully to Supabase!')
        
    except Exception as e:
        print(f"[ERROR] Upload failed: {e}")
        traceback.print_exc()
        flash(f'Upload error: {str(e)}')
    
    return redirect(url_for('index'))


@app.route('/api/status', methods=['GET'])
def api_status():
    """Check system status including Supabase connection"""
    status = {
        'modules_loaded': MODULES_LOADED,
        'processor_available': processor is not None,
        'supabase_url_configured': SUPABASE_URL is not None,
        'supabase_key_configured': SUPABASE_KEY is not None,
        'supabase_connected': supabase_connected,
        'supabase_message': supabase_msg
    }
    
    if db:
        try:
            status['supabase_details'] = db.get_connection_status()
        except Exception as e:
            status['supabase_details'] = {'error': str(e)}
    
    return jsonify(status)


@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({'status': 'ok', 'message': 'Crew Dashboard is running'})


# Vercel handler - export as 'app' (Flask convention)
# Vercel Python runtime looks for 'app' or 'handler'
handler = app

# For local testing
if __name__ == '__main__':
    app.run(debug=True, port=5000)

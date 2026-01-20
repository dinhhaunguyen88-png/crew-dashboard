"""
Vercel Serverless Function Handler for Crew Management Dashboard
With Supabase Database Integration
"""

from flask import Flask, request, render_template, redirect, url_for, flash
import os
import sys
import re
from pathlib import Path

# Add the parent directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_dir)

# Initialize Flask with correct template folder
app = Flask(__name__, template_folder=root_dir, static_folder=os.path.join(root_dir, 'static'))
app.secret_key = os.environ.get('SECRET_KEY', 'crew-dashboard-secret-key-2026')

# Import modules after path is set
try:
    from data_processor import DataProcessor
    import supabase_client as db
    MODULES_LOADED = True
except ImportError as e:
    print(f"Module import error: {e}")
    MODULES_LOADED = False
    db = None

# Initialize DataProcessor
processor = DataProcessor(data_dir=root_dir) if MODULES_LOADED else None

# Check Supabase connection
if MODULES_LOADED and db:
    try:
        supabase_connected, supabase_msg = db.check_connection()
        print(f"Supabase: {supabase_msg}")
    except Exception as e:
        supabase_connected = False
        supabase_msg = str(e)
        print(f"Supabase error: {e}")
else:
    supabase_connected = False
    supabase_msg = "Modules not loaded"


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


def load_data_from_supabase(filter_date=None):
    """Load all data from Supabase and process metrics"""
    if not db or not supabase_connected:
        return get_default_data(), []
    
    try:
        # Get flights from Supabase
        flights = db.get_flights(filter_date)
        available_dates = db.get_available_dates()
        
        if not flights:
            return get_default_data(), available_dates
        
        # Get other data
        rolling_hours = db.get_rolling_hours()
        crew_schedule_summary = db.get_crew_schedule_summary(filter_date)
        ac_utilization = db.get_ac_utilization(filter_date)
        
        # Process flights to calculate metrics
        processor.flights = flights
        processor.available_dates = available_dates
        
        # Re-populate internal data structures from flights
        processor.crew_to_regs.clear()
        processor.reg_flight_hours.clear()
        processor.reg_flight_count.clear()
        
        for flight in flights:
            reg = flight.get('reg', '')
            crew_string = flight.get('crew', '')
            std = flight.get('std', '')
            sta = flight.get('sta', '')
            
            # Calculate flight hours
            if std and sta and ':' in str(std) and ':' in str(sta):
                try:
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
                except:
                    pass
            
            # Extract crew
            if crew_string:
                pattern = r'\(([A-Z]{2})\)\s*(\d+)'
                matches = re.findall(pattern, str(crew_string))
                for role, crew_id in matches:
                    processor.crew_to_regs[crew_id].add(reg)
                    processor.crew_roles[crew_id] = role
        
        # Calculate metrics
        metrics = processor.calculate_metrics(filter_date)
        
        # Override with Supabase data
        metrics['rolling_hours'] = rolling_hours[:20] if rolling_hours else []
        metrics['crew_schedule'] = crew_schedule_summary or {'SL': 0, 'CSL': 0, 'SBY': 0, 'OSBY': 0}
        
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
        print(f"Error loading from Supabase: {e}")
        return get_default_data(), []


def fallback_to_local():
    """Fallback to local CSV files if Supabase not available"""
    if not processor:
        return
    try:
        processor.process_dayrep_csv()
        processor.process_sacutil_csv()
        processor.process_rolcrtot_csv()
        processor.process_crew_schedule_csv()
    except Exception as e:
        print(f"Error loading local files: {e}")


@app.route('/', methods=['GET'])
def index():
    """Render the dashboard with data"""
    filter_date = request.args.get('date', None)
    
    # Start with default data
    data = get_default_data()
    available_dates = []
    
    try:
        # Try Supabase first, fallback to local
        if supabase_connected and db:
            metrics_data, available_dates = load_data_from_supabase(filter_date)
        else:
            fallback_to_local()
            if processor:
                metrics_data = processor.calculate_metrics(filter_date)
                available_dates = processor.available_dates
            else:
                metrics_data = get_default_data()
        
        # Build data structure for template with safe defaults
        data = {
            'summary': metrics_data.get('summary', data['summary']),
            'aircraft': list(processor.reg_flight_hours.keys()) if processor else [],
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
        print(f"Error in index route: {e}")
        # Keep default data on error
    
    return render_template('crew_dashboard.html', data=data, filter_date=filter_date)


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle CSV file uploads and save to Supabase"""
    if not supabase_connected or not db:
        flash('Supabase not connected. Please configure credentials.')
        return redirect(url_for('index'))
    
    try:
        # Process DayRepReport
        if 'dayrep' in request.files:
            file = request.files['dayrep']
            if file.filename and processor:
                content = file.read()
                processor.process_dayrep_csv(file_content=content)
                
                # Prepare data for Supabase
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
        print(f"Upload error: {e}")
        flash(f'Upload error: {str(e)}')
    
    return redirect(url_for('index'))


@app.route('/api/status', methods=['GET'])
def api_status():
    """Check system status including Supabase connection"""
    status = {
        'modules_loaded': MODULES_LOADED,
        'supabase_connected': supabase_connected,
        'supabase_message': supabase_msg
    }
    
    if db:
        status['supabase_details'] = db.get_connection_status()
    
    return status


# Vercel looks for 'app' or 'handler' in the module
# Expose Flask app as the handler for Vercel
handler = app

# For local testing
if __name__ == '__main__':
    app.run(debug=True, port=5000)

"""
Flask Server for Crew Management Dashboard (SSR Version)
Renders the dashboard directly using Jinja2 templates.
Features: Auto-reload on CSV changes, improved error handling, centralized middleware
"""

from flask import Flask, request, render_template, redirect, url_for, jsonify
from werkzeug.utils import secure_filename
import os
import threading
from pathlib import Path
from data_processor import get_processor, refresh_data
from datetime import datetime

# Import file watcher (with fallback if not installed)
try:
    from file_watcher import create_watcher
    FILE_WATCHER_AVAILABLE = True
except ImportError:
    FILE_WATCHER_AVAILABLE = False
    print("⚠️  Warning: watchdog not installed. Auto-reload disabled.")
    print("   Install with: pip install watchdog>=3.0.0")

# Import error handler middleware (with fallback)
try:
    from api.middleware.error_handler import setup_error_handlers, setup_request_logging
    ERROR_HANDLER_AVAILABLE = True
except ImportError:
    ERROR_HANDLER_AVAILABLE = False
    print("⚠️  Warning: Error handler middleware not found.")

app = Flask(__name__, template_folder='.')  # Look for templates in current dir
app.secret_key = 'crew-dashboard-secret'  # Required for sessions if needed

# Setup error handlers
if ERROR_HANDLER_AVAILABLE:
    setup_error_handlers(app)
    # Uncomment for request logging:
    # setup_request_logging(app)

# Configuration
UPLOAD_FOLDER = Path(__file__).parent / 'uploads'
UPLOAD_FOLDER.mkdir(exist_ok=True)
ALLOWED_EXTENSIONS = {'csv'}

app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Global state for file watcher
file_watcher = None
last_update_time = datetime.now()
pending_refresh = False

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def on_csv_file_change(file_path, event_type):
    """Callback when CSV files change"""
    global pending_refresh, last_update_time
    
    print(f"[Auto-Refresh] CSV file {event_type}: {file_path}")
    
    # Determine which file changed and process it
    file_name = Path(file_path).name.lower()
    processor = get_processor()
    
    try:
        if 'dayrep' in file_name:
            print("[Auto-Refresh] Processing DayRepReport...")
            processor.process_dayrep_csv(file_path=Path(file_path))
        elif 'sacutil' in file_name:
            print("[Auto-Refresh] Processing SacutilReport...")
            processor.process_sacutil_csv(file_path=Path(file_path))
        elif 'rolcrtot' in file_name or 'rolcr' in file_name:
            print("[Auto-Refresh] Processing RolCrTotReport...")
            processor.process_rolcrtot_csv(file_path=Path(file_path))
        elif 'crew' in file_name and 'schedule' in file_name:
            print("[Auto-Refresh] Processing Crew Schedule...")
            processor.process_crew_schedule_csv(file_path=Path(file_path))
        else:
            print(f"[Auto-Refresh] Unknown CSV file type: {file_name}")
            return
        
        pending_refresh = True
        last_update_time = datetime.now()
        print(f"[Auto-Refresh] Data updated successfully at {last_update_time.strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"[Auto-Refresh] Error processing file: {e}")

def start_file_watcher():
    """Start watching CSV files in the project directory"""
    global file_watcher
    
    if not FILE_WATCHER_AVAILABLE:
        return
    
    # Watch both the main directory and uploads folder
    project_dir = Path(__file__).parent
    
    try:
        file_watcher = create_watcher(project_dir, on_csv_file_change)
        print(f"✅ File watcher started - monitoring {project_dir}")
        print("   Changes to CSV files will auto-update the dashboard")
    except Exception as e:
        print(f"⚠️  Could not start file watcher: {e}")

@app.route('/', methods=['GET'])
def index():
    """Render the dashboard with data"""
    processor = get_processor()
    
    # Get optional date filter from query parameter
    filter_date = request.args.get('date', None)
    
    # Get data
    data = processor.get_dashboard_data(filter_date)
    
    # Calculate compliance rate from rolling_hours
    compliance_stats = processor.calculate_rolling_28day_stats()
    data['compliance_rate'] = compliance_stats.get('compliance_rate', 100)
    
    # Add last update timestamp
    data['last_updated'] = last_update_time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Check DB connection status for UI debugging
    from supabase_client import is_connected
    db_connected = is_connected()
    
    # Check AIMS availability
    try:
        from aims_soap_client import is_aims_available
        aims_enabled = is_aims_available()
    except ImportError:
        aims_enabled = False
    
    # Render template with data
    return render_template('crew_dashboard.html', 
                          data=data, 
                          filter_date=filter_date, 
                          db_connected=db_connected,
                          aims_enabled=aims_enabled)

@app.route('/api/check_updates', methods=['GET'])
def check_updates():
    """API endpoint to check if data has been updated"""
    global pending_refresh, last_update_time
    
    has_update = pending_refresh
    pending_refresh = False  # Reset flag
    
    return jsonify({
        'has_update': has_update,
        'last_update': last_update_time.isoformat(),
        'update_time_readable': last_update_time.strftime('%H:%M:%S')
    })

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads via standard HTML Form - In-Memory Processing for Vercel/Supabase"""
    
    # Map form field names to processor methods
    file_map = {
        'dayrep': 'process_dayrep_csv',
        'sacutil': 'process_sacutil_csv',
        'rolcrtot': 'process_rolcrtot_csv',
        'crew_schedule': 'process_crew_schedule_csv'
    }
    
    processor = get_processor()
    uploaded_any = False
    errors = []
    
    for field_name, method_name in file_map.items():
        if field_name in request.files:
            file = request.files[field_name]
            # Check if file is selected
            if file and file.filename:
                try:
                    # Read content into memory
                    content = file.read()
                    
                    # Validate file is not empty
                    if len(content) == 0:
                        errors.append(f"{field_name}: File is empty")
                        continue
                    
                    # Get the processing method
                    process_method = getattr(processor, method_name)
                    
                    # Process directly with content
                    result = process_method(file_content=content)
                    
                    if result is not None and result > 0:
                        uploaded_any = True
                        print(f"✅ Processed {field_name}: {result} records")
                    else:
                        errors.append(f"{field_name}: No data processed")
                        
                except Exception as e:
                    error_msg = f"{field_name}: {str(e)}"
                    errors.append(error_msg)
                    print(f"❌ Error processing {field_name}: {e}")
    
    if uploaded_any:
        global last_update_time, pending_refresh
        last_update_time = datetime.now()
        pending_refresh = True
    
    # Show errors if any (could be added to session flash messages)
    if errors:
        print(f"Upload errors: {errors}")
    
    # Redirect back to dashboard
    return redirect(url_for('index'))

@app.route('/debug', methods=['GET'])
def debug_info():
    """Return inner state for debugging"""
    processor = get_processor()
    import json
    filter_date = request.args.get('date', None)
    return {
        'keys_repr': [repr(k) for k in processor.crew_schedule_by_date.keys()],
        'filter_date_repr': repr(filter_date) if filter_date else 'None',
        'sample_key': list(processor.crew_schedule_by_date.keys())[0] if processor.crew_schedule_by_date else 'None',
        'total_summary': processor.crew_schedule['summary'],
        'filter_date_param': filter_date,
        'daily_stats_for_filter_date': processor.crew_schedule_by_date.get(filter_date) if filter_date else 'No Date',
        'file_watcher_active': file_watcher is not None and file_watcher.is_running if file_watcher else False,
        'last_update': last_update_time.isoformat()
    }

if __name__ == '__main__':
    # Initialize processor on startup
    get_processor()
    
    # Start file watcher in a separate thread
    if FILE_WATCHER_AVAILABLE:
        watcher_thread = threading.Thread(target=start_file_watcher, daemon=True)
        watcher_thread.start()
    
    print("============================================================")
    print("Crew Management Dashboard (SSR Version)")
    print("============================================================")
    print("")
    print("Starting server on port 5000...")
    print("Dashboard: http://localhost:5000")
    print("")
    if FILE_WATCHER_AVAILABLE:
        print("✅ Auto-reload enabled - CSV changes will update dashboard")
    else:
        print("⚠️  Auto-reload disabled - install watchdog to enable")
    print("")
    print("Press Ctrl+C to stop")
    print("============================================================")
    app.run(host='0.0.0.0', port=5000, debug=True)


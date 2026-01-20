"""
Flask Server for Crew Management Dashboard (SSR Version)
Renders the dashboard directly using Jinja2 templates.
"""

from flask import Flask, request, render_template, redirect, url_for
from werkzeug.utils import secure_filename
import os
from pathlib import Path
from data_processor import get_processor, refresh_data

app = Flask(__name__, template_folder='.')  # Look for templates in current dir
app.secret_key = 'crew-dashboard-secret'  # Required for sessions if needed

# Configuration
UPLOAD_FOLDER = Path(__file__).parent / 'uploads'
UPLOAD_FOLDER.mkdir(exist_ok=True)
ALLOWED_EXTENSIONS = {'csv'}

app.config['UPLOAD_FOLDER'] = str(UPLOAD_FOLDER)
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET'])
def index():
    """Render the dashboard with data"""
    processor = get_processor()
    
    # Get optional date filter from query parameter
    filter_date = request.args.get('date', None)
    
    # Get data
    data = processor.get_dashboard_data(filter_date)
    
    # Render template with data
    return render_template('crew_dashboard.html', data=data, filter_date=filter_date)

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads via standard HTML Form"""
    
    # Map form field names to filenames
    file_map = {
        'dayrep': 'DayRepReport.csv',
        'sacutil': 'SacutilReport.csv',
        'rolcrtot': 'RolCrTotReport.csv',
        'crew_schedule': 'CrewSchedule.csv'
    }
    
    uploaded_any = False
    
    for field_name, target_filename in file_map.items():
        if field_name in request.files:
            file = request.files[field_name]
            if file and file.filename and allowed_file(file.filename):
                filepath = os.path.join(app.config['UPLOAD_FOLDER'], target_filename)
                file.save(filepath)
                uploaded_any = True
    
    if uploaded_any:
        # Refresh data processor to load new files
        refresh_data()
    
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
        'daily_stats_for_filter_date': processor.crew_schedule_by_date.get(filter_date) if filter_date else 'No Date'
    }

if __name__ == '__main__':
    # Initialize processor on startup
    get_processor()
    print("============================================================")
    print("Crew Management Dashboard (SSR Version)")
    print("============================================================")
    print("")
    print("Starting server on port 5000...")
    print("Dashboard: http://localhost:5000")
    print("")
    print("Press Ctrl+C to stop")
    print("============================================================")
    app.run(host='0.0.0.0', port=5000, debug=True)

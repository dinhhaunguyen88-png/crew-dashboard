"""
Vercel Serverless Function Handler for Crew Management Dashboard
Minimal Debug Version
"""

from flask import Flask, request, render_template_string, jsonify
import os
import sys
import traceback

# Add the parent directory to sys.path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, root_dir)

# Initialize Flask
app = Flask(__name__)
app.secret_key = 'crew-dashboard-secret-key-2026'

# Debug info
DEBUG_INFO = {
    'root_dir': root_dir,
    'python_version': sys.version,
    'modules_loaded': False,
    'processor_loaded': False,
    'supabase_loaded': False,
    'template_found': False,
    'errors': []
}

# Check template exists
template_path = os.path.join(root_dir, 'crew_dashboard.html')
DEBUG_INFO['template_path'] = template_path
DEBUG_INFO['template_found'] = os.path.exists(template_path)

# Try to list files in root_dir
try:
    DEBUG_INFO['files_in_root'] = os.listdir(root_dir)[:20]  # First 20 files
except Exception as e:
    DEBUG_INFO['files_in_root'] = f"Error: {e}"

# Try to import DataProcessor
try:
    from data_processor import DataProcessor
    DEBUG_INFO['processor_loaded'] = True
except Exception as e:
    DEBUG_INFO['errors'].append(f"DataProcessor import: {e}")

# Check environment variables
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
DEBUG_INFO['supabase_url_set'] = SUPABASE_URL is not None
DEBUG_INFO['supabase_key_set'] = SUPABASE_KEY is not None

# Try to import supabase_client only if env vars are set
if SUPABASE_URL and SUPABASE_KEY:
    try:
        import supabase_client
        DEBUG_INFO['supabase_loaded'] = True
    except Exception as e:
        DEBUG_INFO['errors'].append(f"supabase_client import: {e}")

DEBUG_INFO['modules_loaded'] = DEBUG_INFO['processor_loaded']


# Simple HTML template for testing
SIMPLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Crew Dashboard - Debug Mode</title>
    <style>
        body { font-family: Arial, sans-serif; background: #1a1a2e; color: #fff; padding: 20px; }
        .card { background: #2d2d44; padding: 20px; border-radius: 10px; margin: 10px 0; }
        .success { color: #22c55e; }
        .error { color: #ef4444; }
        .warning { color: #eab308; }
        pre { background: #0f0f1a; padding: 10px; border-radius: 5px; overflow-x: auto; }
        h1 { color: #3b82f6; }
    </style>
</head>
<body>
    <h1>✈️ Crew Dashboard - Debug Mode</h1>
    
    <div class="card">
        <h2>Status</h2>
        <p>Template Found: <span class="{{ 'success' if template_found else 'error' }}">{{ 'Yes' if template_found else 'No' }}</span></p>
        <p>DataProcessor: <span class="{{ 'success' if processor_loaded else 'error' }}">{{ 'Loaded' if processor_loaded else 'Failed' }}</span></p>
        <p>Supabase URL Set: <span class="{{ 'success' if supabase_url_set else 'warning' }}">{{ 'Yes' if supabase_url_set else 'No' }}</span></p>
        <p>Supabase Key Set: <span class="{{ 'success' if supabase_key_set else 'warning' }}">{{ 'Yes' if supabase_key_set else 'No' }}</span></p>
        <p>Supabase Client: <span class="{{ 'success' if supabase_loaded else 'warning' }}">{{ 'Loaded' if supabase_loaded else 'Not loaded' }}</span></p>
    </div>
    
    <div class="card">
        <h2>Errors</h2>
        {% if errors %}
        <ul class="error">
        {% for err in errors %}
            <li>{{ err }}</li>
        {% endfor %}
        </ul>
        {% else %}
        <p class="success">No errors!</p>
        {% endif %}
    </div>
    
    <div class="card">
        <h2>Files in Root Directory</h2>
        <pre>{{ files_in_root }}</pre>
    </div>
    
    <div class="card">
        <h2>Paths</h2>
        <p>Root Dir: {{ root_dir }}</p>
        <p>Template Path: {{ template_path }}</p>
        <p>Python: {{ python_version }}</p>
    </div>
    
    <div class="card">
        <h2>Next Steps</h2>
        <p>If this page loads successfully, the basic Flask app is working.</p>
        <p>Check the errors above to identify what needs to be fixed.</p>
        <p><a href="/api/status" style="color: #3b82f6;">Check API Status</a></p>
    </div>
</body>
</html>
"""


@app.route('/', methods=['GET'])
def index():
    """Debug index page"""
    try:
        return render_template_string(SIMPLE_HTML, **DEBUG_INFO)
    except Exception as e:
        return f"<h1>Error</h1><pre>{traceback.format_exc()}</pre>", 500


@app.route('/api/status', methods=['GET'])
def api_status():
    """Return debug info as JSON"""
    return jsonify(DEBUG_INFO)


@app.route('/api/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({'status': 'ok'})


# Try to load the real dashboard
@app.route('/dashboard', methods=['GET'])
def dashboard():
    """Try to load the real dashboard template"""
    try:
        # Re-configure Flask with template folder
        app.template_folder = root_dir
        
        # Get default data
        data = {
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
        
        from flask import render_template
        return render_template('crew_dashboard.html', data=data, filter_date=None)
    except Exception as e:
        return f"<h1>Dashboard Error</h1><pre>{traceback.format_exc()}</pre>", 500


# Vercel handler
handler = app

if __name__ == '__main__':
    app.run(debug=True, port=5000)

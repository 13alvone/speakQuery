# Standard library imports
import os
import re
import time
import uuid
import shutil
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor

# Third-party imports
import requests
import pandas as pd
import antlr4
from flask import Flask, request, jsonify, render_template
from flask import g  # For application-level database connection management
from werkzeug.utils import secure_filename
from croniter import croniter

# Local application imports
from lexers.speakQueryLexer import speakQueryLexer
from lexers.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener
from handlers.JavaHandler import JavaHandler
from validation.SavedSearchValidation import SavedSearchValidation
from functionality.FindNextCron import suggest_next_cron_runtime
from scheduled_input_engine.ScheduledInputEngine import (
    ScheduledInputBackend,
    crank_scheduled_input_engine,
)
from query_engine.QueryEngine import crank_query_engine
from scheduled_input_engine.SIExecution import SIExecution

app = Flask(
    __name__,
    template_folder='frontend/templates',
    static_folder='frontend/static'
)

# Configuration
app.config['UPLOAD_FOLDER'] = 'lookups'
app.config['ALLOWED_EXTENSIONS'] = {'sqlite3', 'system4.system4.parquet', 'csv', 'json'}
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB upload limit
app.config['TEMP_DIR'] = os.path.join('frontend', 'static', 'temp')
app.config['LOOKUP_DIR'] = 'lookups'
app.config['LOADJOB_DIR'] = os.path.join('frontend', 'static', 'temp')
app.config['INDEXES_DIR'] = 'indexes'
app.config['SAVED_SEARCHES_DB'] = 'saved_searches.db'
app.config['SCHEDULED_INPUTS_DB'] = 'scheduled_inputs.db'
app.config['LOG_LEVEL'] = logging.DEBUG
app.config['HISTORY_DB'] = 'history.db'
app.config['SCRIPT_DIR'] = os.getcwd()

# Initialize necessary components
scheduled_input_backend = ScheduledInputBackend()
java_handler = JavaHandler()
validator = SavedSearchValidation()

# Pre-compiled regex patterns
UUID_REGEX = re.compile(
    r'^[0-9]{10}\.[0-9]{6,7}_[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$'
)

# Configure logging
logging.basicConfig(level=app.config['LOG_LEVEL'], format='[%(levelname)s] %(message)s')

# Ensure temp directory exists
os.makedirs(app.config['TEMP_DIR'], exist_ok=True)


def allowed_file(filename):
    """Check if the file has an allowed extension, supporting multi-dot extensions."""
    filename_lower = filename.lower()
    for ext in app.config['ALLOWED_EXTENSIONS']:
        ext_lower = ext.lower()
        if filename_lower.endswith(f".{ext_lower}"):
            return True
    return False


def get_db(db_name):
    """Get a database connection, ensuring it's unique per request."""
    if 'db' not in g:
        g.db = {}
    if db_name not in g.db:
        g.db[db_name] = sqlite3.connect(db_name)
    return g.db[db_name]


@app.teardown_appcontext
def close_db(exception):
    """Close database connections at the end of the request."""
    db_dict = g.pop('db', {})
    # logging.info(f'[i] close_db() Function Exception: {exception}')
    for db_conn in db_dict.values():
        db_conn.close()


def load_config():
    conn = sqlite3.connect(app.config['SCHEDULED_INPUTS_DB'])  # Adjust if different
    cursor = conn.cursor()
    cursor.execute('SELECT key, value FROM app_settings')
    _settings = cursor.fetchall()
    for key, value in _settings:
        # Attempt to convert numerical settings to integers if applicable
        if value.isdigit():
            app.config[key] = int(value)
        else:
            app.config[key] = value
    conn.close()


def initialize_database():
    # Existing initialization for saved_searches
    conn = sqlite3.connect(app.config['SAVED_SEARCHES_DB'])
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS saved_searches (
            id TEXT PRIMARY KEY,
            title TEXT,
            description TEXT,
            query TEXT,
            cron_schedule TEXT,
            trigger TEXT,
            lookback TEXT,
            throttle TEXT,
            throttle_time_period TEXT,
            throttle_by TEXT,
            event_message TEXT,
            send_email TEXT,
            email_address TEXT,
            email_content TEXT,
            file_location TEXT
        )
    ''')
    conn.commit()
    conn.close()

    # Initialization for app_settings
    conn = sqlite3.connect(app.config['SCHEDULED_INPUTS_DB'])  # Assuming same DB; adjust if different
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    conn.commit()

    # Insert default settings if table is empty
    cursor.execute('SELECT COUNT(*) FROM app_settings')
    count = cursor.fetchone()[0]
    if count == 0:
        default_settings = {
            'UPLOAD_FOLDER': 'lookups',
            'ALLOWED_EXTENSIONS': 'sqlite3,system4.system4.parquet,csv,json',
            'MAX_CONTENT_LENGTH': '16777216',  # 16 MB in bytes
            'TEMP_DIR': os.path.join('frontend', 'static', 'temp'),
            'LOOKUP_DIR': 'lookups',
            'LOADJOB_DIR': os.path.join('frontend', 'static', 'temp'),
            'INDEXES_DIR': 'indexes',
            'SAVED_SEARCHES_DB': 'saved_searches.db',
            'SCHEDULED_INPUTS_DB': 'scheduled_inputs.db',
            'HISTORY_DB': 'history.db',  # Added HISTORY_DB to default settings
            'LOG_LEVEL': 'DEBUG',
            'KEEP_LATEST_FILES': '20'
        }
        for key, value in default_settings.items():
            cursor.execute('INSERT INTO app_settings (key, value) VALUES (?, ?)', (key, value))
        conn.commit()
    conn.close()

    # Initialize history.db and create history table
    history_db_path = app.config.get('HISTORY_DB', 'history.db')  # Fallback to 'history.db' if not set
    conn = sqlite3.connect(history_db_path)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS history (
            query_id TEXT PRIMARY KEY,
            query TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()


def load_settings_into_config():
    conn = sqlite3.connect(app.config['SCHEDULED_INPUTS_DB'])  # Assuming same DB; adjust if different
    cursor = conn.cursor()
    cursor.execute('SELECT key, value FROM app_settings')
    _settings = cursor.fetchall()
    conn.close()

    for key, value in _settings:
        # Convert types based on expected setting
        if key == 'ALLOWED_EXTENSIONS':
            app.config[key] = set(ext.strip().lower() for ext in value.split(','))
        elif key == 'MAX_CONTENT_LENGTH':
            app.config[key] = int(value)
        elif key in ['KEEP_LATEST_FILES']:
            app.config[key] = int(value)
        else:
            app.config[key] = value


def delete_old_files(directory_path=None, keep_latest=20):
    """
    Deletes all but the most recent 'keep_latest' files in the specified directory.
    """
    if directory_path is None:
        directory_path = app.config['TEMP_DIR']

    try:
        # Get list of all files in the directory sorted by modification time
        files = sorted(
            Path(directory_path).glob('*'),
            key=os.path.getmtime,
            reverse=True
        )

        # Determine the files to delete (all but the most recent 'keep_latest')
        files_to_delete = files[keep_latest:]

        # Delete the older files
        for file_path in files_to_delete:
            try:
                if file_path.is_file():
                    os.remove(file_path)
                    logging.info(f"Deleted file: {file_path}")
                elif file_path.is_dir():
                    shutil.rmtree(file_path)
                    logging.info(f"Deleted directory: {file_path}")
            except Exception as e:
                logging.error(f"Error deleting {file_path}: {str(e)}")

    except Exception as e:
        logging.error(f"Error processing directory {directory_path}: {str(e)}")


def start_background_engines():
    # Note: Consider using a task queue like Celery for production environments
    with ProcessPoolExecutor() as executor:
        executor.submit(crank_scheduled_input_engine)
        executor.submit(crank_query_engine)


def get_next_runtime(cron_schedule):
    """
    Given a cron job schedule string, return the next expected runtime based on GMT.
    """
    try:
        current_time = datetime.now(timezone.utc)  # Get the current time in GMT
        cron_iter = croniter(cron_schedule, current_time)
        next_runtime = cron_iter.get_next(datetime).astimezone(timezone.utc)
        logging.info(f"Next runtime for schedule '{cron_schedule}' is {next_runtime.isoformat()}")
        return next_runtime.isoformat()
    except Exception as e:
        logging.error(f"Failed to calculate next runtime: {str(e)}")
        return None


def is_title_unique(title):
    """
    Checks if the given title is unique in the 'saved_searches' table.
    """
    try:
        conn = get_db(app.config['SAVED_SEARCHES_DB'])
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM saved_searches WHERE title = ?", (title,))
        count = cursor.fetchone()[0]
        if count > 0:
            logging.info(f"Title '{title}' already exists in the database.")
            return False
        else:
            logging.info(f"Title '{title}' is unique.")
            return True
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {str(e)}")
        return False


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/lookups.html')
def lookups():
    return render_template('lookups.html')


@app.route('/history.html')
def history():
    return render_template('history.html')


@app.route('/settings.html')
def settings():
    return render_template('settings.html')


@app.route('/check_title_unique', methods=['POST'])
def check_title_unique():
    title = request.json.get('title')
    if title:
        is_unique = is_title_unique(title)
        return jsonify({'is_unique': is_unique})
    else:
        return jsonify({'error': 'No title provided'}), 400


@app.route('/fetch_api_data', methods=['POST'])
def fetch_api_data():
    data = request.get_json()
    api_url = data.get('api_url')

    if not api_url:
        return jsonify({'status': 'error', 'message': 'API URL is required.'}), 400

    try:
        # Validate the URL (you may want to restrict allowed domains)
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        api_data = response.json()
        return jsonify({'status': 'success', 'api_data': api_data}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400


@app.route('/toggle_disable_scheduled_input/<int:input_id>', methods=['POST'])
def toggle_disable_scheduled_input(input_id):
    try:
        conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
        cursor = conn.cursor()
        # Fetch current disabled status
        cursor.execute('SELECT disabled FROM scheduled_inputs WHERE id = ?', (input_id,))
        result = cursor.fetchone()
        if result is None:
            return jsonify({'status': 'error', 'message': 'Scheduled input not found.'}), 404
        current_disabled = result[0]
        new_disabled = 0 if current_disabled else 1
        cursor.execute('UPDATE scheduled_inputs SET disabled = ? WHERE id = ?', (new_disabled, input_id))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success', 'new_disabled': new_disabled})
    except Exception as e:
        logging.error(f"Error toggling disable for input {input_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to toggle disable status.'}), 500


@app.route('/delete_scheduled_input/<int:input_id>', methods=['POST'])
def delete_scheduled_input(input_id):
    try:
        conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
        cursor = conn.cursor()
        cursor.execute('DELETE FROM scheduled_inputs WHERE id = ?', (input_id,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error deleting scheduled input {input_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to delete scheduled input.'}), 500


@app.route('/clone_scheduled_input/<int:input_id>', methods=['POST'])
def clone_scheduled_input(input_id):
    try:
        conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
        cursor = conn.cursor()
        # Fetch the input to be cloned
        cursor.execute('SELECT title, description, code, cron_schedule, overwrite, '
                       'subdirectory FROM scheduled_inputs WHERE id = ?', (input_id,))
        result = cursor.fetchone()
        if result is None:
            return jsonify({'status': 'error', 'message': 'Scheduled input not found.'}), 404
        title, description, code, cron_schedule, overwrite, subdirectory = result
        # Create a new scheduled input with similar data
        cursor.execute('''
            INSERT INTO scheduled_inputs (title, description, code, cron_schedule, overwrite, subdirectory, created_at, disabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (f"{title} (Clone)", description, code, cron_schedule, overwrite, subdirectory, int(time.time()), 0))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error cloning scheduled input {input_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to clone scheduled input.'}), 500


@app.route('/toggle_disable_search/<search_id>', methods=['POST'])
def toggle_disable_search(search_id):
    try:
        query = 'SELECT disabled FROM saved_searches WHERE id = ?'
        current_status = execute_sql_query(app.config['SAVED_SEARCHES_DB'], query, (search_id,))
        if current_status:
            new_status = 0 if current_status[0][0] == 1 else 1
            query = 'UPDATE saved_searches SET disabled = ? WHERE id = ?'
            execute_sql_query(app.config['SAVED_SEARCHES_DB'], query, (new_status, search_id))
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'error', 'message': 'Saved search not found'}), 404
    except Exception as e:
        logging.error(f"Error toggling disabled status for saved search: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to toggle disabled status'}), 500


@app.route('/delete_search/<search_id>', methods=['POST'])
def delete_search(search_id):
    try:
        conn = sqlite3.connect(app.config['SAVED_SEARCHES_DB'])
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM saved_searches WHERE id = ?', (search_id,))
        search = cursor.fetchone()
        if not search:
            return jsonify({'status': 'error', 'message': 'Saved search not found.'}), 404

        cursor.execute('DELETE FROM saved_searches WHERE id = ?', (search_id,))
        conn.commit()
        conn.close()

        logging.info(f"Deleted saved search with ID {search_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error deleting saved search: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to delete saved search'}), 500


@app.route('/clone_search/<search_id>', methods=['POST'])
def clone_search(search_id):
    try:
        query = '''
        SELECT title, description, query, cron_schedule, trigger, lookback, throttle, throttle_time_period, throttle_by, 
        event_message, send_email, email_address, email_content
        FROM saved_searches WHERE id = ?
        '''
        search_data = execute_sql_query(app.config['SAVED_SEARCHES_DB'], query, (search_id,))
        if search_data:
            title = search_data[0][0] + '_clone'
            query = '''
            INSERT INTO saved_searches (
                id, title, description, query, cron_schedule, trigger, lookback, 
                throttle, throttle_time_period, throttle_by, event_message,
                send_email, email_address, email_content, disabled
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            '''
            new_id = f"{time.time()}_{str(uuid.uuid4())}"
            execute_sql_query(
                app.config['SAVED_SEARCHES_DB'],
                query,
                (new_id, title, *search_data[0][1:])
            )
            return jsonify({'status': 'success'})
        else:
            return jsonify({'status': 'error', 'message': 'Saved search not found'}), 404
    except Exception as e:
        logging.error(f"Error cloning saved search: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to clone saved search'}), 500


@app.route('/test_scheduled_input', methods=['POST'])
def test_scheduled_input():
    data = request.get_json()
    code = data.get('code')

    if not code:
        return jsonify({'status': 'error', 'message': 'No code provided.'}), 400

    try:
        # Create an instance of SIExecution with test_mode=True
        executor = SIExecution(code, test_mode=True)

        # Execute the code in test mode
        df_summary = executor.execute_code_test()

        return jsonify({'status': 'success', 'df_summary': df_summary}), 200
    except Exception as e:
        error_message = str(e)
        return jsonify({'status': 'error', 'message': error_message}), 400


@app.route('/save_search.html', methods=['GET', 'POST'])
def saved_search():
    try:
        if request.method == 'POST':
            request_id = request.form.get('request_id', '')
            saved_query = request.form.get('saved_query', '')
        else:
            request_id = request.args.get('request_id', '')
            saved_query = request.args.get('saved_query', '')
        cron_schedule = suggest_next_cron_runtime()
        return render_template(
            'save_search.html',
            saved_query=saved_query,
            request_id=request_id,
            cron_schedule=cron_schedule
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        return render_template('save_search.html', saved_query=""), 500


@app.route('/saved_searches.html')
def saved_searches():
    return render_template('saved_searches.html')


# Renamed the function to avoid conflict
@app.route('/schedule_input.html')
def scheduled_input_html():
    return render_template('schedule_input.html')


# Consolidated the route to handle both GET and POST
@app.route('/scheduled_input/<scheduled_input_id>', methods=['GET', 'POST'])
def scheduled_input(scheduled_input_id):
    conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
    cursor = conn.cursor()

    if request.method == 'GET':
        cursor.execute('''
            SELECT title, id, description, code, cron_schedule, subdirectory, created_at, overwrite, 
                   disabled 
            FROM scheduled_inputs
            WHERE id = ?
        ''', (scheduled_input_id,))

        result = cursor.fetchone()

        if result is None:
            return jsonify({'status': 'error', 'message': f'No saved search found with ID {scheduled_input_id}'}), 404

        search = {
            "title": result[0],
            "id": result[1],
            "description": result[2],
            "code": result[3],
            "cron_schedule": result[4],
            "subdirectory": result[5],
            "created_at": result[6],
            "overwrite": result[7],
            "disabled": result[8]
        }

        # Pre-process the data to determine the selected options
        disabled_checked = 'checked' if search['disabled'] else ''

        return render_template(
            'schedule_input.html',
            title=search['title'],
            description=search['description'],
            code=search['code'],
            cron_schedule=search['cron_schedule'],
            overwrite=search['overwrite'],
            subdirectory=search['subdirectory'],
            created_at=search['created_at'],
            disabled=disabled_checked,
            scheduled_input_id=search['id']  # Pass the ID to the template if needed
        )

    elif request.method == 'POST':
        # Handle POST request: Process form data
        data = request.form or request.get_json()

        if not data:
            return jsonify({'status': 'error', 'message': 'No data provided.'}), 400

        title = data.get('title')
        description = data.get('description')
        code = data.get('code')
        cron_schedule = data.get('cron_schedule')
        overwrite = data.get('overwrite') == 'true'
        subdirectory = data.get('subdirectory')
        disabled = data.get('disabled') == 'true'

        # Validate required fields
        if not title or not code or not cron_schedule:
            return jsonify({'status': 'error', 'message': 'Title, Code, and Cron Schedule are required.'}), 400

        # Update the scheduled input in the database
        cursor.execute('''
            UPDATE scheduled_inputs
            SET title = ?, description = ?, code = ?, cron_schedule = ?, subdirectory = ?, overwrite = ?, disabled = ?
            WHERE id = ?
        ''', (title, description, code, cron_schedule, subdirectory, overwrite, disabled, scheduled_input_id))

        conn.commit()
        cursor.close()
        conn.close()

        return jsonify({'status': 'success', 'message': 'Scheduled input updated successfully.'}), 200


@app.route('/scheduled_inputs.html')
def scheduled_inputs():
    return render_template('scheduled_inputs.html')


@app.route('/run_query', methods=['POST'])
def run_query():

    # data = request.json

    data = request.get_json(force=True, silent=True)
    if data is None:
        logging.error("[x] No JSON payload detected in /run_query request.")
        return jsonify({'status': 'error', 'message': 'Expected JSON payload.'}), 400
    query_str = data.get('query', '')
    logging.info(f"[i] Query received: {query_str}")

    try:
        result_df = execute_speakQuery(data.get('query'))  # Process the query using your custom SPL language
        logging.info(f"Query result before processing: {result_df}")

        if result_df is None or result_df.empty:
            logging.warning("No data returned from query.")
            return jsonify({'status': 'error', 'message': 'No data returned from query.'})

        result_df = result_df.fillna('')

        logging.info(f"Query result after processing NaN values: {result_df}")

        column_names = result_df.columns.values.tolist()
        row_data = result_df.to_dict(orient='records')
        logging.debug(f"Column names: {column_names}.")
        logging.debug(f"Data: {row_data}.")

        # Store the result_df in a temporary location
        request_id = f'{time.time()}_{str(uuid.uuid4())}'
        # save_dataframe(request_id, sanitize_dataframe(result_df), data.get('query'))
        save_dataframe(request_id, result_df, data.get('query'))

        response = {
            'status': 'success',
            'results': row_data,
            'column_names': column_names,
            'request_id': request_id
        }
        logging.debug(f"Response: {response}")

        return jsonify(response)

    except Exception as e:
        logging.error(f"Error processing query: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/get_query_for_loadjob/<filename>', methods=['GET'])
def get_query_for_loadjob(filename):
    """
    Retrieve the original query associated with a given load job filename.

    Args:
        filename (str): The load job filename (e.g., '1727138492.564529_167a8a78-1b24-42a3-9634-82707d112423.pkl')

    Returns:
        JSON response containing the original query or an error message.
    """
    try:
        # Ensure the filename ends with '.pkl'
        if not filename.endswith('.pkl'):
            return jsonify({'error': 'Invalid filename format. Expected a .pkl file.'}), 400

        # Extract the request_id by removing the '.pkl' extension
        request_id = filename[:-4]

        # Connect to history.db
        conn = sqlite3.connect(app.config['HISTORY_DB'])
        cursor = conn.cursor()

        # Retrieve the query based on request_id
        cursor.execute('SELECT query FROM history WHERE query_id = ?', (request_id,))
        row = cursor.fetchone()
        conn.close()

        if row:
            original_query = row[0]
            return jsonify({'query': original_query}), 200
        else:
            return jsonify({'error': 'No query found for the given load job filename.'}), 404
    except Exception as e:
        logging.error(f"Error retrieving query for load job '{filename}': {str(e)}")
        return jsonify({'error': 'Internal server error.'}), 500


@app.route('/commit_saved_search', methods=['POST'])
def commit_saved_search():
    try:
        # Extract form data
        data = request.json

        # Validate the request_id to ensure it's a valid UUID
        request_id = data.get('request_id', '')
        if not validate_uuid(request_id):
            return jsonify({'status': 'error', 'message': 'Invalid request_id.'}), 400

        # Validate unique title
        title = validator.validate_utf8(data.get('title'))
        if not is_title_unique(title):
            return jsonify({'status': 'failed'}), 400

        # Validate input fields
        description = validator.validate_utf8(data.get('description'))
        query = data.get('query')
        cron_schedule = validator.validate_cron_schedule(data.get('cron_schedule'))
        trigger = validator.validate_trigger(data.get('trigger'))
        lookback = validator.validate_lookback(data.get('lookback'))
        throttle = validator.validate_boolean(data.get('throttle'))
        if throttle != 'no':
            throttle_time_period = validator.validate_throttle_time_period(data.get('throttle_time_period'))
            throttle_by = validator.validate_utf8(data.get('throttle_by'))
        else:
            throttle_time_period = 0
            throttle_by = "N/A"
        event_message = validator.validate_utf8(data.get('event_message'))
        send_email = validator.validate_boolean(data.get('send_email'))
        if send_email != 'no':
            email_address = validator.validate_email(data.get('email_address'))
            email_content = data.get('email_content')
        else:
            email_address = "N/A"
            email_content = "N/A"
        dest_file = app.config['TEMP_DIR']

        # Save to SQLite3
        conn = get_db(app.config['SAVED_SEARCHES_DB'])
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO saved_searches (
                id, title, description, query, cron_schedule, trigger, lookback, 
                throttle, throttle_time_period, throttle_by, event_message,
                send_email, email_address, email_content, file_location
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            request_id, title, description, query, cron_schedule, trigger, lookback,
            throttle, throttle_time_period, throttle_by, event_message,
            send_email, email_address, email_content, dest_file
        ))

        conn.commit()

        return jsonify({'status': 'success', 'message': f"Successfully saved search: '{title}'"}), 200

    except Exception as e:
        logging.error(f"Error committing saved search: {str(e)}")
        return jsonify({'status': 'error', 'message': f'{e}'}), 400


@app.route('/commit_scheduled_input', methods=['POST'])
def commit_scheduled_input():
    try:
        title = request.form['title']
        description = request.form.get('description', '').strip()
        code = request.form['code']
        cron_schedule = request.form['cron_schedule']
        overwrite = request.form['overwrite']
        subdirectory = request.form.get('subdirectory', '').strip() or None

        # Validate the cron schedule format
        if not croniter.is_valid(cron_schedule):
            raise ValueError("Invalid cron schedule format.")

        # Validate the code by executing it in test mode
        executor = SIExecution(code, test_mode=True)
        executor.execute_code_test()

        # Add scheduled input
        scheduled_input_backend.add_scheduled_input(
            title=title,
            description=description,
            code=code,
            cron_schedule=cron_schedule,
            overwrite=overwrite,
            subdirectory=subdirectory
        )

        return jsonify({'status': 'success'})
    except ValueError as ve:
        logging.error(f"Validation error: {str(ve)}")
        return jsonify({'status': 'error', 'message': str(ve)}), 400
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return jsonify({'status': 'error', 'message': 'An unexpected error occurred.'}), 500


@app.route('/create_saved_search', methods=['POST'])
def create_saved_search():
    try:
        data = request.get_json()

        # Validate the request_id to ensure it's a valid UUID
        request_id = data.get('request_id', '')
        if not validate_uuid(request_id):
            return jsonify({'status': 'error', 'message': 'Invalid request_id.'}), 400

        logging.info("Search saved successfully with request_id: %s", request_id)

        # Render the save_search.html template with the provided data
        return render_template('save_search.html', result=data)

    except ValueError as e:
        logging.error("ValueError: %s", str(e))
        return jsonify({'status': 'error', 'message': str(e)}), 400
    except Exception as e:
        logging.error("Exception: %s", str(e))
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/get_saved_searches', methods=['GET'])
def get_saved_searches():
    try:
        conn = sqlite3.connect(app.config['SAVED_SEARCHES_DB'])
        conn.row_factory = sqlite3.Row  # To access columns by name
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM saved_searches')
        rows = cursor.fetchall()
        _saved_searches = []
        for row in rows:
            next_runtime = get_next_runtime(row['cron_schedule'])
            _saved_search = {
                'id': row['id'],
                'title': row['title'],
                'description': row['description'],
                'cron_schedule': row['cron_schedule'],
                'trigger': row['trigger'],
                'lookback': row['lookback'],
                'owner': row['owner'],
                'execution_count': row['execution_count'],
                'disabled': bool(row['disabled']),
                'send_email': bool(row['send_email']),
                'next_scheduled_time': next_runtime if next_runtime else 'N/A'
            }
            _saved_searches.append(_saved_search)
        conn.close()
        return jsonify({'status': 'success', 'searches': _saved_searches})
    except Exception as e:
        logging.error(f"Error fetching saved searches: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to fetch saved searches.'}), 500


@app.route('/saved_search/<search_id>', methods=['GET'])
def get_saved_search(search_id):
    conn = get_db(app.config['SAVED_SEARCHES_DB'])
    cursor = conn.cursor()

    cursor.execute('''
        SELECT title, description, query, cron_schedule, trigger, lookback, 
               throttle, throttle_time_period, throttle_by, event_message, 
               send_email, email_address, email_content, disabled
        FROM saved_searches
        WHERE id = ?
    ''', (search_id,))

    result = cursor.fetchone()

    if result is None:
        return jsonify({'status': 'error', 'message': f'No saved search found with ID {search_id}'}), 404

    search = {
        "title": result[0],
        "description": result[1],
        "query": result[2],
        "cron_schedule": result[3],
        "trigger": result[4],
        "lookback": result[5],
        "throttle": result[6],
        "throttle_time_period": result[7],
        "throttle_by": result[8],
        "event_message": result[9],
        "send_email": result[10],
        "email_address": result[11],
        "email_content": result[12],
        "disabled": bool(result[13])  # Assuming 'disabled' is stored as INTEGER (0 or 1)
    }

    # Pre-process the data to determine the selected options
    trigger_options = {
        'once_selected': 'selected' if search['trigger'] == 'Once' else '',
        'per_result_selected': 'selected' if search['trigger'] == 'Per Result' else ''
    }
    throttle_options = {
        'yes_selected': 'selected' if search['throttle'] == 'Yes' else '',
        'no_selected': 'selected' if search['throttle'] == 'No' else ''
    }
    send_email_options = {
        'yes_selected': 'selected' if search['send_email'] == 'Yes' else '',
        'no_selected': 'selected' if search['send_email'] == 'No' else ''
    }
    disabled_checked = 'checked' if search['disabled'] else ''

    return render_template(
        'saved_search.html',
        search=search,
        trigger_options=trigger_options,
        throttle_options=throttle_options,
        send_email_options=send_email_options,
        disabled_checked=disabled_checked,  # Pass the checked status
        search_id=search_id  # Pass the search_id for use in JavaScript
    )


@app.route('/update_saved_search/<search_id>', methods=['POST'])
def update_saved_search(search_id):
    data = request.get_json()

    # Define required fields
    required_fields = [
        'title', 'description', 'query', 'cron_schedule', 'trigger',
        'lookback', 'throttle', 'throttle_time_period', 'throttle_by',
        'event_message', 'send_email', 'email_address', 'email_content', 'disabled'
    ]

    # Validate presence of all required fields
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        return jsonify({'status': 'error', 'message': f'Missing fields: {", ".join(missing_fields)}'}), 400

    # Optional: Add further validation (e.g., email format, cron syntax)

    try:
        conn = get_db(app.config['SAVED_SEARCHES_DB'])
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE saved_searches
            SET title = ?, description = ?, query = ?, cron_schedule = ?, trigger = ?, 
                lookback = ?, throttle = ?, throttle_time_period = ?, throttle_by = ?, 
                event_message = ?, send_email = ?, email_address = ?, email_content = ?, disabled = ?
            WHERE id = ?
        ''', (
            data['title'],
            data['description'],
            data['query'],
            data['cron_schedule'],
            data['trigger'],
            data['lookback'],
            data['throttle'],
            data['throttle_time_period'],
            data['throttle_by'],
            data['event_message'],
            data['send_email'],
            data['email_address'],
            data['email_content'],
            int(data['disabled']),  # Convert boolean to integer (1 or 0)
            search_id
        ))

        conn.commit()

        return jsonify({'status': 'success', 'message': 'Saved search updated successfully.'})

    except Exception as e:
        logging.error(f"Error updating saved search: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error.'}), 500


@app.route('/get_scheduled_inputs', methods=['GET'])
def get_scheduled_inputs():
    conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
    cursor = conn.cursor()

    cursor.execute('''
        SELECT id, title, description, code, cron_schedule, overwrite, subdirectory, created_at, disabled
        FROM scheduled_inputs
    ''')

    inputs = cursor.fetchall()

    input_list = []
    for input_item in inputs:
        input_list.append({
            'id': input_item[0],
            'title': input_item[1],
            'description': input_item[2],
            'code': input_item[3],
            'cron_schedule': input_item[4],
            'overwrite': input_item[5],
            'subdirectory': input_item[6],
            'created_at': input_item[7],
            'disabled': input_item[8],  # Ensure this is 1 or 0
            'status': 'Disabled' if input_item[8] else 'Enabled'  # Optional
        })

    return jsonify({'status': 'success', 'inputs': input_list})


@app.route('/run_scheduled_input/<int:input_id>', methods=['POST'])
def run_scheduled_input(input_id):
    conn = get_db(app.config['SCHEDULED_INPUTS_DB'])
    cursor = conn.cursor()

    cursor.execute('SELECT code FROM scheduled_inputs WHERE id = ?', (input_id,))
    input_code = cursor.fetchone()

    if input_code:
        # Execute the code in a safe, controlled environment
        try:
            executor = SIExecution(input_code[0], test_mode=True)
            executor.execute_code_test()
            return jsonify({'status': 'success'})
        except Exception as e:
            logging.error(f"Error executing scheduled input: {str(e)}")
            return jsonify({'status': 'error', 'message': str(e)}), 500
    else:
        return jsonify({'status': 'error', 'message': 'Scheduled input not found'}), 404


@app.route('/edit_scheduled_input/<int:input_id>', methods=['GET'])
def edit_scheduled_input(input_id):
    # Fetch the scheduled input from the database
    conn = sqlite3.connect('scheduled_inputs.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM scheduled_inputs WHERE Id = ?", (input_id,))
    row = cursor.fetchone()
    conn.close()

    if row:
        input_data = {
            'id': row[0],
            'title': row[1],
            'description': row[2],
            'code': row[3],
            'cron_schedule': row[4],
            'overwrite': row[5],
            'subdirectory': row[6],
            'created_at': row[7],
            'disabled': row[8]
        }
        overwrite_options = {
            'yes_selected': input_data['overwrite'] == 'Yes',
            'no_selected': input_data['overwrite'] == 'No'
        }
        disabled_checked = 'checked' if input_data['disabled'] else ''
        return render_template('scheduled_input.html',
                               input_id=input_data['id'],
                               input=input_data,
                               overwrite_options=overwrite_options,
                               disabled_checked=disabled_checked)
    else:
        return "Scheduled Input Not Found", 404


@app.route('/update_scheduled_input/<int:input_id>', methods=['POST'])
def update_scheduled_input(input_id):
    data = request.json
    try:
        conn = sqlite3.connect('scheduled_inputs.db')
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE scheduled_inputs
            SET title = ?,
                description = ?,
                code = ?,
                cron_schedule = ?,
                overwrite = ?,
                subdirectory = ?,
                disabled = ?
            WHERE Id = ?
        """, (
            data['title'],
            data['description'],
            data['code'],
            data['cron_schedule'],
            data['overwrite'],
            data['subdirectory'],
            int(data['disabled']),
            input_id
        ))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(e)
        return jsonify({'status': 'error', 'message': 'Failed to update scheduled input.'}), 500


@app.route('/get_directory_tree', methods=['GET'])
def get_directory_tree():
    root_dir = app.config['INDEXES_DIR']

    def get_tree(path):
        tree = {}
        for dir_path, _, filenames in os.walk(path):
            relative_dir_path = os.path.relpath(dir_path, root_dir)

            # Skip if the directory path includes 'archive'
            if 'archive' in relative_dir_path.split(os.sep):
                continue

            parquet_files = [f for f in filenames if f.endswith('.parquet')]
            if parquet_files:
                tree[relative_dir_path] = {'files': parquet_files}

        return tree

    directory_tree = {
        "status": "success",
        "tree": get_tree(root_dir)
    }
    return jsonify(directory_tree)


@app.route('/save_results', methods=['POST'])
def save_results():
    data = request.json
    request_id = data.get('request_id')
    save_type = data.get('save_type')
    format_type = data.get('format')
    start = data.get('start', 0)
    end = data.get('end', None)

    logging.info(f"Saving results: {format_type} - {save_type}")

    try:
        result_df = load_dataframe(request_id)
        if save_type == 'current_page':
            result_df = result_df.iloc[start:end]

        file_name = f"{round(time.time(), 6)}_{uuid.uuid4()}.{format_type}"
        file_path = os.path.join(app.config['TEMP_DIR'], file_name)

        if format_type == 'csv':
            result_df.to_csv(file_path, index=False)
        elif format_type == 'json':
            result_df.to_json(file_path, orient='records')

        return jsonify({'status': 'success', 'file_url': f'/static/temp/{file_name}', 'file_name': file_name})

    except Exception as e:
        logging.error(f"Error saving results: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/get_lookup_files', methods=['GET'])
def get_lookup_files():
    lookup_dir = app.config['LOOKUP_DIR']
    files = []

    for root, _, filenames in os.walk(lookup_dir):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            stats = os.stat(filepath)
            row_count = get_row_count(filepath)

            files.append({
                'filename': filename,
                'filepath': filepath,
                'created_at': datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'filesize': stats.st_size,
                'permissions': oct(stats.st_mode)[-3:],
                'row_count': row_count
            })

    return jsonify({'status': 'success', 'files': files})


@app.route('/get_loadjob_files', methods=['GET'])
def get_loadjob_files():
    loadjob_dir = app.config['LOADJOB_DIR']
    files = []

    for root, _, filenames in os.walk(loadjob_dir):
        for filename in filenames:
            filepath = os.path.join(root, filename)
            stats = os.stat(filepath)

            files.append({
                'filename': filename,
                'created_at': datetime.fromtimestamp(stats.st_ctime).strftime('%Y-%m-%d %H:%M:%S'),
                'updated_at': datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                'filesize': stats.st_size
            })

    return jsonify({'status': 'success', 'files': files})


@app.route('/upload_file', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file part in the request.'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected for uploading.'})

    if not allowed_file(file.filename):
        return jsonify({'status': 'error', 'message': 'File type not allowed.'})

    filename = secure_filename(file.filename)
    upload_dir = app.config['LOOKUP_DIR']
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, filename)

    try:
        file.save(file_path)
        return jsonify({'status': 'success', 'message': 'File uploaded successfully.'})
    except Exception as e:
        logging.error(f"Error saving file: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})


@app.route('/view_lookup', methods=['GET'])
def view_lookup():
    filepath = request.args.get('file')
    if not filepath:
        return "<p>Error: No file specified.</p>", 400
    filename = filepath.split('/')[-1]

    # Ensure the file is within the allowed directory
    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return "<p>Error: Invalid file path.</p>", 400

    # Prevent directory traversal attacks
    safe_filepath = os.path.normpath(filepath)
    target_filepath = \
        f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')
    if not safe_filepath.startswith(target_filepath):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403

    if not os.path.exists(safe_filepath):
        return "<p>Error: File not found.</p>", 404

    try:
        df = pd.read_csv(safe_filepath)
        return df.to_html()
    except Exception as e:
        logging.error(f"Error reading file: {str(e)}")
        return "<p>Error reading file.</p>", 500


@app.route('/delete_lookup_file', methods=['POST'])
def delete_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    filename = filepath.split('/')[-1]

    # Ensure the file is within the allowed directory
    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400

    # Prevent directory traversal attacks
    safe_filepath = os.path.normpath(filepath)
    target_filepath = \
        f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')
    if not safe_filepath.startswith(target_filepath):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403

    if not os.path.exists(safe_filepath):
        return jsonify({'status': 'error', 'message': 'File not found.'}), 404

    try:
        os.remove(safe_filepath)
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error deleting file: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/clone_lookup_file', methods=['POST'])
def clone_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    new_name = data.get('new_name')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    filename = filepath.split('/')[-1]

    # Ensure the file is within the allowed directory
    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400

    # Prevent directory traversal attacks
    safe_filepath = os.path.normpath(filepath)
    target_filepath = \
        f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')

    if not safe_filepath.startswith(target_filepath):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403

    if not os.path.exists(safe_filepath):
        return jsonify({'status': 'error', 'message': 'File not found.'}), 404

    if not new_name:
        base_name = os.path.basename(filepath)
        new_name = f"{os.path.splitext(base_name)[0]}_copy{os.path.splitext(base_name)[1]}"

    new_filename = secure_filename(new_name)
    new_filepath = os.path.join(os.path.dirname(safe_filepath), new_filename)

    try:
        shutil.copy(safe_filepath, new_filepath)
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error cloning file: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/get_settings', methods=['GET'])
def get_settings():
    try:
        conn = sqlite3.connect(app.config['SCHEDULED_INPUTS_DB'])  # Adjust if different
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM app_settings')
        _settings = cursor.fetchall()
        conn.close()

        settings_dict = {}
        for key, value in _settings:
            # Convert types based on key
            if key == 'ALLOWED_EXTENSIONS':
                settings_dict[key] = ','.join(app.config.get(key, []))
            elif key in ['MAX_CONTENT_LENGTH', 'KEEP_LATEST_FILES']:
                settings_dict[key] = int(app.config.get(key, 0))
            else:
                settings_dict[key] = app.config.get(key, '')

        return jsonify({'status': 'success', 'settings': settings_dict}), 200
    except Exception as e:
        logging.error(f"Error fetching settings: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to fetch settings.'}), 500


@app.route('/update_settings', methods=['POST'])
def update_settings():
    try:
        data = request.json
        if not data or 'settings' not in data:
            return jsonify({'status': 'error', 'message': 'No settings provided.'}), 400

        _settings = data['settings']
        conn = sqlite3.connect(app.config['SCHEDULED_INPUTS_DB'])  # Adjust if different
        cursor = conn.cursor()

        for key, value in _settings.items():
            # Type conversion based on key
            if key == 'ALLOWED_EXTENSIONS':
                # Expecting a comma-separated string
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (value, key))
                app.config[key] = set(ext.strip().lower() for ext in value.split(','))
            elif key == 'MAX_CONTENT_LENGTH':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(int(value)), key))
                app.config[key] = int(value)
            elif key == 'KEEP_LATEST_FILES':
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (str(int(value)), key))
                app.config[key] = int(value)
            else:
                cursor.execute('UPDATE app_settings SET value = ? WHERE key = ?', (value, key))
                app.config[key] = value

        conn.commit()
        conn.close()

        return jsonify({'status': 'success', 'message': 'Settings updated successfully.'}), 200

    except Exception as e:
        logging.error(f"Error updating settings: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to update settings.'}), 500


def get_row_count(filepath):
    try:
        df = pd.read_csv(filepath)
        return len(df)
    except Exception as e:
        logging.error(f"Error reading file for row count: {str(e)}")
        return 0


def sanitize_dataframe(df):
    """Convert java.lang.Long to native Python int or other serializable type."""
    for col in df.columns:
        if df[col].dtype.name == 'object':
            df[col] = df[col].apply(
                lambda x: int(x) if java_handler.is_java_long(x) else x
            )
    return df


def save_dataframe(request_id, df, query):
    """Save DataFrame to a pickle file securely and log the query history."""
    # Save the DataFrame as a pickle file
    file_path = os.path.join(app.config['TEMP_DIR'], f"{request_id}.pkl")
    df.to_pickle(file_path)

    # Save the query and query_id to history.db
    history_db_path = app.config.get('HISTORY_DB', 'history.db')  # Fallback to 'history.db' if not set
    try:
        with sqlite3.connect(history_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO history (query_id, query) VALUES (?, ?)
            ''', (request_id, query))
            conn.commit()
    except sqlite3.IntegrityError:
        # Handle the case where the query_id already exists
        with sqlite3.connect(history_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE history SET query = ?, timestamp = CURRENT_TIMESTAMP WHERE query_id = ?
            ''', (query, request_id))
            conn.commit()
    except Exception as e:
        logging.error(f"Error saving to history.db: {str(e)}")


def load_dataframe(request_id):
    file_path = os.path.join(app.config['TEMP_DIR'], f"{request_id}.pkl")
    if os.path.exists(file_path):
        return pd.read_pickle(file_path)
    else:
        raise FileNotFoundError(f"No saved DataFrame found for request_id: {request_id}")


def validate_uuid(request_id):
    try:
        if UUID_REGEX.match(request_id):
            return True
        return False
    except Exception as e:
        logging.error(f"UUID validation error: {str(e)}")
        return False


def execute_sql_query(db_path, query, params=()):
    """
    Executes an SQL query against the provided SQLite database.
    """
    try:
        conn = get_db(db_path)
        cursor = conn.cursor()
        cursor.execute(query, params)
        if query.strip().upper().startswith("SELECT"):
            result = cursor.fetchall()
        else:
            conn.commit()
            result = None
        return result
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        raise


def execute_speakQuery(speak_query: str):
    logging.info("Starting the parsing process.")
    if not isinstance(speak_query, str):
        raise ValueError("Query must be a string")

    input_stream = antlr4.InputStream(speak_query)
    lexer = speakQueryLexer(input_stream)
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()
    listener = speakQueryListener(speak_query)
    walker = antlr4.ParseTreeWalker()
    walker.walk(listener, tree)

    return listener.main_df


if __name__ == '__main__':
    delete_old_files()
    initialize_database()
    load_settings_into_config()  # Load settings into app.config
    # start_background_engines()  # Initialize Background Engines as Services
    app.run(host='0.0.0.0', debug=True)

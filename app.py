# Standard library imports
import os
import re
import time
import uuid
import shutil
import logging
import sqlite3
import sys
import hashlib
from werkzeug.security import generate_password_hash, check_password_hash
import argparse
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor
from queue import Full
from utils.task_queue import TaskQueue
from utils.file_utils import get_row_count as util_get_row_count, allowed_file as util_allowed_file

# Third-party imports
import requests
import pandas as pd
import antlr4
from flask import Flask, request, jsonify, render_template
from flask_wtf import CSRFProtect
from flask_login import (
    LoginManager,
    login_user,
    logout_user,
    current_user,
)
from utils.auth import login_required
from croniter import croniter
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# Local application imports
from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
try:
    from lexers.speakQueryListener import speakQueryListener
except ImportError as e:
    logging.error(f"[x] Failed to import speakQueryListener: {e}")
    sys.exit(1)
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

# Determine project root based on this file's location
PROJECT_ROOT = Path(__file__).resolve().parent

dotenv_path = PROJECT_ROOT / '.env'
if dotenv_path.exists():
    load_dotenv(dotenv_path)
    logging.info("[i] Loaded environment variables from .env")
else:
    logging.info("[i] No .env file found; using existing environment variables")

secret_key = os.environ.get('SECRET_KEY', 'insecure-default-key')
if secret_key == 'insecure-default-key':
    logging.warning(
        "[!] SECRET_KEY environment variable not set. "
        "Using insecure default for testing."
    )
app.config['SECRET_KEY'] = secret_key
csrf = CSRFProtect(app)
app.config['WTF_CSRF_ENABLED'] = True

# Authentication setup
login_manager = LoginManager()
login_manager.init_app(app)


class User:
    """Simple user model for authentication."""

    def __init__(self, id, username, role, api_token=None):
        self.id = str(id)
        self.username = username
        self.role = role
        self.api_token = api_token

    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return True

    def get_id(self):
        return self.id


# Rate limiting and task queue initialized with safe defaults
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100 per minute"],
    storage_uri="memory://",
    enabled=True,
)
limiter.init_app(app)
limiter.enabled = False

# Default queue before settings are loaded
app.config['TASK_QUEUE'] = TaskQueue(10, 2)

# Configuration
app.config['UPLOAD_FOLDER'] = str(PROJECT_ROOT / 'lookups')
app.config['ALLOWED_EXTENSIONS'] = {'sqlite3', 'system4.system4.parquet', 'csv', 'json'}
app.config['ALLOWED_API_DOMAINS'] = {'jsonplaceholder.typicode.com'}
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16 MB upload limit
app.config['TEMP_DIR'] = str(PROJECT_ROOT / 'frontend' / 'static' / 'temp')
app.config['LOOKUP_DIR'] = str(PROJECT_ROOT / 'lookups')
app.config['LOADJOB_DIR'] = str(PROJECT_ROOT / 'frontend' / 'static' / 'temp')
app.config['INDEXES_DIR'] = str(PROJECT_ROOT / 'indexes')
app.config['SAVED_SEARCHES_DB'] = str(PROJECT_ROOT / 'saved_searches.db')
app.config['SCHEDULED_INPUTS_DB'] = str(PROJECT_ROOT / 'scheduled_inputs.db')
app.config['LOG_LEVEL'] = logging.DEBUG
app.config['HISTORY_DB'] = str(PROJECT_ROOT / 'history.db')
app.config['SCRIPT_DIR'] = str(PROJECT_ROOT)

# Initialize necessary components
scheduled_input_backend = ScheduledInputBackend()
java_handler = JavaHandler()
validator = SavedSearchValidation()

# Pre-compiled regex patterns
UUID_REGEX = re.compile(
    r'^[0-9]{10}\.[0-9]{6,7}_[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$'
)


@login_manager.user_loader
def load_user(user_id):
    """Retrieve a user object from the database."""
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, username, role, api_token FROM users WHERE id = ?',
                (user_id,),
            )
            row = cursor.fetchone()
            if row:
                return User(id=row[0], username=row[1], role=row[2], api_token=row[3])
    except Exception as exc:
        logging.error(f"[x] Failed to load user {user_id}: {exc}")
    return None


@login_manager.request_loader
def load_user_from_request(request):
    """Authenticate via Authorization bearer token."""
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(None, 1)[1]
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, username, role, api_token FROM users WHERE api_token = ?',
                (token_hash,),
            )
            row = cursor.fetchone()
            if row:
                return User(id=row[0], username=row[1], role=row[2], api_token=row[3])
    except Exception as exc:
        logging.error(f"[x] Token auth failed: {exc}")
    return None


# Ensure temp directory exists
os.makedirs(app.config['TEMP_DIR'], exist_ok=True)
# Ensure indexes directory exists
os.makedirs(app.config['INDEXES_DIR'], exist_ok=True)


def allowed_file(filename):
    """Check if *filename* uses one of the extensions configured in ALLOWED_EXTENSIONS."""
    return util_allowed_file(filename, app.config['ALLOWED_EXTENSIONS'])


def is_allowed_api_url(api_url):
    """Return True if the api_url's domain matches ALLOWED_API_DOMAINS patterns."""
    try:
        parsed = requests.utils.urlparse(api_url)
        hostname = parsed.hostname or ''
        allowed_domains = app.config.get('ALLOWED_API_DOMAINS', set())
        for pattern in allowed_domains:
            if re.fullmatch(pattern, hostname):
                return True
        return False
    except Exception:
        return False


def initialize_database(admin_username=None, admin_password=None, admin_role='admin', admin_api_token=None):
    # Existing initialization for saved_searches
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
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
            file_location TEXT,
            owner_id INTEGER REFERENCES users(id)
        )
    ''')
        conn.commit()
        cursor.execute('PRAGMA table_info(saved_searches)')
        cols = [c[1] for c in cursor.fetchall()]
        if 'owner_id' not in cols:
            cursor.execute(
                'ALTER TABLE saved_searches ADD COLUMN owner_id INTEGER REFERENCES users(id)'
            )
            conn.commit()

        # Table to track invalid deletion attempts and bans
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_bans (
            ip TEXT PRIMARY KEY,
            count INTEGER DEFAULT 0,
            last_attempt INTEGER DEFAULT 0,
            banned_until INTEGER DEFAULT 0
        )
    ''')
        conn.commit()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS lookup_files (
            filename TEXT PRIMARY KEY,
            owner_id INTEGER REFERENCES users(id),
            created_at INTEGER
        )
    ''')
        conn.commit()

    # Initialization for app_settings
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        # Assuming same DB; adjust if different
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
        conn.commit()

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password_hash TEXT,
            role TEXT,
            api_token TEXT
        )
    ''')
        conn.commit()

        cursor.execute('SELECT COUNT(*) FROM users')
        users_count = cursor.fetchone()[0]
        if users_count == 0:
            username = admin_username or os.environ.get('ADMIN_USERNAME', 'admin')
            password = admin_password or os.environ.get('ADMIN_PASSWORD', 'admin')
            role = admin_role or os.environ.get('ADMIN_ROLE', 'admin')
            api_token = admin_api_token or os.environ.get('ADMIN_API_TOKEN', str(uuid.uuid4()))
            password_hash = generate_password_hash(password)
            token_hash = hashlib.sha256(api_token.encode()).hexdigest()
            cursor.execute(
                'INSERT INTO users (username, password_hash, role, api_token) VALUES (?, ?, ?, ?)',
                (username, password_hash, role, token_hash),
            )
            conn.commit()
            logging.info('[i] Inserted default admin user')

        # Insert default settings if table is empty
        cursor.execute('SELECT COUNT(*) FROM app_settings')
        count = cursor.fetchone()[0]
        default_settings = {
                'UPLOAD_FOLDER': str(PROJECT_ROOT / 'lookups'),
                'ALLOWED_EXTENSIONS': 'sqlite3,system4.system4.parquet,csv,json',
                'MAX_CONTENT_LENGTH': '16777216',  # 16 MB in bytes
                'TEMP_DIR': str(PROJECT_ROOT / 'frontend' / 'static' / 'temp'),
                'LOOKUP_DIR': str(PROJECT_ROOT / 'lookups'),
                'LOADJOB_DIR': str(PROJECT_ROOT / 'frontend' / 'static' / 'temp'),
                'INDEXES_DIR': str(PROJECT_ROOT / 'indexes'),
                'SAVED_SEARCHES_DB': str(PROJECT_ROOT / 'saved_searches.db'),
                'SCHEDULED_INPUTS_DB': str(PROJECT_ROOT / 'scheduled_inputs.db'),
                'HISTORY_DB': str(PROJECT_ROOT / 'history.db'),  # Added HISTORY_DB to default settings
                'LOG_LEVEL': 'DEBUG',
                'KEEP_LATEST_FILES': '20',
                'ALLOWED_API_DOMAINS': 'jsonplaceholder.typicode.com',
                'QUEUE_SIZE': '20',
                'PROCESSING_LIMIT': '5',
                'THROTTLE_ENABLED': 'true',
                'LOGIN_RATE_LIMIT': '5 per minute',
                'BAN_DELETIONS_ENABLED': 'false',
                'BAN_DURATION': '3600',
            }
        if count == 0:
            for key, value in default_settings.items():
                cursor.execute(
                    'INSERT INTO app_settings (key, value) VALUES (?, ?)',
                    (key, value)
                )
            conn.commit()
        else:
            cursor.execute('SELECT key FROM app_settings')
            existing_keys = {row[0] for row in cursor.fetchall()}
            for key, value in default_settings.items():
                if key not in existing_keys:
                    cursor.execute(
                        'INSERT INTO app_settings (key, value) VALUES (?, ?)',
                        (key, value)
                    )
            conn.commit()

    # Initialize history.db and create history table
    history_db_path = app.config.get('HISTORY_DB', 'history.db')
    # Fallback to 'history.db' if not set
    with sqlite3.connect(history_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS history (
            query_id TEXT PRIMARY KEY,
            query TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
        conn.commit()


def load_settings_into_config():
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        # Assuming same DB; adjust if different
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM app_settings')
        _settings = cursor.fetchall()

    for key, value in _settings:
        # Convert types based on expected setting
        if key == 'ALLOWED_EXTENSIONS':
            app.config[key] = set(ext.strip().lower() for ext in value.split(','))
        elif key == 'ALLOWED_API_DOMAINS':
            app.config[key] = set(d.strip() for d in value.split(',') if d.strip())
        elif key == 'MAX_CONTENT_LENGTH':
            app.config[key] = int(value)
        elif key in ['KEEP_LATEST_FILES', 'QUEUE_SIZE', 'PROCESSING_LIMIT']:
            app.config[key] = int(value)
        elif key == 'THROTTLE_ENABLED':
            app.config[key] = str(value).lower() in {'true', '1', 'yes'}
        elif key == 'BAN_DELETIONS_ENABLED':
            app.config[key] = str(value).lower() in {'true', '1', 'yes'}
        elif key == 'BAN_DURATION':
            app.config[key] = int(value)
        elif key == 'LOGIN_RATE_LIMIT':
            app.config[key] = value
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
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
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


@csrf.exempt
@app.route('/login', methods=['POST'])
@limiter.limit(lambda: app.config.get('LOGIN_RATE_LIMIT', '5 per minute'))
def login():
    """Authenticate a user and start a session."""
    data = request.get_json() or request.form
    username = data.get('username') if data else None
    password = data.get('password') if data else None
    if not username or not password:
        return jsonify({'status': 'error', 'message': 'Missing credentials'}), 400
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT id, password_hash, role, api_token FROM users WHERE username = ?',
                (username,),
            )
            row = cursor.fetchone()
            if row and check_password_hash(row[1], password):
                user = User(id=row[0], username=username, role=row[2], api_token=row[3])
                login_user(user)
                return jsonify({'status': 'success'})
    except Exception as exc:
        if isinstance(exc, sqlite3.Error) and 'no such table' in str(exc):
            logging.error(
                "[x] Users table missing during login. Initialize the database first"
            )
            return (
                jsonify(
                    {
                        'status': 'error',
                        'message': 'Database uninitialized. Run create-admin to set up.'
                    }
                ),
                500,
            )
        logging.error(f"[x] Login failed for {username}: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal server error'}), 500
    return jsonify({'status': 'error', 'message': 'Invalid credentials'}), 401


@app.route('/logout')
@login_required
def logout():
    """Log out the current user."""
    logout_user()
    return jsonify({'status': 'success'})


@app.route('/lookups.html')
def lookups():
    return render_template('lookups.html')


@app.route('/history.html')
def history():
    return render_template('history.html')


@app.route('/settings.html')
def settings():
    return render_template('settings.html')


# Blueprint routes moved to routes/query.py


@app.route('/toggle_disable_scheduled_input/<int:input_id>', methods=['POST'])
def toggle_disable_scheduled_input(input_id):
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            # Fetch current disabled status
            cursor.execute('SELECT disabled FROM scheduled_inputs WHERE id = ?', (input_id,))
            result = cursor.fetchone()
            if result is None:
                return jsonify({'status': 'error', 'message': 'Scheduled input not found.'}), 404
            current_disabled = result[0]
            new_disabled = 0 if current_disabled else 1
            cursor.execute(
                'UPDATE scheduled_inputs SET disabled = ? WHERE id = ?',
                (new_disabled, input_id)
            )
            conn.commit()
        return jsonify({'status': 'success', 'new_disabled': new_disabled})
    except Exception as e:
        logging.error(f"Error toggling disable for input {input_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to toggle disable status.'}), 500


@app.route('/delete_scheduled_input/<int:input_id>', methods=['POST'])
def delete_scheduled_input(input_id):
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM scheduled_inputs WHERE id = ?', (input_id,))
            conn.commit()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error deleting scheduled input {input_id}: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to delete scheduled input.'}), 500


@app.route('/clone_scheduled_input/<int:input_id>', methods=['POST'])
def clone_scheduled_input(input_id):
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            # Fetch the input to be cloned
            cursor.execute(
                'SELECT title, description, code, cron_schedule, overwrite, '
                'subdirectory FROM scheduled_inputs WHERE id = ?',
                (input_id,)
            )
            result = cursor.fetchone()
            if result is None:
                return jsonify({'status': 'error', 'message': 'Scheduled input not found.'}), 404
            title, description, code, cron_schedule, overwrite, subdirectory = result
            # Create a new scheduled input with similar data
            cursor.execute(
                '''
                INSERT INTO scheduled_inputs (
                    title, description, code, cron_schedule,
                    overwrite, subdirectory, created_at, disabled
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    f"{title} (Clone)",
                    description,
                    code,
                    cron_schedule,
                    overwrite,
                    subdirectory,
                    int(time.time()),
                    0,
                ),
            )
            conn.commit()
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
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM saved_searches WHERE id = ?', (search_id,))
            search = cursor.fetchone()
            if not search:
                return jsonify({'status': 'error', 'message': 'Saved search not found.'}), 404

            cursor.execute('DELETE FROM saved_searches WHERE id = ?', (search_id,))
            conn.commit()

        logging.info(f"Deleted saved search with ID {search_id}")
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(f"Error deleting saved search: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to delete saved search'}), 500


@app.route('/clone_search/<search_id>', methods=['POST'])
def clone_search(search_id):
    try:
        query = '''
        SELECT title, description, query, cron_schedule, trigger, lookback,
               throttle, throttle_time_period, throttle_by,
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
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()

        if request.method == 'GET':
            cursor.execute(
                '''
                SELECT title, id, description, code, cron_schedule, subdirectory,
                       created_at, overwrite,
                       disabled
                FROM scheduled_inputs
                WHERE id = ?
                ''',
                (scheduled_input_id,)
            )

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
                scheduled_input_id=search['id']
            )

        elif request.method == 'POST':
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

            if not title or not code or not cron_schedule:
                return jsonify(
                    {
                        'status': 'error',
                        'message': 'Title, Code, and Cron Schedule are required.'
                    }
                ), 400

            cursor.execute(
                '''
                UPDATE scheduled_inputs
                SET title = ?, description = ?, code = ?, cron_schedule = ?,
                    subdirectory = ?, overwrite = ?, disabled = ?
                WHERE id = ?
                ''',
                (
                    title,
                    description,
                    code,
                    cron_schedule,
                    subdirectory,
                    overwrite,
                    disabled,
                    scheduled_input_id,
                ),
            )

            conn.commit()

            return jsonify({'status': 'success', 'message': 'Scheduled input updated successfully.'}), 200


@app.route('/scheduled_inputs.html')
def scheduled_inputs():
    return render_template('scheduled_inputs.html')


# Query routes moved to routes/query.py


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
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
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
@login_required
def commit_scheduled_input():
    try:
        data = request.get_json(silent=True)
        if data:
            title = data.get('title')
            description = data.get('description', '').strip()
            code = data.get('code')
            cron_schedule = data.get('cron_schedule')
            overwrite = data.get('overwrite')
            subdirectory = data.get('subdirectory', '').strip() or None
            api_url = data.get('api_url')
        else:
            title = request.form['title']
            description = request.form.get('description', '').strip()
            code = request.form['code']
            cron_schedule = request.form['cron_schedule']
            overwrite = request.form['overwrite']
            subdirectory = request.form.get('subdirectory', '').strip() or None
            api_url = request.form.get('api_url')

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
            subdirectory=subdirectory,
            api_url=api_url,
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
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
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
        return jsonify({'status': 'success', 'searches': _saved_searches})
    except Exception as e:
        logging.error(f"Error fetching saved searches: {str(e)}")
        return jsonify({'status': 'error', 'message': 'Failed to fetch saved searches.'}), 500


@app.route('/saved_search/<search_id>', methods=['GET'])
def get_saved_search(search_id):
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        cursor = conn.cursor()

        cursor.execute(
            '''
            SELECT title, description, query, cron_schedule, trigger, lookback,
                   throttle, throttle_time_period, throttle_by, event_message,
                   send_email, email_address, email_content, disabled
            FROM saved_searches
            WHERE id = ?
            ''',
            (search_id,)
        )

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
            disabled_checked=disabled_checked,
            search_id=search_id
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
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()

        cursor.execute(
            '''
            UPDATE saved_searches
            SET title = ?, description = ?, query = ?, cron_schedule = ?, trigger = ?,
                lookback = ?, throttle = ?, throttle_time_period = ?, throttle_by = ?,
                event_message = ?, send_email = ?, email_address = ?, email_content = ?, disabled = ?
            WHERE id = ?
            ''',
            (
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
                search_id,
            ),
        )

        conn.commit()

        return jsonify({'status': 'success', 'message': 'Saved search updated successfully.'})

    except Exception as e:
        logging.error(f"Error updating saved search: {e}")
        return jsonify({'status': 'error', 'message': 'Internal server error.'}), 500


@app.route('/get_scheduled_inputs', methods=['GET'])
def get_scheduled_inputs():
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
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
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT code FROM scheduled_inputs WHERE id = ?', (input_id,))
        input_code = cursor.fetchone()

        if input_code:
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
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM scheduled_inputs WHERE Id = ?", (input_id,))
        row = cursor.fetchone()

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
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE scheduled_inputs
                SET title = ?,
                    description = ?,
                    code = ?,
                    cron_schedule = ?,
                    overwrite = ?,
                    subdirectory = ?,
                    disabled = ?
                WHERE Id = ?
                """,
                (
                    data['title'],
                    data['description'],
                    data['code'],
                    data['cron_schedule'],
                    data['overwrite'],
                    data['subdirectory'],
                    int(data['disabled']),
                    input_id,
                ),
            )
            conn.commit()
            return jsonify({'status': 'success'})
    except Exception as e:
        logging.error(e)
        return jsonify({'status': 'error', 'message': 'Failed to update scheduled input.'}), 500


@app.route('/get_directory_tree', methods=['GET'])
def get_directory_tree():
    """Return a nested representation of the indexes directory."""

    root_dir = app.config['INDEXES_DIR']

    if not os.path.isdir(root_dir):
        logging.error(f"[x] Indexes directory not found: {root_dir}")
        return jsonify({'status': 'error', 'message': 'Indexes directory not found'}), 404

    def build_tree(current_path):
        tree = {"dirs": {}, "files": []}

        for entry in sorted(os.listdir(current_path)):
            if entry == 'archive':
                continue

            full_path = os.path.join(current_path, entry)
            if os.path.isdir(full_path):
                subtree = build_tree(full_path)
                tree["dirs"][entry] = subtree
            else:
                rel_path = os.path.relpath(full_path, root_dir)
                tree["files"].append({"name": entry, "path": rel_path})

        return tree

    directory_tree = {
        "status": "success",
        "tree": build_tree(root_dir)
    }
    return jsonify(directory_tree)


# Query route moved to routes/query.py


# Lookup routes moved to routes/lookups.py


# Settings routes moved to routes/settings.py


def get_row_count(filepath):
    """Return the number of rows in ``filepath`` using :mod:`utils.file_utils`."""
    return util_get_row_count(filepath)


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
        with sqlite3.connect(db_path) as conn:
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


# Register blueprints
from routes.query import query_bp
from routes.lookups import lookups_bp
from routes.settings import settings_bp
from routes.api import api_bp

csrf.exempt(query_bp)
csrf.exempt(lookups_bp)
csrf.exempt(settings_bp)
csrf.exempt(api_bp)

app.register_blueprint(query_bp)
app.register_blueprint(lookups_bp)
app.register_blueprint(settings_bp)
app.register_blueprint(api_bp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SpeakQuery application')
    sub = parser.add_subparsers(dest='command')
    admin_parser = sub.add_parser('create-admin', help='Create admin user and exit')
    admin_parser.add_argument('username', help='Admin username')
    admin_parser.add_argument('password', help='Admin password')
    admin_parser.add_argument('--role', default='admin', help='User role')
    admin_parser.add_argument('--token', help='API token')
    args = parser.parse_args()

    if args.command == 'create-admin':
        initialize_database(args.username, args.password, args.role, args.token)
        sys.exit(0)

    delete_old_files()
    initialize_database()
    load_settings_into_config()  # Load settings into app.config
    queue_size = int(app.config.get('QUEUE_SIZE', 10))
    processing_limit = int(app.config.get('PROCESSING_LIMIT', 2))
    app.config['TASK_QUEUE'] = TaskQueue(queue_size, processing_limit)
    limiter.enabled = app.config.get('THROTTLE_ENABLED', True)
    logging.basicConfig(
        level=app.config['LOG_LEVEL'],
        format='[%(levelname)s] %(message)s',
    )
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() in {'1', 'true', 't'}
    app.run(host='0.0.0.0', debug=debug_mode)

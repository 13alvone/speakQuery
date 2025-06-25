from flask import Blueprint, request, jsonify
from flask import current_app as app
import os
from werkzeug.utils import secure_filename
from app import (
    execute_speakQuery,
    save_dataframe,
    validator,
    is_title_unique,
    get_next_runtime,
)
from queue import Full
import sqlite3
import time
import uuid
import logging

api_bp = Blueprint('api_bp', __name__)

BAN_THRESHOLD = 5
BAN_WINDOW = 300  # seconds


def _is_ip_banned(ip: str) -> bool:
    """Return True if the IP is currently banned."""
    if not app.config.get('BAN_DELETIONS_ENABLED', False):
        return False
    now = int(time.time())
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT banned_until FROM api_bans WHERE ip=?', (ip,))
        row = cursor.fetchone()
        return bool(row and row[0] and row[0] > now)


def _record_failure(ip: str):
    """Increment failure counter for IP and ban if threshold exceeded."""
    if not app.config.get('BAN_DELETIONS_ENABLED', False):
        return
    now = int(time.time())
    ban_duration = int(app.config.get('BAN_DURATION', 3600))
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT count, last_attempt, banned_until FROM api_bans WHERE ip=?',
            (ip,),
        )
        row = cursor.fetchone()
        if row:
            count, last_attempt, banned_until = row
            if banned_until and banned_until > now:
                return
            if last_attempt and now - last_attempt <= BAN_WINDOW:
                count += 1
            else:
                count = 1
            last_attempt = now
        else:
            count = 1
            last_attempt = now
            banned_until = 0

        if count >= BAN_THRESHOLD:
            banned_until = now + ban_duration
            count = 0

        cursor.execute(
            'REPLACE INTO api_bans (ip, count, last_attempt, banned_until) VALUES (?, ?, ?, ?)',
            (ip, count, last_attempt, banned_until),
        )
        conn.commit()

@api_bp.route('/api/query', methods=['POST'])
def api_query():
    """Execute a query and return results with timing metadata."""
    start_ts = time.time()
    data = request.get_json(force=True, silent=True)
    if data is None or 'query' not in data:
        logging.error("[x] No query provided to /api/query")
        return jsonify({'status': 'error', 'message': 'Expected JSON payload.'}), 400

    query_str = data.get('query')
    try:
        future = app.config['TASK_QUEUE'].submit(execute_speakQuery, query_str)
        result_df = future.result()
        if result_df is None or result_df.empty:
            return jsonify({'status': 'error', 'message': 'No data returned from query.'}), 200

        result_df = result_df.fillna('')
        column_names = result_df.columns.values.tolist()
        row_data = result_df.to_dict(orient='records')

        request_id = f"{start_ts}_{uuid.uuid4()}"
        save_dataframe(request_id, result_df, query_str)

        end_ts = time.time()
        duration_ms = round((end_ts - start_ts) * 1000, 2)

        return jsonify({
            'status': 'success',
            'results': row_data,
            'column_names': column_names,
            'request_id': request_id,
            'time_sent': start_ts,
            'time_received': end_ts,
            'duration_ms': duration_ms
        })
    except Full:
        return jsonify({'status': 'error', 'message': 'Server busy'}), 429
    except Exception as exc:
        logging.error(f"Error processing query: {exc}")
        return jsonify({'status': 'error', 'message': str(exc)}), 500


@api_bp.route('/api/saved_search', methods=['POST'])
def api_create_saved_search():
    """Create a new saved search."""
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided.'}), 400

    search_id = data.get('request_id') or data.get('id')
    if not search_id:
        return jsonify({'status': 'error', 'message': 'request_id is required.'}), 400

    validate_utf8 = getattr(validator, 'validate_utf8', lambda x: x)
    validate_cron = getattr(validator, 'validate_cron_schedule', lambda x: x)
    validate_trigger = getattr(validator, 'validate_trigger', lambda x: x)
    validate_lookback = getattr(validator, 'validate_lookback', lambda x: x)
    validate_bool = getattr(validator, 'validate_boolean', lambda x: x)
    validate_tp = getattr(validator, 'validate_throttle_time_period', lambda x: x)
    validate_email = getattr(validator, 'validate_email', lambda x: x)

    if not is_title_unique(validate_utf8(data.get('title', ''))):
        return jsonify({'status': 'error', 'message': 'Title must be unique.'}), 400

    try:
        description = validate_utf8(data.get('description', ''))
        query = data.get('query', '')
        cron_schedule = validate_cron(data.get('cron_schedule', '* * * * *'))
        trigger = validate_trigger(data.get('trigger', 'Once'))
        lookback = validate_lookback(data.get('lookback', '-1h'))
        throttle = validate_bool(data.get('throttle', 'no'))
        if throttle != 'no':
            throttle_time_period = validate_tp(data.get('throttle_time_period', '-1h'))
            throttle_by = validate_utf8(data.get('throttle_by', ''))
        else:
            throttle_time_period = 0
            throttle_by = 'N/A'
        event_message = validate_utf8(data.get('event_message', ''))
        send_email = validate_bool(data.get('send_email', 'no'))
        if send_email != 'no':
            email_address = validate_email(data.get('email_address', 'user@example.com'))
            email_content = data.get('email_content', '')
        else:
            email_address = 'N/A'
            email_content = 'N/A'
        dest_file = app.config['TEMP_DIR']

        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''INSERT INTO saved_searches (
                    id, title, description, query, cron_schedule, trigger, lookback,
                    throttle, throttle_time_period, throttle_by, event_message,
                    send_email, email_address, email_content, file_location
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    search_id, data['title'], description, query, cron_schedule, trigger,
                    lookback, throttle, str(throttle_time_period), throttle_by, event_message,
                    send_email, email_address, email_content, dest_file
                )
            )
            conn.commit()
        return jsonify({'status': 'success', 'id': search_id}), 201
    except Exception as exc:
        logging.error(f"Error creating saved search: {exc}")
        return jsonify({'status': 'error', 'message': str(exc)}), 400


@api_bp.route('/api/saved_search/<search_id>', methods=['PATCH'])
def api_update_saved_search(search_id):
    """Update an existing saved search."""
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({'status': 'error', 'message': 'No data provided.'}), 400

    required = [
        'title', 'description', 'query', 'cron_schedule', 'trigger', 'lookback',
        'throttle', 'throttle_time_period', 'throttle_by', 'event_message',
        'send_email', 'email_address', 'email_content', 'disabled'
    ]
    missing = [f for f in required if f not in data]
    if missing:
        return jsonify({'status': 'error', 'message': f'Missing fields: {", ".join(missing)}'}), 400

    try:
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''UPDATE saved_searches SET title=?, description=?, query=?, cron_schedule=?, trigger=?,
                    lookback=?, throttle=?, throttle_time_period=?, throttle_by=?, event_message=?,
                    send_email=?, email_address=?, email_content=?, disabled=? WHERE id=?''',
                (
                    data['title'], data['description'], data['query'], data['cron_schedule'],
                    data['trigger'], data['lookback'], data['throttle'], data['throttle_time_period'],
                    data['throttle_by'], data['event_message'], data['send_email'], data['email_address'],
                    data['email_content'], int(data['disabled']), search_id
                )
            )
            conn.commit()
        return jsonify({'status': 'success'}), 200
    except Exception as exc:
        logging.error(f"Error updating saved search: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal server error.'}), 500


@api_bp.route('/api/saved_search/<search_id>', methods=['DELETE'])
def api_delete_saved_search(search_id):
    """Delete a saved search."""
    ip = request.remote_addr
    if _is_ip_banned(ip):
        return jsonify({'status': 'error', 'message': 'IP banned'}), 403
    try:
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM saved_searches WHERE id = ?', (search_id,))
            if cursor.rowcount == 0:
                _record_failure(ip)
                return jsonify({'status': 'error', 'message': 'Saved search not found'}), 404
            conn.commit()
        return jsonify({'status': 'success'}), 200
    except Exception as exc:
        logging.error(f"Error deleting saved search: {exc}")
        _record_failure(ip)
        return jsonify({'status': 'error', 'message': 'Failed to delete saved search'}), 500


@api_bp.route('/api/saved_search/<search_id>', methods=['GET'])
def api_get_saved_search(search_id):
    """Return saved search metadata."""
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM saved_searches WHERE id = ?', (search_id,))
        row = cursor.fetchone()

    if not row:
        return jsonify({'status': 'error', 'message': 'Saved search not found'}), 404

    search = {key: row[key] for key in row.keys()}
    search['disabled'] = bool(search.get('disabled', 0))
    return jsonify({'status': 'success', 'search': search}), 200


@api_bp.route('/api/saved_search/<search_id>/settings', methods=['GET'])
def api_get_saved_search_settings(search_id):
    """Return saved search settings including next scheduled runtime."""
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM saved_searches WHERE id = ?', (search_id,))
        row = cursor.fetchone()

    if not row:
        return jsonify({'status': 'error', 'message': 'Saved search not found'}), 404

    search = {key: row[key] for key in row.keys()}
    search['disabled'] = bool(search.get('disabled', 0))
    search['next_scheduled_time'] = get_next_runtime(search.get('cron_schedule', '* * * * *'))
    return jsonify({'status': 'success', 'search': search}), 200


@api_bp.route('/api/lookup/<name>', methods=['DELETE'])
def api_delete_lookup(name):
    """Delete a lookup file by name."""
    ip = request.remote_addr
    if _is_ip_banned(ip):
        return jsonify({'status': 'error', 'message': 'IP banned'}), 403

    safe_name = secure_filename(name)
    lookup_dir = app.config['LOOKUP_DIR']
    file_path = os.path.join(lookup_dir, safe_name)

    if not os.path.abspath(file_path).startswith(os.path.abspath(lookup_dir)):
        _record_failure(ip)
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403

    if not os.path.exists(file_path):
        _record_failure(ip)
        return jsonify({'status': 'error', 'message': 'File not found.'}), 404

    try:
        os.remove(file_path)
        return jsonify({'status': 'success'}), 200
    except Exception as exc:
        logging.error(f"Error deleting lookup file: {exc}")
        _record_failure(ip)
        return jsonify({'status': 'error', 'message': 'Failed to delete file.'}), 500


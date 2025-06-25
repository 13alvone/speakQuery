from flask import Blueprint, request, jsonify
from flask import current_app as app
from app import execute_speakQuery, save_dataframe, validator, is_title_unique
import sqlite3
import time
import uuid
import logging

api_bp = Blueprint('api_bp', __name__)

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
        result_df = execute_speakQuery(query_str)
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
    try:
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM saved_searches WHERE id = ?', (search_id,))
            conn.commit()
        return jsonify({'status': 'success'}), 200
    except Exception as exc:
        logging.error(f"Error deleting saved search: {exc}")
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


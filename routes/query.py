from flask import Blueprint, request, jsonify
from flask import current_app as app
from app import execute_speakQuery, save_dataframe, load_dataframe, is_allowed_api_url, is_title_unique, requests
import sqlite3
import time
import uuid
import logging

query_bp = Blueprint('query_bp', __name__)

@query_bp.route('/check_title_unique', methods=['POST'])
def check_title_unique_route():
    title = request.json.get('title')
    if title:
        is_unique = is_title_unique(title)
        return jsonify({'is_unique': is_unique})
    else:
        return jsonify({'error': 'No title provided'}), 400


@query_bp.route('/fetch_api_data', methods=['POST'])
def fetch_api_data():
    data = request.get_json()
    api_url = data.get('api_url')

    if not api_url:
        return jsonify({'status': 'error', 'message': 'API URL is required.'}), 400

    try:
        if not is_allowed_api_url(api_url):
            return jsonify({'status': 'error', 'message': 'Domain not allowed.'}), 400

        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        api_data = response.json()
        return jsonify({'status': 'success', 'api_data': api_data}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 400


@query_bp.route('/run_query', methods=['POST'])
def run_query():
    data = request.get_json(force=True, silent=True)
    if data is None:
        logging.error("[x] No JSON payload detected in /run_query request.")
        return jsonify({'status': 'error', 'message': 'Expected JSON payload.'}), 400
    query_str = data.get('query', '')
    logging.info(f"[i] Query received: {query_str}")

    try:
        result_df = execute_speakQuery(data.get('query'))
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

        request_id = f'{time.time()}_{str(uuid.uuid4())}'
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


@query_bp.route('/get_query_for_loadjob/<filename>', methods=['GET'])
def get_query_for_loadjob(filename):
    try:
        if not filename.endswith('.pkl'):
            return jsonify({'error': 'Invalid filename format. Expected a .pkl file.'}), 400

        request_id = filename[:-4]
        with sqlite3.connect(app.config['HISTORY_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT query FROM history WHERE query_id = ?', (request_id,))
            row = cursor.fetchone()

        if row:
            original_query = row[0]
            return jsonify({'query': original_query}), 200
        else:
            return jsonify({'error': 'No query found for the given load job filename.'}), 404
    except Exception as e:
        logging.error(f"Error retrieving query for load job '{filename}': {str(e)}")
        return jsonify({'error': 'Internal server error.'}), 500


@query_bp.route('/save_results', methods=['POST'])
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


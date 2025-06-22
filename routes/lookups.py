from flask import Blueprint, request, jsonify
from flask import current_app as app
from app import get_row_count, allowed_file
import pandas as pd
import os
import shutil
import logging
from datetime import datetime
from werkzeug.utils import secure_filename

lookups_bp = Blueprint('lookups_bp', __name__)

@lookups_bp.route('/get_lookup_files', methods=['GET'])
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


@lookups_bp.route('/get_loadjob_files', methods=['GET'])
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


@lookups_bp.route('/upload_file', methods=['POST'])
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


@lookups_bp.route('/view_lookup', methods=['GET'])
def view_lookup():
    filepath = request.args.get('file')
    if not filepath:
        return "<p>Error: No file specified.</p>", 400
    filename = filepath.split('/')[-1]

    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return "<p>Error: Invalid file path.</p>", 400

    safe_filepath = os.path.normpath(filepath)
    target_filepath = f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')
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


@lookups_bp.route('/delete_lookup_file', methods=['POST'])
def delete_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    filename = filepath.split('/')[-1]

    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400

    safe_filepath = os.path.normpath(filepath)
    target_filepath = f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')
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


@lookups_bp.route('/clone_lookup_file', methods=['POST'])
def clone_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    new_name = data.get('new_name')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    filename = filepath.split('/')[-1]

    if not filepath.startswith(app.config['LOOKUP_DIR']):
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400

    safe_filepath = os.path.normpath(filepath)
    target_filepath = f"{os.path.abspath(app.config['LOOKUP_DIR']).replace(app.config['SCRIPT_DIR'], '')}/{filename}".lstrip('/')

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


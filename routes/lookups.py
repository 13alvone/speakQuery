from flask import Blueprint, request, jsonify
from flask import current_app as app
from flask_login import login_required, current_user
from utils.file_utils import get_row_count, allowed_file
import pandas as pd
import csv
import os
import shutil
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from werkzeug.utils import secure_filename

lookups_bp = Blueprint('lookups_bp', __name__)

@lookups_bp.route('/get_lookup_files', methods=['GET'])
def get_lookup_files():
    lookup_dir = app.config['LOOKUP_DIR']
    files = []
    owner_map = {}
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT filename, owner_id FROM lookup_files')
        owner_map = dict(cursor.fetchall())
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
                'row_count': row_count,
                'owner_id': owner_map.get(filename)
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
@login_required
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'message': 'No file part in the request.'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'message': 'No file selected for uploading.'}), 400

    if not allowed_file(file.filename, app.config['ALLOWED_EXTENSIONS']):
        return jsonify({'status': 'error', 'message': 'File type not allowed.'}), 400

    file_ext = file.filename.rsplit('.', 1)[-1].lower()
    if file_ext == 'csv':
        try:
            file.stream.seek(0)
            sample = file.stream.read(1024)
            if isinstance(sample, bytes):
                sample_decoded = sample.decode('utf-8', errors='ignore')
            else:
                sample_decoded = sample
            dialect = csv.Sniffer().sniff(sample_decoded)
            if dialect.delimiter not in {',', ';', '\t', '|'}:
                raise csv.Error('unexpected delimiter')
            file.stream.seek(0)
        except csv.Error:
            file.stream.seek(0)
            try:
                df = pd.read_csv(file.stream, nrows=5)
                file.stream.seek(0)
                if df.shape[1] <= 1 and ',' not in sample_decoded:
                    raise ValueError('parsed but single column')
            except Exception as e:
                logging.warning("[!] Invalid CSV upload %s: %s", file.filename, e)
                return jsonify({'status': 'error', 'message': 'Invalid CSV content.'}), 400

    filename = secure_filename(file.filename)
    upload_dir = app.config['LOOKUP_DIR']
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, filename)

    try:
        file.save(file_path)
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO lookup_files (filename, owner_id, created_at) VALUES (?, ?, ?)',
                (filename, current_user.get_id(), int(time.time()))
            )
            conn.commit()
        return jsonify({'status': 'success', 'message': 'File uploaded successfully.'})
    except Exception as e:
        logging.error(f"[x] Error saving file: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)})


@lookups_bp.route('/view_lookup', methods=['GET'])
def view_lookup():
    filepath = request.args.get('file')
    if not filepath:
        return "<p>Error: No file specified.</p>", 400
    lookup_dir = Path(app.config['LOOKUP_DIR']).resolve()
    try:
        target_path = Path(filepath).resolve(strict=False)
    except OSError:
        return "<p>Error: Invalid file path.</p>", 400
    if not target_path.is_relative_to(lookup_dir):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403
    if not target_path.exists():
        return "<p>Error: File not found.</p>", 404
    try:
        # Limit rows read to avoid loading large files entirely into memory
        row_limit = request.args.get('rows', '100')
        try:
            row_limit = int(row_limit)
        except ValueError:
            row_limit = 100
        if row_limit <= 0 or row_limit > 1000:
            row_limit = 100
        df = pd.read_csv(target_path, nrows=row_limit)
        return df.to_html()
    except Exception as e:
        logging.error("[x] Error reading file: %s", e)
        return "<p>Error reading file.</p>", 500


@lookups_bp.route('/delete_lookup_file', methods=['POST'])
@login_required
def delete_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    lookup_dir = Path(app.config['LOOKUP_DIR']).resolve()
    try:
        target_path = Path(filepath).resolve(strict=False)
    except OSError:
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400
    if not target_path.is_relative_to(lookup_dir):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403
    if not target_path.exists():
        return jsonify({'status': 'error', 'message': 'File not found.'}), 404
    filename = target_path.name
    try:
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT owner_id FROM lookup_files WHERE filename=?', (filename,))
            row = cursor.fetchone()
            owner_id = row[0] if row else None
            if owner_id is not None and str(getattr(current_user, 'role', '')) != 'admin' and str(current_user.get_id()) != str(owner_id):
                return jsonify({'status': 'error', 'message': 'Forbidden'}), 403
            cursor.execute('DELETE FROM lookup_files WHERE filename=?', (filename,))
            conn.commit()
        target_path.unlink()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error("[x] Error deleting file: %s", e)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@lookups_bp.route('/clone_lookup_file', methods=['POST'])
@login_required
def clone_lookup_file():
    data = request.json
    filepath = data.get('filepath')
    new_name = data.get('new_name')
    if not filepath:
        return jsonify({'status': 'error', 'message': 'No file specified.'}), 400
    lookup_dir = Path(app.config['LOOKUP_DIR']).resolve()
    try:
        target_path = Path(filepath).resolve(strict=False)
    except OSError:
        return jsonify({'status': 'error', 'message': 'Invalid file path.'}), 400
    if not target_path.is_relative_to(lookup_dir):
        return jsonify({'status': 'error', 'message': 'Access denied.'}), 403
    if not target_path.exists():
        return jsonify({'status': 'error', 'message': 'File not found.'}), 404
    if not new_name:
        base_name = target_path.name
        new_name = f"{Path(base_name).stem}_copy{Path(base_name).suffix}"
    new_filename = secure_filename(new_name)
    new_filepath = target_path.with_name(new_filename)
    filename = target_path.name
    try:
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT owner_id FROM lookup_files WHERE filename=?', (filename,))
            row = cursor.fetchone()
            owner_id = row[0] if row else None
            if owner_id is not None and str(getattr(current_user, 'role', '')) != 'admin' and str(current_user.get_id()) != str(owner_id):
                return jsonify({'status': 'error', 'message': 'Forbidden'}), 403
        shutil.copy(target_path, new_filepath)
        with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
            cursor = conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO lookup_files (filename, owner_id, created_at) VALUES (?, ?, ?)',
                (new_filename, current_user.get_id(), int(time.time()))
            )
            conn.commit()
        return jsonify({'status': 'success'})
    except Exception as e:
        logging.error("[x] Error cloning file: %s", e)
        return jsonify({'status': 'error', 'message': str(e)}), 500


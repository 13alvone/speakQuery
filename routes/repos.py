from flask import Blueprint, request, jsonify
from flask import current_app as app
from utils.auth import admin_required
import sqlite3
from pathlib import Path
import subprocess
import logging

repos_bp = Blueprint('repos_bp', __name__)

@repos_bp.route('/clone_repo', methods=['POST'])
@admin_required
def clone_repo():
    data = request.get_json(silent=True) or request.form
    name = data.get('name') if data else None
    git_url = data.get('git_url') if data else None
    if not name or not git_url:
        return jsonify({'status': 'error', 'message': 'Missing name or git_url'}), 400
    base_dir = Path(app.config['INPUT_REPOS_DIR']).resolve()
    target_dir = (base_dir / name).resolve()
    if not target_dir.is_relative_to(base_dir):
        logging.warning(f"[!] Clone path {target_dir} outside of INPUT_REPOS_DIR")
        return jsonify({'status': 'error', 'message': 'Invalid path'}), 400
    if target_dir.exists():
        return jsonify({'status': 'error', 'message': 'Repo already exists'}), 400
    try:
        subprocess.check_call(['git', 'clone', git_url, str(target_dir)])
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO input_repos (name, git_url, path, active) VALUES (?, ?, ?, 1)',
                (name, git_url, str(target_dir))
            )
            conn.commit()
        return jsonify({'status': 'success'})
    except subprocess.CalledProcessError as exc:
        logging.error(f"[x] Git clone failed: {exc}")
        return jsonify({'status': 'error', 'message': 'Clone failed'}), 500
    except Exception as exc:
        logging.error(f"[x] Error cloning repo: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

@repos_bp.route('/list_repo_scripts/<int:repo_id>', methods=['GET'])
@admin_required
def list_repo_scripts(repo_id):
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('SELECT path FROM input_repos WHERE id=?', (repo_id,))
            row = cur.fetchone()
        if not row:
            return jsonify({'status': 'error', 'message': 'Repo not found'}), 404
        base_dir = Path(app.config['INPUT_REPOS_DIR']).resolve()
        repo_path = Path(row[0]).resolve()
        if not repo_path.is_relative_to(base_dir):
            logging.warning(f"[!] Repo path {repo_path} outside of INPUT_REPOS_DIR")
            return jsonify({'status': 'error', 'message': 'Invalid repo path'}), 400
        scripts = [str(p.relative_to(repo_path)) for p in repo_path.rglob('*.py')]
        return jsonify({'status': 'success', 'scripts': scripts})
    except Exception as exc:
        logging.error(f"[x] Error listing scripts: {exc}")
        return jsonify({'status': 'error', 'message': 'Failed to list scripts'}), 500

@repos_bp.route('/set_script_schedule', methods=['POST'])
@admin_required
def set_script_schedule():
    data = request.get_json(silent=True) or request.form
    repo_id = data.get('repo_id')
    script = data.get('script_name')
    cron = data.get('cron_schedule')
    if not repo_id or not script or not cron:
        return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute(
                'REPLACE INTO repo_scripts (repo_id, script_name, cron_schedule) VALUES (?, ?, ?)',
                (int(repo_id), script, cron)
            )
            conn.commit()
        return jsonify({'status': 'success'})
    except Exception as exc:
        logging.error(f"[x] Error setting schedule: {exc}")
        return jsonify({'status': 'error', 'message': 'Failed to set schedule'}), 500

@repos_bp.route('/pull_repo/<int:repo_id>', methods=['POST'])
@admin_required
def pull_repo(repo_id):
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('SELECT path FROM input_repos WHERE id=?', (repo_id,))
            row = cur.fetchone()
        if not row:
            return jsonify({'status': 'error', 'message': 'Repo not found'}), 404
        base_dir = Path(app.config['INPUT_REPOS_DIR']).resolve()
        repo_path = Path(row[0]).resolve()
        if not repo_path.is_relative_to(base_dir):
            logging.warning(f"[!] Repo path {repo_path} outside of INPUT_REPOS_DIR")
            return jsonify({'status': 'error', 'message': 'Invalid repo path'}), 400
        if not repo_path.exists():
            return jsonify({'status': 'error', 'message': 'Path missing'}), 404
        subprocess.check_call(['git', '-C', str(repo_path), 'pull'])
        return jsonify({'status': 'success'})
    except subprocess.CalledProcessError as exc:
        logging.error(f"[x] Git pull failed: {exc}")
        return jsonify({'status': 'error', 'message': 'Pull failed'}), 500
    except Exception as exc:
        logging.error(f"[x] Error pulling repo: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

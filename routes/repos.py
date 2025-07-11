from flask import Blueprint, request, jsonify
from flask import current_app as app
from utils.auth import admin_required
import sqlite3
from pathlib import Path
import subprocess
import shutil
import logging
from apscheduler.triggers.cron import CronTrigger

repos_bp = Blueprint('repos_bp', __name__)

@repos_bp.route('/list_repos', methods=['GET'])
@admin_required
def list_repos():
    """Return a list of repos stored in the database."""
    try:
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('SELECT id, name, git_url FROM input_repos ORDER BY id')
            rows = cur.fetchall()
        repos = [
            {'id': r[0], 'name': r[1], 'git_url': r[2]}
            for r in rows
        ]
        return jsonify({'status': 'success', 'repos': repos})
    except Exception as exc:
        logging.error(f"[x] Error listing repos: {exc}")
        return jsonify({'status': 'error', 'message': 'Failed to list repos'}), 500

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

    # Validate cron expression
    try:
        CronTrigger.from_crontab(cron)
    except Exception as exc:  # ValueError on invalid spec
        logging.warning(f"[!] Invalid cron schedule '{cron}': {exc}")
        return jsonify({'status': 'error', 'message': 'Invalid cron schedule'}), 400

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
        script_path = (repo_path / script).resolve()
        if not script_path.is_relative_to(repo_path):
            logging.warning(f"[!] Script path {script_path} outside repo {repo_path}")
            return jsonify({'status': 'error', 'message': 'Invalid script path'}), 400
        if not script_path.is_file():
            logging.warning(f"[!] Script file {script_path} not found")
            return jsonify({'status': 'error', 'message': 'Script not found'}), 400

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


@repos_bp.route('/edit_repo_file', methods=['POST'])
@admin_required
def edit_repo_file():
    """Edit the contents of an existing file within a repo."""
    data = request.get_json(silent=True) or request.form
    repo_id = data.get('repo_id') if data else None
    rel_path = data.get('file_path') if data else None
    content = data.get('content') if data else None
    if not repo_id or not rel_path or content is None:
        return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
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

        file_path = (repo_path / rel_path).resolve()
        if not file_path.is_relative_to(repo_path):
            logging.warning(f"[!] File path {file_path} outside repo {repo_path}")
            return jsonify({'status': 'error', 'message': 'Invalid file path'}), 400
        if not file_path.exists():
            return jsonify({'status': 'error', 'message': 'File not found'}), 404
        if file_path.is_dir():
            return jsonify({'status': 'error', 'message': 'Path is directory'}), 400
        with file_path.open('w', encoding='utf-8') as fh:
            fh.write(content)
        return jsonify({'status': 'success'})
    except Exception as exc:
        logging.error(f"[x] Error editing repo file: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500


@repos_bp.route('/delete_repo_file', methods=['POST'])
@admin_required
def delete_repo_file():
    """Delete a file from a repo."""
    data = request.get_json(silent=True) or request.form
    repo_id = data.get('repo_id') if data else None
    rel_path = data.get('file_path') if data else None
    if not repo_id or not rel_path:
        return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
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

        file_path = (repo_path / rel_path).resolve()
        if not file_path.is_relative_to(repo_path):
            logging.warning(f"[!] File path {file_path} outside repo {repo_path}")
            return jsonify({'status': 'error', 'message': 'Invalid file path'}), 400
        if not file_path.exists():
            return jsonify({'status': 'error', 'message': 'File not found'}), 404
        if file_path.is_dir():
            return jsonify({'status': 'error', 'message': 'Path is directory'}), 400
        file_path.unlink()
        return jsonify({'status': 'success'})
    except Exception as exc:
        logging.error(f"[x] Error deleting repo file: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500


@repos_bp.route('/delete_repo/<int:repo_id>', methods=['POST'])
@admin_required
def delete_repo(repo_id):
    """Delete an entire repo and its database record."""
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
        if repo_path.exists():
            shutil.rmtree(repo_path)
        with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
            cur = conn.cursor()
            cur.execute('DELETE FROM repo_scripts WHERE repo_id=?', (repo_id,))
            cur.execute('DELETE FROM input_repos WHERE id=?', (repo_id,))
            conn.commit()
        return jsonify({'status': 'success'})
    except Exception as exc:
        logging.error(f"[x] Error deleting repo: {exc}")
        return jsonify({'status': 'error', 'message': 'Internal error'}), 500

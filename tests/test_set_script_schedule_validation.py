import os
import sys
import sqlite3
import shutil
import subprocess
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def _setup_app(tmp_path, monkeypatch):
    from app import app, initialize_database
    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)
    app.config['INPUT_REPOS_DIR'] = str(tmp_path / 'input_repos')
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        conn.execute('UPDATE users SET force_password_change=0 WHERE username="admin"')
        conn.commit()
    return app, orig_db


def _create_repo(src_dir):
    (src_dir / 'script.py').write_text('print("hi")')
    sub = src_dir / 'scheduled_input_scripts'
    sub.mkdir()
    (sub / 'example_dataframe_job.py').write_text('print("demo")')
    subprocess.check_call(['git', '-C', str(src_dir), 'init'])
    subprocess.check_call(['git', '-C', str(src_dir), 'config', 'user.email', 'a@b.com'])
    subprocess.check_call(['git', '-C', str(src_dir), 'config', 'user.name', 't'])
    subprocess.check_call(['git', '-C', str(src_dir), 'add', 'script.py'])
    subprocess.check_call(['git', '-C', str(src_dir), 'commit', '-m', 'init'])


def test_invalid_cron_schedule(mock_heavy_modules, tmp_path, monkeypatch):
    app, orig_db = _setup_app(tmp_path, monkeypatch)
    repo_src = tmp_path / 'src'
    repo_src.mkdir()
    _create_repo(repo_src)

    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/clone_repo', json={'name': 'r1', 'git_url': str(repo_src)})
    assert resp.status_code == 200

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        repo_id = conn.execute('SELECT id FROM input_repos WHERE name=?', ('r1',)).fetchone()[0]

    resp = client.post(
        '/set_script_schedule',
        json={
            'repo_id': repo_id,
            'script_name': 'scheduled_input_scripts/example_dataframe_job.py',
            'cron_schedule': 'invalid cron'
        }
    )
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    app.config['SCHEDULED_INPUTS_DB'] = orig_db


def test_invalid_script_path(mock_heavy_modules, tmp_path, monkeypatch):
    app, orig_db = _setup_app(tmp_path, monkeypatch)
    repo_src = tmp_path / 'src'
    repo_src.mkdir()
    _create_repo(repo_src)

    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/clone_repo', json={'name': 'r1', 'git_url': str(repo_src)})
    assert resp.status_code == 200

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        repo_id = conn.execute('SELECT id FROM input_repos WHERE name=?', ('r1',)).fetchone()[0]

    # Nonexistent script
    resp = client.post(
        '/set_script_schedule',
        json={
            'repo_id': repo_id,
            'script_name': 'missing.py',
            'cron_schedule': '* * * * *'
        }
    )
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'

    # Path traversal attempt
    resp = client.post(
        '/set_script_schedule',
        json={
            'repo_id': repo_id,
            'script_name': '../evil.py',
            'cron_schedule': '* * * * *'
        }
    )
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    app.config['SCHEDULED_INPUTS_DB'] = orig_db

import os
import sys
import sqlite3
import shutil
import subprocess
from pathlib import Path
from werkzeug.security import generate_password_hash

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


def test_edit_delete_repo_flow(mock_heavy_modules, tmp_path, monkeypatch):
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
    repo_path = Path(app.config['INPUT_REPOS_DIR']) / 'r1'
    file_path = repo_path / 'script.py'
    resp = client.post('/edit_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py', 'content': 'print("bye")'})
    assert resp.status_code == 200
    assert file_path.read_text() == 'print("bye")'
    resp = client.post('/edit_repo_file', json={'repo_id': repo_id, 'file_path': 'new.py', 'content': 'x'})
    assert resp.status_code == 404
    resp = client.post('/delete_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py'})
    assert resp.status_code == 200
    assert not file_path.exists()
    resp = client.post(f'/delete_repo/{repo_id}')
    assert resp.status_code == 200
    assert not repo_path.exists()
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        assert not conn.execute('SELECT 1 FROM input_repos WHERE id=?', (repo_id,)).fetchone()
    app.config['SCHEDULED_INPUTS_DB'] = orig_db


def test_repo_admin_endpoints_permissions(mock_heavy_modules, tmp_path, monkeypatch):
    app, orig_db = _setup_app(tmp_path, monkeypatch)
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        conn.execute('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)', ('user', generate_password_hash('pw'), 'user'))
        conn.commit()
    repo_src = tmp_path / 'src'
    repo_src.mkdir()
    _create_repo(repo_src)
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    client.post('/clone_repo', json={'name': 'r1', 'git_url': str(repo_src)})
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        repo_id = conn.execute('SELECT id FROM input_repos WHERE name=?', ('r1',)).fetchone()[0]
    client.get('/logout')
    resp = client.post('/edit_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py', 'content': 'x'})
    assert resp.status_code == 302
    resp = client.post('/delete_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py'})
    assert resp.status_code == 302
    resp = client.post(f'/delete_repo/{repo_id}')
    assert resp.status_code == 302
    client.post('/login', json={'username': 'user', 'password': 'pw'})
    resp = client.post('/edit_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py', 'content': 'x'})
    assert resp.status_code == 403
    resp = client.post('/delete_repo_file', json={'repo_id': repo_id, 'file_path': 'script.py'})
    assert resp.status_code == 403
    resp = client.post(f'/delete_repo/{repo_id}')
    assert resp.status_code == 403
    app.config['SCHEDULED_INPUTS_DB'] = orig_db


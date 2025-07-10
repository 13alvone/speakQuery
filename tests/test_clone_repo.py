import os
import sys
import sqlite3
import shutil
import subprocess
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_clone_repo_endpoint(mock_heavy_modules, tmp_path, monkeypatch):
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

    repo_src = tmp_path / 'src'
    repo_src.mkdir()
    (repo_src / 'script.py').write_text('print("hi")')
    subprocess.check_call(['git', '-C', str(repo_src), 'init'])
    subprocess.check_call(['git', '-C', str(repo_src), 'config', 'user.email', 'a@b.com'])
    subprocess.check_call(['git', '-C', str(repo_src), 'config', 'user.name', 't'])
    subprocess.check_call(['git', '-C', str(repo_src), 'add', 'script.py'])
    subprocess.check_call(['git', '-C', str(repo_src), 'commit', '-m', 'init'])

    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/clone_repo', json={'name': 'r1', 'git_url': str(repo_src)})
    assert resp.status_code == 200
    cloned = Path(app.config['INPUT_REPOS_DIR']) / 'r1'
    assert cloned.exists()
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.execute('SELECT name FROM input_repos WHERE name=?', ('r1',))
        assert cur.fetchone()

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

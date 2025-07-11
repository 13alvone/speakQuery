import os
import sys
import sqlite3
import shutil
import subprocess
import asyncio
import types
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


def test_ui_repo_schedule(mock_heavy_modules, tmp_path, monkeypatch):
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
            'cron_schedule': '* * * * *'
        }
    )
    assert resp.status_code == 200

    backend_mod = types.ModuleType('ScheduledInputBackend')
    backend_mod.ScheduledInputBackend = lambda *a, **k: None
    sys.modules['ScheduledInputBackend'] = backend_mod
    cleanup_mod = types.ModuleType('SICleanup')
    cleanup_mod.cleanup_indexes = lambda: []
    sys.modules['SICleanup'] = cleanup_mod

    from scheduled_input_engine import ScheduledInputEngine as sie
    sie.SCHEDULED_INPUTS_DB = Path(app.config['SCHEDULED_INPUTS_DB'])
    sie.scheduler.remove_all_jobs()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(sie.schedule_repo_scripts())
        jobs = sie.scheduler.get_jobs()
        assert any(j.id.startswith('repo_') for j in jobs)
    finally:
        asyncio.set_event_loop(None)
        loop.close()
    app.config['SCHEDULED_INPUTS_DB'] = orig_db

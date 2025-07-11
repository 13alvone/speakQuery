import os
import sys
import sqlite3
import asyncio
import types
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tests.test_repo_ui_schedule import _setup_app, _create_repo


def test_delete_script_schedule_updates_scheduler(mock_heavy_modules, tmp_path, monkeypatch):
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

    # Set schedule for a repo script
    script_name = 'scheduled_input_scripts/example_dataframe_job.py'
    resp = client.post(
        '/set_script_schedule',
        json={
            'repo_id': repo_id,
            'script_name': script_name,
            'cron_schedule': '* * * * *'
        }
    )
    assert resp.status_code == 200

    from scheduled_input_engine import ScheduledInputEngine as sie
    if not hasattr(sie, 'schedule_repo_scripts'):
        import importlib
        sys.modules.pop('scheduled_input_engine.ScheduledInputEngine', None)
        sie = importlib.import_module('scheduled_input_engine.ScheduledInputEngine')
    sie.SCHEDULED_INPUTS_DB = Path(app.config['SCHEDULED_INPUTS_DB'])

    class _DummyScheduler:
        def __init__(self):
            self.jobs = []
        def add_job(self, func, trigger, args=None, id=None, replace_existing=False):
            self.jobs.append(types.SimpleNamespace(id=id))
        def get_jobs(self):
            return self.jobs
        def remove_all_jobs(self):
            self.jobs.clear()

    sie.scheduler = _DummyScheduler()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(sie.schedule_repo_scripts())
        jobs = sie.scheduler.get_jobs()
        assert any(j.id.startswith('repo_') for j in jobs)
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    # Delete schedule and reschedule
    resp = client.post('/delete_script_schedule', json={'repo_id': repo_id, 'script_name': script_name})
    assert resp.status_code == 200
    sie.scheduler.remove_all_jobs()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(sie.schedule_repo_scripts())
        jobs = sie.scheduler.get_jobs()
        assert not any(j.id.startswith('repo_') for j in jobs)
    finally:
        asyncio.set_event_loop(None)
        loop.close()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        assert not conn.execute('SELECT 1 FROM repo_scripts WHERE repo_id=? AND script_name=?', (repo_id, script_name)).fetchone()
    app.config['SCHEDULED_INPUTS_DB'] = orig_db

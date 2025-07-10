import os
import sys
import shutil
import sqlite3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_repo_tables_created(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='input_repos'")
        assert cur.fetchone()
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='repo_scripts'")
        assert cur.fetchone()

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

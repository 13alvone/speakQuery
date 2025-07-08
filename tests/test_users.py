import os
import sys
import sqlite3
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_users_table_created(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'pass')
    initialize_database()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute('PRAGMA table_info(users)')
        cols = [c[1] for c in cursor.fetchall()]
        assert 'force_password_change' in cols
        cursor.execute('SELECT username, force_password_change FROM users')
        row = cursor.fetchone()
        assert row[0] == 'admin'
        assert row[1] == 1

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

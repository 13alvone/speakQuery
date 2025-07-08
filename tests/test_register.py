import os
import sys
import sqlite3
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def _setup_tmp_dbs(app, tmp_path, monkeypatch):
    orig_sched = app.config['SCHEDULED_INPUTS_DB']
    orig_saved = app.config['SAVED_SEARCHES_DB']
    tmp_sched = tmp_path / 'scheduled_inputs.db'
    tmp_saved = tmp_path / 'saved_searches.db'
    sqlite3.connect(tmp_sched).close()
    sqlite3.connect(tmp_saved).close()
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_saved)
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    from app import initialize_database
    initialize_database()
    return orig_sched, orig_saved, Path(tmp_sched)


def test_first_registration_is_admin(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_sched, orig_saved, db_path = _setup_tmp_dbs(app, tmp_path, monkeypatch)

    with sqlite3.connect(db_path) as conn:
        conn.execute('DELETE FROM users')
        conn.commit()

    client = app.test_client()
    resp = client.post('/register', json={'username': 'first', 'password': 'pw'})
    assert resp.status_code == 201

    with sqlite3.connect(db_path) as conn:
        role = conn.execute('SELECT role FROM users WHERE username=?', ('first',)).fetchone()[0]
    assert role == 'admin'

    app.config['SCHEDULED_INPUTS_DB'] = orig_sched
    app.config['SAVED_SEARCHES_DB'] = orig_saved


def test_registration_defaults_standard_user(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_sched, orig_saved, db_path = _setup_tmp_dbs(app, tmp_path, monkeypatch)

    client = app.test_client()
    resp = client.post('/register', json={'username': 'user1', 'password': 'pw'})
    assert resp.status_code == 201

    with sqlite3.connect(db_path) as conn:
        role = conn.execute('SELECT role FROM users WHERE username=?', ('user1',)).fetchone()[0]
    assert role == 'standard_user'

    resp = client.post('/register', json={'username': 'admin2', 'password': 'pw', 'is_admin': 'on'})
    assert resp.status_code == 201
    with sqlite3.connect(db_path) as conn:
        role = conn.execute('SELECT role FROM users WHERE username=?', ('admin2',)).fetchone()[0]
    assert role == 'admin'

    app.config['SCHEDULED_INPUTS_DB'] = orig_sched
    app.config['SAVED_SEARCHES_DB'] = orig_saved

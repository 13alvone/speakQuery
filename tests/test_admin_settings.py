import os
import sys
import sqlite3
import shutil
import hashlib
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_admin_required_for_settings(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    # Add a regular user
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user', generate_password_hash('pw'), 'user'),
        )
        conn.commit()

    client = app.test_client()

    resp = client.get('/get_settings')
    assert resp.status_code == 302

    resp = client.post('/login', json={'username': 'user', 'password': 'pw'})
    assert resp.status_code == 200
    resp = client.get('/get_settings')
    assert resp.status_code == 403
    client.get('/logout')

    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200
    resp = client.get('/get_settings')
    assert resp.status_code == 200

    resp = client.post('/update_settings', json={'settings': {'LOG_LEVEL': 'INFO'}})
    assert resp.status_code == 200

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

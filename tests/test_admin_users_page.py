import os
import sys
import sqlite3
import shutil
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def setup_db(app, tmp_path, monkeypatch):
    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    from app import initialize_database
    initialize_database()
    return orig_db, tmp_db


def test_user_creation_and_listing(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_db, db_path = setup_db(app, tmp_path, monkeypatch)

    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})

    resp = client.post('/users', json={'username': 'new', 'password': 'Passw0rd!', 'role': 'standard_user'})
    assert resp.status_code == 201

    resp = client.get('/users')
    assert resp.status_code == 200
    data = resp.get_json()['users']
    assert any(u['username'] == 'new' for u in data)

    resp = client.get('/users.html')
    assert resp.status_code == 200

    app.config['SCHEDULED_INPUTS_DB'] = orig_db


def test_users_endpoints_admin_only(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_db, db_path = setup_db(app, tmp_path, monkeypatch)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user', generate_password_hash('pw'), 'user')
        )
        conn.commit()

    client = app.test_client()
    # unauthenticated
    resp = client.get('/users')
    assert resp.status_code == 302
    resp = client.post('/users', json={})
    assert resp.status_code == 302
    resp = client.get('/users.html')
    assert resp.status_code == 302

    # standard user
    client.post('/login', json={'username': 'user', 'password': 'pw'})
    resp = client.get('/users')
    assert resp.status_code == 403
    resp = client.post('/users', json={'username': 'x', 'password': 'Passw0rd!'})
    assert resp.status_code == 403
    resp = client.get('/users.html')
    assert resp.status_code == 403
    client.get('/logout')

    # admin
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.get('/users')
    assert resp.status_code == 200
    resp = client.get('/users.html')
    assert resp.status_code == 200

    app.config['SCHEDULED_INPUTS_DB'] = orig_db



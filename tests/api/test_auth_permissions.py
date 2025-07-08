import os
import sys
import sqlite3
import hashlib
from werkzeug.security import generate_password_hash
import uuid
import time
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))


def _setup_db(app, tmp_path, monkeypatch):
    from app import initialize_database
    orig_sched = app.config['SCHEDULED_INPUTS_DB']
    orig_saved = app.config['SAVED_SEARCHES_DB']
    tmp_sched = tmp_path / 'scheduled_inputs.db'
    tmp_saved = tmp_path / 'saved_searches.db'
    # Start with fresh databases for each test
    sqlite3.connect(tmp_sched).close()
    sqlite3.connect(tmp_saved).close()
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_saved)
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    monkeypatch.setenv('ADMIN_API_TOKEN', 'admintoken')
    initialize_database()
    # Ensure 'disabled' column exists for saved_searches
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute('PRAGMA table_info(saved_searches)')
        cols = [c[1] for c in cursor.fetchall()]
        if 'disabled' not in cols:
            cursor.execute('ALTER TABLE saved_searches ADD COLUMN disabled INTEGER DEFAULT 0')
            conn.commit()
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user1', generate_password_hash('pw1'), 'user'),
        )
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user2', generate_password_hash('pw2'), 'user'),
        )
        conn.commit()
    return orig_sched, orig_saved


def test_login_and_token_usage(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_sched, orig_saved = _setup_db(app, tmp_path, monkeypatch)

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.cursor()
        cur.execute('SELECT api_token FROM users WHERE username=?', ('admin',))
        stored_hash = cur.fetchone()[0]
    assert stored_hash == hashlib.sha256(b'admintoken').hexdigest()

    client = app.test_client()
    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200
    client.get('/logout')
    resp = client.post('/login', json={'username': 'user1', 'password': 'pw1'})
    assert resp.status_code == 200
    client.get('/logout')

    headers = {'Authorization': 'Bearer admintoken'}
    search_id = f"{time.time()}_{uuid.uuid4()}"
    payload = {
        'request_id': search_id,
        'title': f'Test {uuid.uuid4()}',
        'description': 'desc',
        'query': 'index="dummy"',
        'cron_schedule': '* * * * *',
        'trigger': 'Once',
        'lookback': '-1h',
        'throttle': 'no',
        'throttle_time_period': '-1h',
        'throttle_by': 'user',
        'event_message': 'msg',
        'send_email': 'no',
        'email_address': 'test@example.com',
        'email_content': 'body'
    }

    resp = client.post('/api/saved_search', json=payload, headers=headers)
    assert resp.status_code == 201
    resp = client.delete(f'/api/saved_search/{search_id}', headers=headers)
    assert resp.status_code == 200

    app.config['SCHEDULED_INPUTS_DB'] = orig_sched
    app.config['SAVED_SEARCHES_DB'] = orig_saved


def test_admin_and_user_permissions(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_sched, orig_saved = _setup_db(app, tmp_path, monkeypatch)

    client = app.test_client()
    client.post('/login', json={'username': 'user1', 'password': 'pw1'})
    search_id1 = f"{time.time()}_{uuid.uuid4()}"
    payload = {
        'request_id': search_id1,
        'title': f'Test {uuid.uuid4()}',
        'description': 'desc',
        'query': 'index="dummy"',
        'cron_schedule': '* * * * *',
        'trigger': 'Once',
        'lookback': '-1h',
        'throttle': 'no',
        'throttle_time_period': '-1h',
        'throttle_by': 'user',
        'event_message': 'msg',
        'send_email': 'no',
        'email_address': 'test@example.com',
        'email_content': 'body'
    }
    resp = client.post('/api/saved_search', json=payload)
    assert resp.status_code == 201
    client.get('/logout')

    client.post('/login', json={'username': 'user2', 'password': 'pw2'})
    resp = client.delete(f'/api/saved_search/{search_id1}')
    assert resp.status_code == 403
    client.get('/logout')

    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.delete(f'/api/saved_search/{search_id1}')
    assert resp.status_code == 200
    client.get('/logout')

    app.config['SCHEDULED_INPUTS_DB'] = orig_sched
    app.config['SAVED_SEARCHES_DB'] = orig_saved

import os
import sys
import sqlite3
import hashlib
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
    shutil.copy(orig_sched, tmp_sched)
    shutil.copy(orig_saved, tmp_saved)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_saved)
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    monkeypatch.setenv('ADMIN_API_TOKEN', 'admintoken')
    initialize_database()
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user1', hashlib.sha256(b'pw1').hexdigest(), 'user'),
        )
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user2', hashlib.sha256(b'pw2').hexdigest(), 'user'),
        )
        conn.commit()
    return orig_sched, orig_saved


def test_login_and_token(mock_heavy_modules, tmp_path, monkeypatch):
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


def test_ownership_restrictions(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app
    orig_sched, orig_saved = _setup_db(app, tmp_path, monkeypatch)

    client = app.test_client()
    client.post('/login', json={'username': 'user1', 'password': 'pw1'})
    search_id = f"{time.time()}_{uuid.uuid4()}"
    base_payload = {
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
    resp = client.post('/api/saved_search', json=base_payload)
    assert resp.status_code == 201

    update_payload = dict(base_payload)
    update_payload['title'] = 'changed'
    update_payload['disabled'] = False
    resp = client.patch(f'/api/saved_search/{search_id}', json=update_payload)
    assert resp.status_code == 200
    client.get('/logout')

    client.post('/login', json={'username': 'user2', 'password': 'pw2'})
    resp = client.patch(f'/api/saved_search/{search_id}', json=update_payload)
    assert resp.status_code == 403
    resp = client.delete(f'/api/saved_search/{search_id}')
    assert resp.status_code == 403
    client.get('/logout')

    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.patch(f'/api/saved_search/{search_id}', json=update_payload)
    assert resp.status_code == 200
    resp = client.delete(f'/api/saved_search/{search_id}')
    assert resp.status_code == 200
    client.get('/logout')

    app.config['SCHEDULED_INPUTS_DB'] = orig_sched
    app.config['SAVED_SEARCHES_DB'] = orig_saved

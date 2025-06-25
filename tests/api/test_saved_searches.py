import os
import sys
import uuid
import time
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_saved_search_crud(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)

    orig_sched = app.config['SCHEDULED_INPUTS_DB']
    tmp_sched = tmp_path / 'sched.db'
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)
    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    monkeypatch.setattr('routes.api.is_title_unique', lambda t: True)
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})

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

    resp = client.post('/api/saved_search', json=payload)
    assert resp.status_code == 201

    resp = client.get(f'/api/saved_search/{search_id}')
    assert resp.status_code == 200
    meta = resp.get_json()['search']
    assert meta['id'] == search_id

    payload_update = payload.copy()
    payload_update['title'] = 'Updated'
    payload_update['disabled'] = 0
    resp = client.patch(f'/api/saved_search/{search_id}', json=payload_update)
    assert resp.status_code == 200

    resp = client.delete(f'/api/saved_search/{search_id}')
    assert resp.status_code == 200

    app.config['SAVED_SEARCHES_DB'] = orig_db
    app.config['SCHEDULED_INPUTS_DB'] = orig_sched


def test_saved_search_owner_restriction(mock_heavy_modules, tmp_path, monkeypatch):
    import sqlite3
    import hashlib
    from werkzeug.security import generate_password_hash
    from app import app, initialize_database

    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)

    orig_sched = app.config['SCHEDULED_INPUTS_DB']
    tmp_sched = tmp_path / 'sched.db'
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    # add user1 and user2
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user1', generate_password_hash('pw1'), 'user')
        )
        cursor.execute(
            'INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
            ('user2', generate_password_hash('pw2'), 'user')
        )
        conn.commit()

    client = app.test_client()
    client.post('/login', json={'username': 'user1', 'password': 'pw1'})

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

    resp = client.post('/api/saved_search', json=payload)
    assert resp.status_code == 201
    client.get('/logout')

    client.post('/login', json={'username': 'user2', 'password': 'pw2'})
    resp = client.delete(f'/api/saved_search/{search_id}')
    assert resp.status_code == 403
    client.get('/logout')

    client.post('/login', json={'username': 'user1', 'password': 'pw1'})
    resp = client.delete(f'/api/saved_search/{search_id}')
    assert resp.status_code == 200

    app.config['SAVED_SEARCHES_DB'] = orig_db
    app.config['SCHEDULED_INPUTS_DB'] = orig_sched

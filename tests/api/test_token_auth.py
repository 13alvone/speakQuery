import os
import sys
import sqlite3
import uuid
import time
import hashlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

def test_api_token_auth(mock_heavy_modules, monkeypatch, tmp_path):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'sched.db'
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    monkeypatch.setenv('ADMIN_API_TOKEN', 'secrettoken')
    initialize_database()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.cursor()
        cur.execute('SELECT api_token FROM users WHERE username=?', ('admin',))
        stored_hash = cur.fetchone()[0]
    assert stored_hash == hashlib.sha256(b'secrettoken').hexdigest()

    client = app.test_client()
    headers = {'Authorization': 'Bearer secrettoken'}

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

    app.config['SCHEDULED_INPUTS_DB'] = orig_db


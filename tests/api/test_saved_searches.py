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
    initialize_database()

    monkeypatch.setattr('routes.api.is_title_unique', lambda t: True)
    client = app.test_client()

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

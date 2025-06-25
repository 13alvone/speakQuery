import os
import sys
import uuid
import time
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_api_query_metadata(mock_heavy_modules, monkeypatch):
    from app import app

    df = pd.DataFrame({'a': [1]})
    monkeypatch.setattr('app.execute_speakQuery', lambda q: df)
    monkeypatch.setattr('app.save_dataframe', lambda *a, **k: None)
    monkeypatch.setattr('routes.api.execute_speakQuery', lambda q: df)
    monkeypatch.setattr('routes.api.save_dataframe', lambda *a, **k: None)

    client = app.test_client()
    resp = client.post('/api/query', json={'query': 'index="dummy"'})
    data = resp.get_json()
    assert resp.status_code == 200
    assert data['status'] == 'success'
    assert 'time_sent' in data and 'time_received' in data and 'duration_ms' in data


def test_api_saved_search_crud(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
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


def test_api_saved_search_settings(mock_heavy_modules, monkeypatch):
    from app import app, initialize_database
    initialize_database()

    fixed_time = '2023-01-01T00:00:00Z'
    monkeypatch.setattr('routes.api.get_next_runtime', lambda s: fixed_time)

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

    resp = client.get(f'/api/saved_search/{search_id}/settings')
    assert resp.status_code == 200
    data = resp.get_json()['search']
    assert data['id'] == search_id
    assert data['next_scheduled_time'] == fixed_time

    client.delete(f'/api/saved_search/{search_id}')


import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_query_execution(mock_heavy_modules, monkeypatch):
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
    assert data['results'] == [{'a': 1}]
    assert 'time_sent' in data and 'time_received' in data and 'duration_ms' in data

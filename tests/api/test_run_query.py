import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_run_query_endpoint(mock_heavy_modules, monkeypatch):
    from app import app

    df = pd.DataFrame({'a': [1]})
    monkeypatch.setattr('app.execute_speakQuery', lambda q: df)
    monkeypatch.setattr('app.save_dataframe', lambda *a, **k: None)

    client = app.test_client()
    resp = client.post('/run_query', json={'query': 'index="dummy"'})
    data = resp.get_json()

    assert resp.status_code == 200
    assert data['status'] == 'success'
    assert 'request_id' in data

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

class MockResp:
    def __init__(self, data=None):
        self._data = data or {}
        self.status_code = 200
    def raise_for_status(self):
        pass
    def json(self):
        return self._data


def test_fetch_api_data_allowed(mock_heavy_modules, monkeypatch):
    from app import app
    client = app.test_client()
    def mock_get(url, timeout=10):
        assert url.startswith('https://')
        return MockResp({'ok': True})
    monkeypatch.setattr('app.requests.get', mock_get)
    resp = client.post('/fetch_api_data', json={'api_url': 'https://jsonplaceholder.typicode.com/todos/1'})
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'
    assert resp.get_json()['api_data'] == {'ok': True}


def test_fetch_api_data_blocked(mock_heavy_modules, monkeypatch):
    from app import app
    client = app.test_client()
    called = {'called': False}
    def mock_get(url, timeout=10):
        called['called'] = True
        return MockResp()
    monkeypatch.setattr('app.requests.get', mock_get)
    resp = client.post('/fetch_api_data', json={'api_url': 'https://example.com/data'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    assert not called['called']

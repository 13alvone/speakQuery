import os
import sys
import types

# Mock heavy modules before importing app
mock_modules = {
    'lexers.antlr4_active.speakQueryLexer': types.SimpleNamespace(speakQueryLexer=object),
    'lexers.antlr4_active.speakQueryParser': types.SimpleNamespace(speakQueryParser=object),
    'handlers.JavaHandler': types.SimpleNamespace(JavaHandler=lambda *a, **k: None),
    'validation.SavedSearchValidation': types.SimpleNamespace(SavedSearchValidation=lambda *a, **k: None),
    'functionality.FindNextCron': types.SimpleNamespace(suggest_next_cron_runtime=lambda *a, **k: None),
    'scheduled_input_engine.ScheduledInputEngine': types.SimpleNamespace(
        ScheduledInputBackend=lambda *a, **k: None,
        crank_scheduled_input_engine=lambda: None,
    ),
    'query_engine.QueryEngine': types.SimpleNamespace(crank_query_engine=lambda *a, **k: None),
    'scheduled_input_engine.SIExecution': types.SimpleNamespace(SIExecution=lambda *a, **k: None),
}
sys.modules.update(mock_modules)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from app import app

client = app.test_client()

class MockResp:
    def __init__(self, data=None):
        self._data = data or {}
        self.status_code = 200
    def raise_for_status(self):
        pass
    def json(self):
        return self._data


def test_fetch_api_data_allowed(monkeypatch):
    def mock_get(url, timeout=10):
        assert url.startswith('https://')
        return MockResp({'ok': True})
    monkeypatch.setattr('app.requests.get', mock_get)
    resp = client.post('/fetch_api_data', json={'api_url': 'https://jsonplaceholder.typicode.com/todos/1'})
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'
    assert resp.get_json()['api_data'] == {'ok': True}


def test_fetch_api_data_blocked(monkeypatch):
    called = {'called': False}
    def mock_get(url, timeout=10):
        called['called'] = True
        return MockResp()
    monkeypatch.setattr('app.requests.get', mock_get)
    resp = client.post('/fetch_api_data', json={'api_url': 'https://example.com/data'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    assert not called['called']

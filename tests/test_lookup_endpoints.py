import os
import sys
import types

# Mock heavy modules before importing app
mock_modules = {
    'lexers.antlr4_active.speakQueryLexer': types.SimpleNamespace(speakQueryLexer=object),
    'lexers.antlr4_active.speakQueryParser': types.SimpleNamespace(speakQueryParser=object),
    'lexers.speakQueryListener': types.SimpleNamespace(speakQueryListener=object),
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


def test_view_lookup_missing_filepath():
    resp = client.get('/view_lookup')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_view_lookup_empty_filepath():
    resp = client.get('/view_lookup?file=')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_delete_lookup_file_missing_filepath():
    resp = client.post('/delete_lookup_file', json={})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_delete_lookup_file_empty_filepath():
    resp = client.post('/delete_lookup_file', json={'filepath': ''})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_missing_filepath():
    resp = client.post('/clone_lookup_file', json={'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_empty_filepath():
    resp = client.post('/clone_lookup_file', json={'filepath': '', 'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'

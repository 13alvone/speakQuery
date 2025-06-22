import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_view_lookup_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.get('/view_lookup')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_view_lookup_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.get('/view_lookup?file=')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_delete_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.post('/delete_lookup_file', json={})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_delete_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.post('/delete_lookup_file', json={'filepath': ''})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.post('/clone_lookup_file', json={'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    resp = client.post('/clone_lookup_file', json={'filepath': '', 'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'

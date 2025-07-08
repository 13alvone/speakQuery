import os
import sys
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_page_requires_login(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    client = app.test_client()

    resp = client.get('/')
    assert resp.status_code == 302
    assert '/login.html' in resp.headers.get('Location', '')

    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200

    resp = client.get('/')
    assert resp.status_code == 200

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

import os
import sys
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_force_password_change_redirect_and_navbar(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    client = app.test_client()

    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200
    assert resp.get_json()['force_password_change'] is True

    resp = client.get('/lookups.html')
    assert resp.status_code == 302
    assert '/change_password.html' in resp.headers.get('Location', '')

    resp = client.get('/change_password.html')
    assert resp.status_code == 200
    page = resp.get_data(as_text=True)
    assert '<nav' not in page

    resp = client.post('/change_password', json={'old_password': 'admin', 'new_password': 'Newpass1!'})
    assert resp.status_code == 200

    resp = client.get('/lookups.html')
    assert resp.status_code == 200
    page = resp.get_data(as_text=True)
    assert '<nav' in page

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

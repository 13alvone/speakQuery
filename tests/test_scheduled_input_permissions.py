import os
import sys
import shutil
import sqlite3
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.mark.parametrize('endpoint', [
    '/toggle_disable_scheduled_input/1',
    '/delete_scheduled_input/1',
    '/clone_scheduled_input/1',
])
def test_scheduled_input_endpoints_require_login(mock_heavy_modules, tmp_path, monkeypatch, endpoint):
    import app as app_module
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()

    resp = client.post(endpoint)
    assert resp.status_code == 302

    app.config['SCHEDULED_INPUTS_DB'] = orig_db
    app.config['WTF_CSRF_ENABLED'] = True


@pytest.mark.parametrize('endpoint', [
    '/toggle_disable_scheduled_input/1',
    '/delete_scheduled_input/1',
    '/clone_scheduled_input/1',
])
def test_scheduled_input_endpoints_require_csrf(mock_heavy_modules, tmp_path, monkeypatch, endpoint):
    import app as app_module
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        conn.execute("UPDATE users SET force_password_change = 0 WHERE username='admin'")
        conn.commit()

    resp = client.post(endpoint)
    assert resp.status_code == 400

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

import os
import sys
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_login_logout(mock_heavy_modules, tmp_path, monkeypatch):
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
    data = resp.get_json()
    assert data['status'] == 'success'
    assert data['force_password_change'] is True

    resp = client.post(
        '/change_password',
        json={'old_password': 'admin', 'new_password': 'Newpass1!'}
    )
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'

    client.get('/logout')

    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 401

    resp = client.post('/login', json={'username': 'admin', 'password': 'Newpass1!'})
    assert resp.status_code == 200
    assert resp.get_json()['force_password_change'] is False

    resp = client.get('/logout')
    assert resp.status_code == 200

    resp = client.post('/login', json={'username': 'admin', 'password': 'bad'})
    assert resp.status_code == 401

    app.config['SCHEDULED_INPUTS_DB'] = orig_db


def test_login_rate_limit(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database, load_settings_into_config, limiter

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()
    load_settings_into_config()
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)
    limiter.enabled = True
    limiter.reset()

    client = app.test_client()

    for _ in range(5):
        resp = client.post('/login', json={'username': 'admin', 'password': 'bad'})
        assert resp.status_code == 401

    resp = client.post('/login', json={'username': 'admin', 'password': 'bad'})
    assert resp.status_code == 429

    limiter.enabled = False
    limiter.reset()
    app.config['SCHEDULED_INPUTS_DB'] = orig_db

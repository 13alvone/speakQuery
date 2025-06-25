import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_view_lookup_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.get('/view_lookup')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_view_lookup_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.get('/view_lookup?file=')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_delete_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/delete_lookup_file', json={})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_delete_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/delete_lookup_file', json={'filepath': ''})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/clone_lookup_file', json={'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    resp = client.post('/clone_lookup_file', json={'filepath': '', 'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_upload_file_valid_csv(mock_heavy_modules, tmp_path):
    from app import app
    import io

    app.config['LOOKUP_DIR'] = str(tmp_path)
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})

    data = {'file': (io.BytesIO(b'a,b\n1,2\n'), 'good.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'
    assert (tmp_path / 'good.csv').exists()


def test_upload_file_invalid_csv(mock_heavy_modules, tmp_path):
    from app import app
    import io

    app.config['LOOKUP_DIR'] = str(tmp_path)
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})

    data = {'file': (io.BytesIO(b'{"json": true}'), 'bad.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    assert not (tmp_path / 'bad.csv').exists()


def test_lookup_file_owner_restriction(mock_heavy_modules, tmp_path, monkeypatch):
    import io
    import sqlite3
    import hashlib
    import shutil
    from app import app, initialize_database

    app.config['LOOKUP_DIR'] = str(tmp_path)

    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)

    tmp_sched = tmp_path / 'sched.db'
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_sched)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.cursor()
        cur.execute('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                    ('user1', hashlib.sha256(b"pw1").hexdigest(), 'user'))
        cur.execute('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                    ('user2', hashlib.sha256(b"pw2").hexdigest(), 'user'))
        conn.commit()

    client = app.test_client()
    client.post('/login', json={'username': 'user1', 'password': 'pw1'})
    data = {'file': (io.BytesIO(b'a,b\n1,2\n'), 'own.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 200
    client.get('/logout')

    client.post('/login', json={'username': 'user2', 'password': 'pw2'})
    resp = client.post('/delete_lookup_file', json={'filepath': str(tmp_path / 'own.csv')})
    assert resp.status_code == 403

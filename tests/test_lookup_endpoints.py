import os
import sys
from pathlib import Path
import sqlite3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def login_as_admin(app, client):
    client.post('/login', json={'username': 'admin', 'password': 'admin'})
    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        conn.execute("UPDATE users SET force_password_change=0 WHERE username='admin'")
        conn.commit()


def test_view_lookup_missing_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.get('/view_lookup')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_view_lookup_empty_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.get('/view_lookup?file=')
    assert resp.status_code == 400
    assert b"No file specified" in resp.data


def test_delete_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.post('/delete_lookup_file', json={})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_delete_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.post('/delete_lookup_file', json={'filepath': ''})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_missing_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.post('/clone_lookup_file', json={'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_clone_lookup_file_empty_filepath(mock_heavy_modules):
    from app import app, initialize_database
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)
    resp = client.post('/clone_lookup_file', json={'filepath': '', 'new_name': 'copy.csv'})
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'


def test_upload_file_valid_csv(mock_heavy_modules, tmp_path):
    from app import app, initialize_database
    import io

    app.config['LOOKUP_DIR'] = str(tmp_path)
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)

    data = {'file': (io.BytesIO(b'a,b\n1,2\n'), 'good.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'
    assert (tmp_path / 'good.csv').exists()


def test_upload_file_invalid_csv(mock_heavy_modules, tmp_path):
    from app import app, initialize_database
    import io

    app.config['LOOKUP_DIR'] = str(tmp_path)
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)

    data = {'file': (io.BytesIO(b'{"json": true}'), 'bad.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 400
    assert resp.get_json()['status'] == 'error'
    assert not (tmp_path / 'bad.csv').exists()


def test_upload_file_too_large(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database
    import io

    app.config['LOOKUP_DIR'] = str(tmp_path)
    monkeypatch.setitem(app.config, 'MAX_CONTENT_LENGTH', 10 * 1024 * 1024)
    monkeypatch.setitem(app.config, 'LOOKUP_MAX_FILESIZE', 100)
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)

    data = {'file': (io.BytesIO(b'a' * 200), 'big.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 413
    assert resp.get_json()['status'] == 'error'
    assert not (tmp_path / 'big.csv').exists()


def test_lookup_file_owner_restriction(mock_heavy_modules, tmp_path, monkeypatch):
    import io
    import sqlite3
    import hashlib
    from werkzeug.security import generate_password_hash
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
                    ('user1', generate_password_hash('pw1'), 'user'))
        cur.execute('INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)',
                    ('user2', generate_password_hash('pw2'), 'user'))
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


def test_view_lookup_large_file_limited_rows(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database
    from routes import lookups
    pd = lookups.pd

    test_dir = Path('test_lookups')
    test_dir.mkdir(exist_ok=True)
    app.config['LOOKUP_DIR'] = str(test_dir)
    initialize_database()

    large_file = test_dir / 'large.csv'
    with large_file.open('w', encoding='utf-8') as fh:
        fh.write('a\n')
        for i in range(5000):
            fh.write(f"{i}\n")

    client = app.test_client()
    login_as_admin(app, client)

    called = {}

    real_read_csv = pd.read_csv

    def fake_read_csv(path, *args, **kwargs):
        called['nrows'] = kwargs.get('nrows')
        return real_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(pd, 'read_csv', fake_read_csv)

    resp = client.get(f'/view_lookup?file={large_file}')
    assert resp.status_code == 200
    assert called.get('nrows') is not None and called['nrows'] <= 100


def test_lookup_endpoints_path_traversal(mock_heavy_modules, tmp_path):
    from app import app, initialize_database

    lookup_dir = tmp_path / 'lookups'
    lookup_dir.mkdir()
    outside_file = tmp_path / 'outside.csv'
    outside_file.write_text('a,b\n1,2\n', encoding='utf-8')

    app.config['LOOKUP_DIR'] = str(lookup_dir)
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)

    traversal_path = lookup_dir / '..' / outside_file.name
    resp = client.get(f'/view_lookup?file={traversal_path}')
    assert resp.status_code == 403
    resp = client.post('/delete_lookup_file', json={'filepath': str(traversal_path)})
    assert resp.status_code == 403
    resp = client.post('/clone_lookup_file', json={'filepath': str(traversal_path), 'new_name': 'copy.csv'})
    assert resp.status_code == 403


def test_lookup_endpoints_symlink_traversal(mock_heavy_modules, tmp_path):
    from app import app, initialize_database

    lookup_dir = tmp_path / 'lookups'
    lookup_dir.mkdir()
    outside_file = tmp_path / 'outside.csv'
    outside_file.write_text('a,b\n1,2\n', encoding='utf-8')
    symlink_path = lookup_dir / 'link.csv'
    symlink_path.symlink_to(outside_file)

    app.config['LOOKUP_DIR'] = str(lookup_dir)
    initialize_database()
    client = app.test_client()
    login_as_admin(app, client)

    resp = client.get(f'/view_lookup?file={symlink_path}')
    assert resp.status_code == 403
    resp = client.post('/delete_lookup_file', json={'filepath': str(symlink_path)})
    assert resp.status_code == 403
    resp = client.post('/clone_lookup_file', json={'filepath': str(symlink_path), 'new_name': 'copy.csv'})
    assert resp.status_code == 403

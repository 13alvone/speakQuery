import os
import sys
import shutil
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_page_requires_login(mock_heavy_modules, tmp_path, monkeypatch):
    # Stub out heavy antlr4 dependency before importing the app
    monkeypatch.setitem(sys.modules, 'antlr4', types.SimpleNamespace(
        InputStream=object,
        CommonTokenStream=object,
        ParseTreeWalker=object
    ))

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

    # Login page should not show the navbar when unauthenticated
    resp = client.get('/login.html')
    assert resp.status_code == 200
    page = resp.get_data(as_text=True)
    assert '<nav' not in page

    resp = client.post('/login', json={'username': 'admin', 'password': 'admin'})
    assert resp.status_code == 200

    # Access is redirected due to forced password change
    resp = client.get('/')
    assert resp.status_code == 302
    assert '/change_password.html' in resp.headers.get('Location', '')

    app.config['SCHEDULED_INPUTS_DB'] = orig_db


def test_login_page_redirects_when_authenticated(mock_heavy_modules, tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, 'antlr4', types.SimpleNamespace(
        InputStream=object,
        CommonTokenStream=object,
        ParseTreeWalker=object
    ))

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

    resp = client.get('/login.html')
    assert resp.status_code == 302
    assert resp.headers['Location'].endswith('/change_password.html')

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

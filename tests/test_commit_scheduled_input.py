import os
import sys
import shutil
import sqlite3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_commit_scheduled_input_requires_login(mock_heavy_modules, tmp_path, monkeypatch):
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

    resp = client.post(
        '/commit_scheduled_input',
        data={
            'title': 'test',
            'description': '',
            'code': 'print(1)',
            'cron_schedule': '* * * * *',
            'overwrite': 'true',
        },
    )
    assert resp.status_code == 302

    app.config['SCHEDULED_INPUTS_DB'] = orig_db
    app.config['WTF_CSRF_ENABLED'] = True


def test_commit_scheduled_input_with_api_url(mock_heavy_modules, tmp_path, monkeypatch):
    import app as app_module
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    shutil.copy(orig_db, tmp_db)
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    class DummyExec:
        def __init__(self, *a, **k):
            pass
        def execute_code_test(self):
            return None

    class DummyBackend:
        def __init__(self, db):
            self.SCHEDULED_INPUTS_DB = db
            self.initialize_database()
        def initialize_database(self):
            with sqlite3.connect(self.SCHEDULED_INPUTS_DB) as conn:
                conn.execute(
                    '''CREATE TABLE IF NOT EXISTS scheduled_inputs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT UNIQUE,
                        description TEXT,
                        code TEXT,
                        cron_schedule TEXT,
                        overwrite BOOLEAN,
                        subdirectory TEXT,
                        api_url TEXT,
                        created_at REAL,
                        disabled BOOLEAN DEFAULT 0
                    )'''
                )
                cur = conn.execute("PRAGMA table_info(scheduled_inputs)")
                cols = [r[1] for r in cur.fetchall()]
                if 'api_url' not in cols:
                    conn.execute('ALTER TABLE scheduled_inputs ADD COLUMN api_url TEXT')
                conn.commit()
        def add_scheduled_input(self, **kw):
            with sqlite3.connect(self.SCHEDULED_INPUTS_DB) as conn:
                conn.execute(
                    'INSERT INTO scheduled_inputs (title, description, code, cron_schedule, overwrite, subdirectory, api_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (
                        kw['title'],
                        kw['description'],
                        kw['code'],
                        kw['cron_schedule'],
                        kw['overwrite'].lower() == 'true',
                        kw['subdirectory'],
                        kw.get('api_url'),
                        0,
                    ),
                )
                conn.commit()

    app_module.SIExecution = DummyExec
    app_module.scheduled_input_backend = DummyBackend(str(tmp_db))
    app.config['WTF_CSRF_ENABLED'] = False
    client = app.test_client()
    client.post('/login', json={'username': 'admin', 'password': 'admin'})

    resp = client.post(
        '/commit_scheduled_input',
        json={
            'title': 't',
            'description': 'd',
            'code': 'print(1)',
            'cron_schedule': '* * * * *',
            'overwrite': 'true',
            'subdirectory': '',
            'api_url': 'https://jsonplaceholder.typicode.com/todos/1',
        },
    )
    assert resp.status_code == 200
    assert resp.get_json()['status'] == 'success'

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        cur = conn.execute('SELECT api_url FROM scheduled_inputs WHERE title="t"')
        row = cur.fetchone()
    assert row and row[0] == 'https://jsonplaceholder.typicode.com/todos/1'

    app.config['SCHEDULED_INPUTS_DB'] = orig_db
    app.config['WTF_CSRF_ENABLED'] = True

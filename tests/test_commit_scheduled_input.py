import os
import sys
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_commit_scheduled_input_requires_login(mock_heavy_modules, tmp_path, monkeypatch):
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
    assert resp.status_code == 401

    app.config['SCHEDULED_INPUTS_DB'] = orig_db
    app.config['WTF_CSRF_ENABLED'] = True

import os
import sys
import sqlite3
import hashlib
from werkzeug.security import generate_password_hash

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_cli_admin_limit(mock_heavy_modules, tmp_path, monkeypatch):
    from app import app, initialize_database

    orig_db = app.config['SCHEDULED_INPUTS_DB']
    tmp_db = tmp_path / 'scheduled_inputs.db'
    sqlite3.connect(tmp_db).close()
    app.config['SCHEDULED_INPUTS_DB'] = str(tmp_db)

    monkeypatch.setenv('ADMIN_USERNAME', 'admin')
    monkeypatch.setenv('ADMIN_PASSWORD', 'admin')
    initialize_database()

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        for i in range(9):
            conn.execute(
                'INSERT INTO users (username, password_hash, role, api_token) VALUES (?, ?, ?, ?)',
                (
                    f'a{i}',
                    generate_password_hash('pw'),
                    'admin',
                    hashlib.sha256(f't{i}'.encode()).hexdigest(),
                ),
            )
        conn.commit()

    initialize_database('extra', 'Passw0rd!', 'admin', 'tok')

    with sqlite3.connect(app.config['SCHEDULED_INPUTS_DB']) as conn:
        count = conn.execute('SELECT COUNT(*) FROM users WHERE role=?', ('admin',)).fetchone()[0]
        extra = conn.execute('SELECT COUNT(*) FROM users WHERE username=?', ('extra',)).fetchone()[0]
    assert count == 10
    assert extra == 0

    app.config['SCHEDULED_INPUTS_DB'] = orig_db

import os
import sys
import sqlite3
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_ban_after_failed_deletes(mock_heavy_modules, tmp_path):
    from app import app, initialize_database

    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)
    app.config['LOOKUP_DIR'] = str(tmp_path)
    app.config['BAN_DELETIONS_ENABLED'] = True
    app.config['BAN_DURATION'] = 1
    initialize_database()

    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        conn.execute('DELETE FROM api_bans')
        conn.commit()

    client = app.test_client()

    for _ in range(5):
        resp = client.delete('/api/lookup/missing.csv')
        assert resp.status_code == 404
    resp = client.delete('/api/lookup/missing.csv')
    assert resp.status_code == 403

    app.config['BAN_DELETIONS_ENABLED'] = False
    app.config['SAVED_SEARCHES_DB'] = orig_db

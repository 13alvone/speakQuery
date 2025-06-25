import os
import sys
import sqlite3
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def _reset_ban_db(app):
    with sqlite3.connect(app.config['SAVED_SEARCHES_DB']) as conn:
        conn.execute('DELETE FROM api_bans')
        conn.commit()


def test_saved_search_delete_ban(mock_heavy_modules, tmp_path):
    from app import app, initialize_database
    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)
    app.config['BAN_DELETIONS_ENABLED'] = True
    app.config['BAN_DURATION'] = 1
    initialize_database()
    _reset_ban_db(app)
    client = app.test_client()
    for _ in range(5):
        resp = client.delete('/api/saved_search/no-such-id')
        assert resp.status_code in {404, 500}
    resp = client.delete('/api/saved_search/no-such-id')
    assert resp.status_code == 403
    app.config['BAN_DELETIONS_ENABLED'] = False
    app.config['SAVED_SEARCHES_DB'] = orig_db


def test_lookup_delete_ban(mock_heavy_modules, tmp_path):
    from app import app, initialize_database
    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)
    app.config['BAN_DELETIONS_ENABLED'] = True
    app.config['BAN_DURATION'] = 1
    app.config['LOOKUP_DIR'] = str(tmp_path)
    initialize_database()
    _reset_ban_db(app)
    client = app.test_client()
    for _ in range(5):
        resp = client.delete('/api/lookup/missing.csv')
        assert resp.status_code == 404
    resp = client.delete('/api/lookup/missing.csv')
    assert resp.status_code == 403
    app.config['BAN_DELETIONS_ENABLED'] = False
    app.config['SAVED_SEARCHES_DB'] = orig_db

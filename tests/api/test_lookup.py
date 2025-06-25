import os
import sys
import io
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_lookup_upload_and_delete(mock_heavy_modules, tmp_path):
    from app import app, initialize_database

    orig_db = app.config['SAVED_SEARCHES_DB']
    tmp_db = tmp_path / 'db.sqlite3'
    shutil.copy(orig_db, tmp_db)
    app.config['SAVED_SEARCHES_DB'] = str(tmp_db)
    app.config['LOOKUP_DIR'] = str(tmp_path)
    initialize_database()

    client = app.test_client()

    data = {'file': (io.BytesIO(b'a,b\n1,2\n'), 'test.csv')}
    resp = client.post('/upload_file', data=data, content_type='multipart/form-data')
    assert resp.status_code == 200
    assert (tmp_path / 'test.csv').exists()

    resp = client.delete('/api/lookup/test.csv')
    assert resp.status_code == 200
    assert not (tmp_path / 'test.csv').exists()

    app.config['SAVED_SEARCHES_DB'] = orig_db

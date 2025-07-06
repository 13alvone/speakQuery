import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))


def test_directory_tree_missing_dir(mock_heavy_modules, tmp_path):
    from app import app

    orig_dir = app.config['INDEXES_DIR']
    missing = tmp_path / 'missing'
    app.config['INDEXES_DIR'] = str(missing)

    client = app.test_client()
    resp = client.get('/get_directory_tree')
    data = resp.get_json()

    assert resp.status_code == 404
    assert data['status'] == 'error'
    assert data['message'] == 'Indexes directory not found'

    app.config['INDEXES_DIR'] = orig_dir

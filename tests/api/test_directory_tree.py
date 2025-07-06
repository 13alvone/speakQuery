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


def test_directory_tree_listing(mock_heavy_modules, tmp_path):
    """Verify /get_directory_tree returns all files and directories."""
    from app import app

    orig_dir = app.config['INDEXES_DIR']
    root = tmp_path / 'indexes'
    root.mkdir()

    (root / 'archive').mkdir()
    (root / 'archive' / 'ignored.parquet').touch()

    (root / 'root.parquet').touch()
    (root / 'root.db').touch()
    dir1 = root / 'dir1'
    dir1.mkdir()
    (dir1 / 'file1.parquet').touch()
    (dir1 / 'notes.txt').touch()
    sub = dir1 / 'sub'
    sub.mkdir()
    (sub / 'file2.parquet').touch()

    dir2 = root / 'dir2'
    dir2.mkdir()
    (dir2 / 'file3.parquet').touch()

    (root / 'other.txt').touch()
    empty = root / 'empty_dir'
    empty.mkdir()

    app.config['INDEXES_DIR'] = str(root)

    client = app.test_client()
    resp = client.get('/get_directory_tree')
    data = resp.get_json()

    assert resp.status_code == 200
    assert data['status'] == 'success'

    def gather(tree):
        paths = [f['path'] for f in tree['files']]
        for sub in tree['dirs'].values():
            paths.extend(gather(sub))
        return paths

    expected = {
        'root.parquet',
        'root.db',
        'other.txt',
        'dir1/file1.parquet',
        'dir1/notes.txt',
        'dir1/sub/file2.parquet',
        'dir2/file3.parquet',
    }
    assert set(gather(data['tree'])) == expected

    assert set(data['tree']['dirs']) == {'dir1', 'dir2', 'empty_dir'}
    assert 'archive' not in data['tree']['dirs']

    app.config['INDEXES_DIR'] = orig_dir

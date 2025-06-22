import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_multi_dot_extension_allowed(mock_heavy_modules):
    from app import allowed_file
    assert allowed_file('example.system4.system4.parquet')


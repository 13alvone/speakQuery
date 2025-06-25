import os
import sys
import csv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def test_get_row_count_large_csv_no_pandas(mock_heavy_modules, tmp_path, monkeypatch):
    from app import get_row_count
    large_csv = tmp_path / "big.csv"
    with large_csv.open("w", encoding="utf-8") as f:
        f.write("col\n")
        for i in range(10000):
            f.write(f"{i}\n")

    def raise_called(*args, **kwargs):
        raise AssertionError("pandas.read_csv should not be called")

    monkeypatch.setattr('pandas.read_csv', raise_called)
    count = get_row_count(str(large_csv))
    assert count == 10000

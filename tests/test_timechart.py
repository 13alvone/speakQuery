import pytest

pytest.importorskip("pandas")

import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from handlers.ChartHandler import ChartHandler


def run_chart(tokens, df):
    handler = ChartHandler()
    return handler.run_timechart(tokens, df)


def test_timechart_basic():
    df = pd.DataFrame({
        "_time": ["2024-01-01 00:10:00", "2024-01-01 00:40:00"],
        "value": [1, 2],
    })
    result = run_chart(['timechart', 'span=1hours', 'sum(value)', 'as', 'total'], df)
    assert result["_time"].tolist() == [pd.Timestamp("2024-01-01 00:00:00")]
    assert result["total"].iloc[0] == 3


def test_timechart_with_by():
    df = pd.DataFrame({
        "_time": [
            "2024-01-01 00:15:00",
            "2024-01-01 00:45:00",
            "2024-01-01 01:05:00",
        ],
        "page": ["A", "A", "B"],
    })
    result = run_chart(['timechart', 'span=1hours', 'count', 'by', 'page'], df)
    pages = dict(zip(result["page"], result["count"]))
    assert pages.get("A") == 2
    assert pages.get("B") == 1

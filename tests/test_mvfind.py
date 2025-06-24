import pytest

pytest.importorskip("pandas")

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from handlers.GeneralHandler import GeneralHandler

def test_execute_mvfind_basic():
    df = pd.DataFrame({"vals": [["a", "b", "urgent"], ["login", "urgent"], ["none"]]})
    gh = GeneralHandler()
    result = gh.execute_mvfind(df.copy(), "vals", "urgent")
    assert result["mvfind"].tolist() == [2, 1, -1]

def test_execute_mvfind_no_match():
    df = pd.DataFrame({"vals": [["a", "b"], []]})
    gh = GeneralHandler()
    result = gh.execute_mvfind(df.copy(), "vals", "urgent")
    assert result["mvfind"].tolist() == [-1, -1]

def test_execute_mvfind_mixed_values():
    df = pd.DataFrame({"vals": ["urgent", None, ["x", "y"]]})
    gh = GeneralHandler()
    result = gh.execute_mvfind(df.copy(), "vals", "urgent")
    assert result["mvfind"].tolist() == [0, -1, -1]

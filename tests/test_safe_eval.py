import pytest

pytest.importorskip("pandas")

import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_custom_eval_basic(mock_heavy_modules):
    from handlers.EvalHandler import EvalHandler
    df = pd.DataFrame({"a": [1, 2]})
    handler = EvalHandler()
    result = handler.custom_eval("if_(a > 1, 10, 0)", df)
    assert result.tolist() == [0, 10]


def test_custom_eval_rejects_malicious(mock_heavy_modules):
    from handlers.EvalHandler import EvalHandler
    df = pd.DataFrame({"a": [1]})
    handler = EvalHandler()
    with pytest.raises(Exception):
        handler.custom_eval("__import__('os').system('echo bad')", df)

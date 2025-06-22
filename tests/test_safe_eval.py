import pandas as pd
import pytest
import sys
import os
import types

# Mock heavy modules before importing EvalHandler
mock_modules = {
    'handlers.JavaHandler': types.SimpleNamespace(JavaHandler=lambda *a, **k: None),
}
sys.modules.update(mock_modules)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from handlers.EvalHandler import EvalHandler


def test_custom_eval_basic():
    df = pd.DataFrame({"a": [1, 2]})
    handler = EvalHandler()
    result = handler.custom_eval("if_(a > 1, 10, 0)", df)
    assert result.tolist() == [0, 10]


def test_custom_eval_rejects_malicious():
    df = pd.DataFrame({"a": [1]})
    handler = EvalHandler()
    with pytest.raises(Exception):
        handler.custom_eval("__import__('os').system('echo bad')", df)

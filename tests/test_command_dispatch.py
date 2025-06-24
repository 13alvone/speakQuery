import pytest

pytest.importorskip("pandas")

import os
import sys
import types
import importlib
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.fixture
def patched_listener(monkeypatch):
    """Return speakQueryListener with heavy dependencies patched."""
    fake_loader = types.ModuleType("functionality.so_loader")
    fake_loader.resolve_and_import_so = lambda p, n: types.SimpleNamespace(
        process_index_calls=lambda tokens: pd.DataFrame(),
        parse_dates_to_epoch=lambda x: x,
    )
    monkeypatch.setitem(sys.modules, "functionality.so_loader", fake_loader)

    fake_eval = types.ModuleType("handlers.EvalHandler")

    class _FakeEvalHandler:
        def run_eval(self, tokens, df):
            return "eval"

    fake_eval.EvalHandler = _FakeEvalHandler
    monkeypatch.setitem(sys.modules, "handlers.EvalHandler", fake_eval)
    handlers_pkg = importlib.import_module("handlers")
    monkeypatch.setattr(handlers_pkg, "EvalHandler", fake_eval, raising=False)

    listener_mod = importlib.import_module("lexers.speakQueryListener")
    importlib.reload(listener_mod)
    yield listener_mod.speakQueryListener


def test_command_dispatch(patched_listener, monkeypatch):
    speakQueryListener = patched_listener
    listener = speakQueryListener('index="dummy"')
    listener.main_df = pd.DataFrame()

    for cmd in list(listener._command_map.keys()):
        called = {}

        def stub(tokens, seg, _cmd=cmd):
            called[_cmd] = True
            return _cmd

        monkeypatch.setitem(listener._command_map, cmd, stub)
        result = listener._apply_command(cmd, [cmd], cmd)
        assert called.get(cmd) is True
        assert result == cmd


def test_eval_fallback(patched_listener, monkeypatch):
    speakQueryListener = patched_listener
    listener = speakQueryListener('index="dummy"')
    listener.main_df = pd.DataFrame()
    monkeypatch.setattr('handlers.EvalHandler.EvalHandler.run_eval', lambda self, t, df: "eval")
    assert listener._apply_command('if_', ['if_'], 'if_') == "eval"


def test_macro_branch(patched_listener, monkeypatch):
    speakQueryListener = patched_listener
    listener = speakQueryListener('index="dummy"')
    listener.main_df = pd.DataFrame()
    monkeypatch.setattr('handlers.MacroHandler.MacroHandler.parse_arguments', lambda self, s: [])
    monkeypatch.setattr('handlers.MacroHandler.MacroHandler.execute_macro', lambda self, n, a, df: "macro")
    assert listener._apply_command('macro', [], '`macro()`') == "macro"

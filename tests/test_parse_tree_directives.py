import pytest

pytest.importorskip("antlr4")
pytest.importorskip("pandas")

import os
import sys
import types
import antlr4
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

fake_loader = types.ModuleType("functionality.so_loader")
fake_loader.resolve_and_import_so = lambda path, name: types.SimpleNamespace(
    process_index_calls=lambda tokens: pd.DataFrame(),
    parse_dates_to_epoch=lambda x: x,
)
sys.modules["functionality.so_loader"] = fake_loader

from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener


def run_query(query: str):
    input_stream = antlr4.InputStream(query)
    lexer = speakQueryLexer(input_stream)
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()
    listener = speakQueryListener(query)
    antlr4.ParseTreeWalker().walk(listener, tree)
    return listener


def test_stats_directive(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df
    )
    called = {}
    def fake_stats(self, tokens, d):
        called["t"] = tokens
        return d
    monkeypatch.setattr("handlers.StatsHandler.StatsHandler.run_stats", fake_stats)
    listener = run_query('index="dummy" | stats count')
    assert called["t"] == ["stats", "count"]
    assert listener.main_df.equals(df)


def test_stats_directive_paren_collapse(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df
    )
    called = {}

    def fake_stats(self, tokens, d):
        called["t"] = tokens
        return d

    monkeypatch.setattr("handlers.StatsHandler.StatsHandler.run_stats", fake_stats)
    listener = run_query('index="dummy" | stats values(a) as levels')
    assert called["t"] == ["stats", "values(a)", "as", "levels"]
    assert listener.main_df.equals(df)


def test_eval_directive(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df
    )
    called = {}
    class FakeEval:
        def run_eval(self, tokens, d):
            called["t"] = tokens
            return d

    fake_eval_mod = types.ModuleType("handlers.EvalHandler")
    fake_eval_mod.EvalHandler = FakeEval
    monkeypatch.setitem(sys.modules, "handlers.EvalHandler", fake_eval_mod)
    listener = run_query('index="dummy" | eval b=a')
    assert called["t"] == ["eval", "b", "=", "a"]
    assert listener.main_df.equals(df)



import pytest

pytest.importorskip("antlr4")
pytest.importorskip("pandas")

import os
import sys
import types
import antlr4
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# provide dummy so_loader module
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
    return parser.getNumberOfSyntaxErrors(), listener


def test_fieldsummary_directive(monkeypatch):
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    errors, _ = run_query('index="dummy" | fieldsummary')
    assert errors == 0


def test_outputlookup_directive(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    monkeypatch.setattr(
        "handlers.GeneralHandler.GeneralHandler.execute_outputlookup",
        lambda self, df, **kwargs: None,
    )
    errors, _ = run_query('index="dummy" | outputlookup out.csv')
    assert errors == 0


def test_outputlookup_with_options(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    monkeypatch.setattr(
        "handlers.GeneralHandler.GeneralHandler.execute_outputlookup",
        lambda self, df, **kwargs: None,
    )
    q = 'index="dummy" | outputlookup out.csv window=5 overwrite_if_empty=true create_empty=true'
    errors, _ = run_query(q)
    assert errors == 0


def test_outputnew_directive(monkeypatch):
    df = pd.DataFrame({"a": [1]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    monkeypatch.setattr(
        "handlers.GeneralHandler.GeneralHandler.execute_outputnew",
        lambda self, df, filename: None,
    )
    errors, _ = run_query('index="dummy" | outputnew new.csv')
    assert errors == 0

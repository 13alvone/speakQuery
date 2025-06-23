import os
import sys
import types
import antlr4
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Provide lightweight stand-in for so_loader
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
    return listener.main_df


def test_dedup_basic(monkeypatch):
    df = pd.DataFrame({
        "transactionID": [1, 1, 2, 1],
        "userID": [10, 10, 20, 10],
        "val": [100, 200, 300, 400],
    })
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    result = run_query('index="dummy" | dedup transactionID, userID')
    assert len(result) == 2
    assert set(result["transactionID"]) == {1, 2}


def test_dedup_consecutive(monkeypatch):
    df = pd.DataFrame({
        "sessionID": [1, 1, 1, 2, 2, 1],
        "val": range(6),
    })
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    result = run_query('index="dummy" | dedup sessionID consecutive=true')
    assert result["sessionID"].tolist() == [1, 1, 1, 2]
    assert len(result) == 4

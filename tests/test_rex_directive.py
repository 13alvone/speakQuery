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
    return listener.main_df


def test_rex_basic(monkeypatch):
    df = pd.DataFrame({"_raw": ["user=1", "user=2"]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    result = run_query('index="dummy" | rex field=_raw "user=(?P<uid>\\d+)"')
    assert result["uid"].tolist() == ["1", "2"]


def test_rex_with_field(monkeypatch):
    df = pd.DataFrame({"message": ["status=200", "status=404"]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    query = 'index="dummy" | rex field=message "status=(?P<code>\\d{3})"'
    result = run_query(query)
    assert result["code"].tolist() == ["200", "404"]


import pytest

pytest.importorskip("antlr4")
pytest.importorskip("pandas")

import os
import sys
import types
import antlr4
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# provide dummy so_loader module for listener import
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


def test_spath_basic(monkeypatch):
    df = pd.DataFrame({"jsonField": [{"details": {"info": 1}}, {"details": {"info": 2}}]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    query = 'index="dummy" | spath input=jsonField output=extractedField path=details.info'
    result = run_query(query)
    assert result["extractedField"].tolist() == [1, 2]

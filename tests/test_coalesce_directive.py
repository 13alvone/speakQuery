import pytest

pytest.importorskip("antlr4")
pytest.importorskip("pandas")

import os
import sys
import types
import antlr4
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

fake_loader = types.ModuleType("functionality.so_loader")
fake_loader.resolve_and_import_so = lambda path, name: types.SimpleNamespace(
    process_index_calls=lambda x: None,
    parse_dates_to_epoch=lambda x: x,
)
sys.modules["functionality.so_loader"] = fake_loader

from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener


def run_query(query):
    input_stream = antlr4.InputStream(query)
    lexer = speakQueryLexer(input_stream)
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()
    listener = speakQueryListener(query)
    antlr4.ParseTreeWalker().walk(listener, tree)
    return listener.main_df


def test_coalesce_directive(monkeypatch):
    df = pd.DataFrame({"fieldA": [None, "A2", ""], "fieldB": ["B1", "B2", "B3"]})

    monkeypatch.setattr("lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy())
    result = run_query('index="dummy" | coalesce(fieldA, fieldB)')
    assert result["coalesce"].tolist() == ["B1", "A2", "B3"]

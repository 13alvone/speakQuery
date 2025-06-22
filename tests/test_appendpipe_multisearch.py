import os
import sys
import types
import antlr4
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Fake loader for functionality.so_loader
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


def test_appendpipe(monkeypatch):
    df = pd.DataFrame({"x": [1, 2, 3]})
    monkeypatch.setattr(
        "lexers.speakQueryListener.process_index_calls", lambda tokens: df.copy()
    )
    result = run_query('index="dummy" | appendpipe [ stats sum(x) as total ]')
    assert result["total"].iloc[-1] == 6
    assert len(result) == 4


def test_multisearch(monkeypatch):
    df1 = pd.DataFrame({"x": [1]})
    df2 = pd.DataFrame({"x": [2]})

    def fake_process(tokens):
        joined = " ".join(tokens)
        if "foo" in joined:
            return df1.copy()
        if "bar" in joined:
            return df2.copy()
        return pd.DataFrame()

    monkeypatch.setattr("lexers.speakQueryListener.process_index_calls", fake_process)
    result = run_query('index="dummy" | multisearch [ index="foo" ] [ index="bar" ]')
    assert result["x"].tolist() == [1, 2]


import os
import sys
import pytest
from antlr4 import InputStream, CommonTokenStream

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser


@pytest.mark.parametrize(
    "query",
    [
        'index="logs/main.log" | stats count',
        'index="logs/main.log" | eval total=price*quantity',
        'index="logs/main.log" | head 5',
        'index="logs/main.log" | fields field1 field2',
    ],
)
def test_pipeline_parses(query):
    lexer = speakQueryLexer(InputStream(query))
    stream = CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    parser.speakQuery()
    assert parser.getNumberOfSyntaxErrors() == 0

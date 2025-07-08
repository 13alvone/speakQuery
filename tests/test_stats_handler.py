import os
import sys
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from handlers.StatsHandler import StatsHandler


def test_parse_wildcard():
    handler = StatsHandler()
    specs = handler._parse_function_specs("values(*) as *")
    assert specs == [{'func': 'values', 'field': '*', 'alias': '*'}]


def test_stats_wildcard_expansion():
    handler = StatsHandler()
    df = pd.DataFrame({'a': [1, 2], 'b': [3, 3]})
    result = handler.run_stats(['stats', 'values(*)', 'as', '*'], df)
    assert set(result.columns) == {'a', 'b'}
    assert result['a'].iloc[0] == [1, 2]
    assert result['b'].iloc[0] == [3]

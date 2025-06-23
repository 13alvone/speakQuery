import os
import sys
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from handlers.GeneralHandler import GeneralHandler


def test_execute_outputnew_creates_file(tmp_path):
    df = pd.DataFrame({'a': [1, 2]})
    file_path = tmp_path / 'out.csv'
    GeneralHandler.execute_outputnew(df.copy(), str(file_path))
    assert file_path.exists()


def test_execute_outputnew_fails_if_exists(tmp_path):
    df = pd.DataFrame({'a': [1]})
    file_path = tmp_path / 'out.csv'
    file_path.write_text('')
    with pytest.raises(FileExistsError):
        GeneralHandler.execute_outputnew(df, str(file_path))

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pytest
from handlers.MacroHandler import MacroHandler


def test_parse_arguments():
    handler = MacroHandler()
    arg_str = 'to="admin@example.com" subject="Module Error" sendresults=true'
    args = handler.parse_arguments(arg_str)
    assert args == {
        'to': 'admin@example.com',
        'subject': 'Module Error',
        'sendresults': True,
    }


def test_execute_macro_records_last_call():
    handler = MacroHandler()
    args = {'foo': 'bar'}
    handler.execute_macro('my_custom_macro', args, None)
    assert handler.last_macro == ('my_custom_macro', args)

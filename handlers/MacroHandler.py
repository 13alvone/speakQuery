#!/usr/bin/env python3
"""
MacroHandler.py
Purpose: Simple macro execution framework for speakQuery.
"""

import logging
from typing import Callable, Dict, Any
import pandas as pd
import shlex

logger = logging.getLogger(__name__)


class MacroHandler:
    def __init__(self):
        self.last_macro = None
        self.macros: Dict[str, Callable[[pd.DataFrame | None, Dict[str, Any]], pd.DataFrame | None]] = {
            'my_custom_macro': self.my_custom_macro
        }

    def parse_arguments(self, arg_str: str) -> Dict[str, Any]:
        """Parse a string of key=value pairs into a dictionary."""
        if not arg_str:
            return {}
        tokens = shlex.split(arg_str)
        args: Dict[str, Any] = {}
        for token in tokens:
            if '=' in token:
                key, value = token.split('=', 1)
                value = value.strip('"').strip("'")
                if value.lower() == 'true':
                    value = True
                elif value.lower() == 'false':
                    value = False
                args[key] = value
            else:
                args[token] = True
        return args

    def execute_macro(self, name: str, args: Dict[str, Any], df: pd.DataFrame | None):
        handler = self.macros.get(name)
        if handler is None:
            logging.warning(f"[!] Unknown macro '{name}'. Skipping.")
            return df
        logging.info(f"[i] Executing macro '{name}' with args: {args}")
        self.last_macro = (name, args)
        return handler(df, args)

    @staticmethod
    def my_custom_macro(df: pd.DataFrame | None, args: Dict[str, Any]):
        logging.info(f"[i] my_custom_macro called with {args}")
        return df

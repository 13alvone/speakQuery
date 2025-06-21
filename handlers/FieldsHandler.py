#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class FieldsHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_fields(self, tokens, df):
        """Apply the fields directive to include or exclude columns."""
        if not tokens or df is None:
            return df
        mode = '+'
        columns = []
        for tok in tokens[1:]:
            if tok in ('+', '-'):
                mode = tok
            else:
                columns.append(tok)
        try:
            return self.general_handler.filter_df_columns(df, columns, mode)
        except Exception as e:
            logging.error(f"[x] fields command failed: {e}")
            return df

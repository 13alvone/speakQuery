#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class SortHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_sort(self, tokens, df):
        mode = '+'
        cols = []
        for tok in tokens[1:]:
            if tok in ('+', '-'):
                mode = tok
            else:
                cols.append(tok)
        try:
            return self.general_handler.sort_df_by_columns(df, cols, mode)
        except Exception as e:
            logging.error(f"[x] sort failed: {e}")
            return df

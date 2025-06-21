#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class RegexHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_regex(self, tokens, df):
        if len(tokens) < 4:
            return df
        field = tokens[1]
        pattern = tokens[3]
        try:
            return self.general_handler.filter_df_by_regex(df, field, pattern)
        except Exception as e:
            logging.error(f"[x] regex failed: {e}")
            return df

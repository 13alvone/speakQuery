#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class ReverseHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_reverse(self, tokens, df):
        try:
            return self.general_handler.reverse_df_rows(df)
        except Exception as e:
            logging.error(f"[x] reverse failed: {e}")
            return df

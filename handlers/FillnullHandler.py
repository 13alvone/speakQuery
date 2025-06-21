#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class FillnullHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_fillnull(self, tokens, df):
        try:
            return self.general_handler.execute_fillnull(df, tokens)
        except Exception as e:
            logging.error(f"[x] fillnull failed: {e}")
            return df

#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class DedupHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_dedup(self, tokens, df):
        try:
            return self.general_handler.execute_dedup(df, tokens[1:])
        except Exception as e:
            logging.error(f"[x] dedup failed: {e}")
            return df

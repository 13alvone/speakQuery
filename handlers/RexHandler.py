#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class RexHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_rex(self, tokens, df):
        try:
            return self.general_handler.execute_rex(df, tokens)
        except Exception as e:
            logging.error(f"[x] rex failed: {e}")
            return df

#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class Base64Handler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_base64(self, tokens, df):
        try:
            return self.general_handler.handle_base64(df, tokens)
        except Exception as e:
            logging.error(f"[x] base64 failed: {e}")
            return df

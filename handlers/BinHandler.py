#!/usr/bin/env python3
import logging
from handlers.archive.TimeHandler import TimeHandler


class BinHandler:
    def __init__(self):
        self.time_handler = TimeHandler()

    def run_bin(self, tokens, df):
        try:
            return self.time_handler.bin_time_data(df, tokens)
        except Exception as e:
            logging.error(f"[x] bin failed: {e}")
            return df

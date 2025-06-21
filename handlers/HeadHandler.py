#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class HeadHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_head(self, tokens, df):
        if len(tokens) != 2:
            return df
        try:
            n = int(tokens[1])
        except ValueError:
            logging.error("[x] head/limit requires numeric argument")
            return df
        mode = 'head' if tokens[0].lower() == 'head' else 'tail'
        try:
            return self.general_handler.head_call(df, n, mode)
        except Exception as e:
            logging.error(f"[x] head/limit failed: {e}")
            return df

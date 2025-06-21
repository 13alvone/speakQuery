#!/usr/bin/env python3
import logging
from handlers.LookupHandler import LookupHandler


class JoinHandler:
    def __init__(self, lookup_root=None):
        self.lookup_handler = LookupHandler()
        self.lookup_root = lookup_root

    def run_join(self, tokens, df):
        logging.warning("Join command not fully implemented; returning original DataFrame")
        return df

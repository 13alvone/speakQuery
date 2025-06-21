#!/usr/bin/env python3
import logging
import os
from handlers.LookupHandler import LookupHandler


class LookupCmdHandler:
    def __init__(self, lookup_root=None):
        self.lookup_handler = LookupHandler()
        self.lookup_root = lookup_root or os.path.join(os.getcwd(), 'lookups')

    def run_lookup(self, tokens, df):
        """Join lookup file data to df on a shared field."""
        if len(tokens) < 3:
            return df
        filename = tokens[1].strip('"')
        shared = tokens[2]
        path = os.path.join(self.lookup_root, filename)
        lookup_df = self.lookup_handler.load_data(path)
        if lookup_df is None:
            logging.error(f"[x] lookup file not found: {path}")
            return df
        if shared not in df.columns or shared not in lookup_df.columns:
            logging.error(f"[x] shared field '{shared}' missing for lookup")
            return df
        try:
            return df.merge(lookup_df, on=shared, how='left')
        except Exception as e:
            logging.error(f"[x] lookup merge failed: {e}")
            return df

#!/usr/bin/env python3
import logging
from handlers.GeneralHandler import GeneralHandler


class RenameHandler:
    def __init__(self):
        self.general_handler = GeneralHandler()

    def run_rename(self, tokens, df):
        """Rename columns based on 'rename old AS new' syntax."""
        if df is None or len(tokens) < 4:
            return df
        i = 1
        while i + 2 < len(tokens):
            old = tokens[i]
            if tokens[i+1].lower() != 'as':
                break
            new = tokens[i+2].strip('"')
            try:
                df = self.general_handler.rename_column(df, old, new)
            except Exception as e:
                logging.error(f"[x] rename failed for {old}->{new}: {e}")
            i += 3
        return df

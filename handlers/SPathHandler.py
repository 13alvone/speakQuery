#!/usr/bin/env python3
import logging
import json


class SPathHandler:
    def run_spath(self, tokens, df):
        if len(tokens) < 5:
            return df
        source = tokens[1]
        target = tokens[4]
        try:
            df[target] = df[source].apply(lambda x: json.loads(x))
        except Exception as e:
            logging.error(f"[x] spath failed: {e}")
        return df

#!/usr/bin/env python3
"""Example scheduled input script that fetches a CSV and stores it as a Parquet file.

This script downloads a small sample dataset, loads it into a pandas DataFrame
and then calls ``GENERATE_RESULTS`` so the scheduled input engine writes the
Parquet output. Pass ``--help`` for usage information.
"""
import argparse
import logging
from io import StringIO
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
DEFAULT_CSV = "https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv"

def download_csv(url: str) -> pd.DataFrame:
    """Download ``url`` and return it as a DataFrame."""
    try:
        logging.info("[i] Downloading %s", url)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return pd.read_csv(StringIO(response.text))
    except Exception as exc:
        logging.error("[x] Failed to fetch CSV: %s", exc)
        raise

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch a CSV and generate a Parquet results file")
    parser.add_argument("--csv_url", default=DEFAULT_CSV, help="URL of the CSV file to download")
    args = parser.parse_args()
    df = download_csv(args.csv_url)
    logging.info("[i] Retrieved %d rows", len(df))
    try:
        GENERATE_RESULTS(df)
    except Exception as exc:
        logging.error("[x] GENERATE_RESULTS failed: %s", exc)
        raise

if __name__ == "__main__":
    main()

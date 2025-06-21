#!/usr/bin/env python3
"""
Test Script: test_stats.py
Purpose: To perform testing of stats, eventstats, and streamstats commands.
         Test cases are loaded from an external YAML file for modular extension.
         Run a single test, multiple tests, or a range via CLI options.
         --output_mode controls verbosity:
           - verbose: detailed output for all tests
           - error_only_verbose: detailed output only for failed tests
           - error_only (default): minimal output for failed tests
"""

import os
import sys
import logging
import argparse
import yaml
import pandas as pd
import time

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger("test_stats")


def find_project_root(start_path: str, marker_files=('app.py', '.git')):
    logger.debug("[DEBUG] Attempting to find project root...")
    current = os.path.abspath(start_path)
    while current != os.path.dirname(current):
        if any(os.path.exists(os.path.join(current, m)) for m in marker_files):
            logger.debug(f"[DEBUG] Project root identified: {current}")
            return current
        current = os.path.dirname(current)
    raise RuntimeError("Project root not found")

# Determine and set project root
project_root = find_project_root(__file__)
os.chdir(project_root)
logger.info(f"[i] Project root: {project_root}")

# Define shared-library paths
cpp_index_path = os.path.join(project_root, "functionality", "cpp_index_call", "build")
cpp_datetime_path = os.path.join(project_root, "functionality", "cpp_datetime_parser", "build")
logger.info(f"[i] CPP Index Call Build Directory: {cpp_index_path}")
logger.info(f"[i] CPP Datetime Parser Build Directory: {cpp_datetime_path}")

# Insert project root for imports
sys.path.insert(0, project_root)

# Load dynamic .so modules
try:
    from functionality.so_loader import resolve_and_import_so
except ImportError as e:
    logger.error(f"[x] Could not import so_loader: {e}")
    sys.exit(1)

try:
    cpp_index = resolve_and_import_so(cpp_index_path, "cpp_index_call")
    process_index_calls = cpp_index.process_index_calls
    logger.info("[i] Loaded 'cpp_index_call'.")
except ImportError as e:
    logger.error(f"[x] Could not import cpp_index_call: {e}")
    sys.exit(1)

try:
    cpp_dt = resolve_and_import_so(cpp_datetime_path, "cpp_datetime_parser")
    parse_dates_to_epoch = cpp_dt.parse_dates_to_epoch
    logger.info("[i] Loaded 'cpp_datetime_parser'.")
except ImportError as e:
    logger.error(f"[x] Could not import cpp_datetime_parser: {e}")
    sys.exit(1)

# Import StatsHandler
try:
    from handlers.StatsHandler import StatsHandler
except ImportError as e:
    logger.error(f"[x] Could not import StatsHandler: {e}")
    sys.exit(1)


def load_test_cases(yaml_file):
    """Load test cases from YAML file."""
    with open(yaml_file, 'r') as f:
        return yaml.safe_load(f)


def parse_selection(test_cases, sel):
    if not sel:
        return test_cases
    selected = {}
    for part in sel.split(','):
        part = part.strip()
        if '-' in part:
            start, end = part.split('-')
            for tc in test_cases:
                tid = int(tc['id'])
                if int(start) <= tid <= int(end):
                    selected[tid] = tc
        else:
            for tc in test_cases:
                if int(tc['id']) == int(part):
                    selected[int(part)] = tc
    return [selected[k] for k in sorted(selected)]


def run_tests(cases, mode):
    passed = failed = 0
    start = time.time()
    handler = StatsHandler()

    for tc in cases:
        header = '='*80 + f"\nTest {tc['id']}: {tc.get('description','')}\nQuery: {tc.get('original_query','')}\n"
        # Execute index call
        df = process_index_calls(tc['index_tokens'])
        if df is None or df.empty:
            logger.warning(header + "Index call returned empty DataFrame. Skipping stats.")
            continue

        # Verbose: show initial df
        if mode == 'verbose':
            logger.info(header + "DataFrame before stats (first 5 rows):\n" + str(df.head()))

        try:
            result = handler.run_stats(tc['stats_tokens'], df)
            # Validate expected columns
            for col in tc.get('expected_columns', []):
                if col not in result.columns:
                    raise AssertionError(f"Expected column '{col}' not in result.")
            # Validate expected row count
            if 'expected_row_count' in tc:
                if result.shape[0] != tc['expected_row_count']:
                    raise AssertionError(f"Expected {tc['expected_row_count']} rows, got {result.shape[0]}.")
            # Validate expected values
            for alias, exp in tc.get('expected_values', {}).items():
                val = result.iloc[0][alias]
                if isinstance(exp, list):
                    if val != exp:
                        raise AssertionError(f"Expected {alias}={exp}, got {val}.")
                else:
                    if pd.isna(exp):
                        if not pd.isna(val):
                            raise AssertionError(f"Expected {alias}=NaN, got {val}.")
                    elif val != exp:
                        raise AssertionError(f"Expected {alias}={exp}, got {val}.")

            passed += 1
            if mode == 'verbose':
                logger.info("Result (first 5 rows):\n" + str(result.head()))
                logger.info(f"[i] Test {tc['id']} PASSED.\n")
        except Exception as e:
            failed += 1
            if mode == 'verbose':
                logger.error(header + f"Error: {e}\n")
            elif mode == 'error_only_verbose':
                logger.error(header + f"Error: {e}\n")
            else:  # error_only
                logger.error(header.rstrip() + f"\nError: {e}\n")

    elapsed = time.time() - start
    summary = '\n' + '='*80 + f"\nPassed: {passed}, Failed: {failed}, Time: {elapsed:.2f}s\n"
    logger.info(summary)


def main():
    parser = argparse.ArgumentParser(description="Run stats test cases.")
    parser.add_argument('-t', '--tests', type=str, help="Test id(s) to run (e.g. '1,3-5').")
    parser.add_argument('--output_mode', type=str, default='error_only',
                        choices=['verbose','error_only_verbose','error_only'],
                        help="Output verbosity mode.")
    args = parser.parse_args()

    yaml_file = os.path.join(project_root, 'tests', 'stats_test_cases.yaml')
    all_cases = load_test_cases(yaml_file)
    selected = parse_selection(all_cases, args.tests)
    run_tests(selected, args.output_mode)


if __name__ == '__main__':
    main()

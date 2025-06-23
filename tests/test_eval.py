#!/usr/bin/env python3
"""
Test Script: test_eval.py
Purpose: To perform extensive testing of our index calls followed by eval commands.
         Test cases are loaded from an external YAML file for easy modular extension.
         The user can run a single test, multiple tests, or a range of tests via CLI options.
         Additionally, a summary is logged at the end showing tests passed/failed and time elapsed.
         The command-line option --output_mode controls verbosity:
           - verbose: Log complete output for all tests.
           - error_only_verbose: Log detailed output only for failed tests.
           - error_only (default): Log only minimal output (header, original query, error message) for failed tests.
"""

import os
import sys
import logging
import argparse
import yaml
import time
import pytest

# Configure logging – using a dedicated logger for this module.
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
logger = logging.getLogger("test_eval")

def find_project_root(start_path: str, marker_files=('app.py', '.git')):
    logger.debug("[DEBUG] Attempting to find project root...")
    current = os.path.abspath(start_path)
    while current != os.path.dirname(current):
        if any(os.path.exists(os.path.join(current, marker)) for marker in marker_files):
            logger.debug(f"[DEBUG] Project root identified: {current}")
            return current
        current = os.path.dirname(current)
    raise RuntimeError("Project root not found")

# Determine the project root.
project_root = find_project_root(__file__)
os.chdir(project_root)
logger.info(f"[i] Project root: {project_root}")

# Define module build directories.
cpp_index_path = os.path.join(project_root, "functionality", "cpp_index_call", "build")
cpp_datetime_path = os.path.join(project_root, "functionality", "cpp_datetime_parser", "build")
logger.info(f"[i] CPP Index Call Build Directory: {cpp_index_path}")
logger.info(f"[i] CPP Datetime Parser Build Directory: {cpp_datetime_path}")

# Insert project root into sys.path.
sys.path.insert(0, project_root)

# Import the secure dynamic loader.
try:
    from functionality.so_loader import resolve_and_import_so
except Exception as e:
    pytest.skip(f"so_loader not available: {e}", allow_module_level=True)

# Dynamically load shared objects.
try:
    cpp_index_module = resolve_and_import_so(cpp_index_path, "cpp_index_call")
    process_index_calls = cpp_index_module.process_index_calls
    logger.info("[i] Successfully loaded 'cpp_index_call' module.")
except ImportError as e:
    pytest.skip(f"cpp_index_call not available: {e}", allow_module_level=True)

try:
    cpp_datetime_module = resolve_and_import_so(cpp_datetime_path, "cpp_datetime_parser")
    parse_dates_to_epoch = cpp_datetime_module.parse_dates_to_epoch
    logger.info("[i] Successfully loaded 'cpp_datetime_parser' module.")
except ImportError as e:
    pytest.skip(f"cpp_datetime_parser not available: {e}", allow_module_level=True)

# Import EvalHandler.
try:
    from handlers.EvalHandler import EvalHandler
except Exception as e:
    pytest.skip(f"EvalHandler not available: {e}", allow_module_level=True)

def load_test_cases(yaml_file):
    with open(yaml_file, "r") as f:
        test_cases = yaml.safe_load(f)
    return test_cases

def parse_cli_args():
    parser = argparse.ArgumentParser(description="Run eval test cases.")
    parser.add_argument(
        "-t", "--tests",
        type=str,
        help=("Test id(s) to run. Can be a single id (e.g., '4'), a comma-separated list (e.g., '1,5,19'), "
              "or a range (e.g., '3-7'). If not specified, all tests are run.")
    )
    parser.add_argument(
        "--output_mode",
        type=str,
        default="error_only",
        choices=["verbose", "error_only_verbose", "error_only"],
        help=("Output mode: verbose logs complete output for all tests; "
              "error_only_verbose logs detailed output only for failed tests; "
              "error_only (default) logs only minimal output (header, original query, error message) for failed tests.")
    )
    return parser.parse_args()

def select_tests(test_cases, selection):
    if not selection:
        return test_cases
    selected = []
    parts = selection.split(",")
    for part in parts:
        part = part.strip()
        if "-" in part:
            start, end = part.split("-")
            selected.extend([tc for tc in test_cases if int(tc["id"]) >= int(start) and int(tc["id"]) <= int(end)])
        else:
            selected.extend([tc for tc in test_cases if int(tc["id"]) == int(part)])
    # Remove duplicate keys and sort.
    selected = {tc["id"]: tc for tc in selected}.values()
    return sorted(selected, key=lambda x: int(x["id"]))

def run_tests(selected_tests, output_mode):
    passed_count = 0
    failed_count = 0
    start_time = time.time()

    for tc in selected_tests:
        header = ("=" * 80 + "\n" +
                  f"Test Eval {tc['id']}: {tc['description']}\n" +
                  f"Original Query: {tc['original_query']}\n")
        # Process index call.
        df = process_index_calls(tc['index_tokens'])
        if df.empty:
            if output_mode == "verbose":
                logger.info(header + "Index call produced an empty DataFrame.\n")
            continue

        # Output index call DataFrame if full verbosity is requested.
        if output_mode == "verbose":
            logger.info(header + "DataFrame from index call (first 5 rows):")
            logger.info("\n" + str(df.head()) + "\n")
        # For error-only modes, we don't log index call details.

        eval_handler = EvalHandler()
        try:
            df = eval_handler.run_eval(tc['eval_tokens'], df)
            # Check for expected result column if specified.
            if tc.get('expected_column') and tc['expected_column'] not in df.columns:
                raise ValueError(f"Expected column '{tc['expected_column']}' not found in DataFrame.")
            passed_count += 1
            if output_mode == "verbose":
                logger.info("DataFrame after applying eval (first 5 rows):")
                logger.info("\n" + str(df.head()) + "\n")
                logger.info(f"Test PASSED: Column '{tc.get('expected_column','')}' successfully added/modified.\n")
        except Exception as e:
            failed_count += 1
            if output_mode == "verbose":
                logger.error(f"Error processing eval clause for Test {tc['id']}: {str(e)}\n")
            elif output_mode == "error_only_verbose":
                logger.error(header)
                logger.error(f"Error processing eval clause for Test {tc['id']}: {str(e)}\n")
            elif output_mode == "error_only":
                logger.error(header.rstrip())
                logger.error(f"Error processing eval clause for Test {tc['id']}: {str(e)}\n")

    elapsed = time.time() - start_time
    summary = ("\n" + "=" * 80 + "\n" +
               f"Total tests passed: {passed_count}\n" +
               f"Total tests failed: {failed_count}\n" +
               f"Elapsed time: {elapsed:.2f} seconds\n")
    logger.info(summary)

def main() -> None:
    args = parse_cli_args()
    yaml_file = os.path.join(project_root, "tests", "eval_test_cases.yaml")
    all_tests = load_test_cases(yaml_file)
    selected_tests = select_tests(all_tests, args.tests)
    run_tests(selected_tests, args.output_mode)


if __name__ == '__main__':
    main()


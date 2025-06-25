import pytest

pytest.importorskip("antlr4")
pytest.importorskip("pandas")
pytest.importorskip("yaml")

import os
import sys
import shutil
import datetime
import subprocess
from antlr4 import *
from antlr4.error.ErrorListener import ErrorListener
import logging
import pandas as pd
import yaml  # Import the yaml library


# Function to load test queries from a YAML file
def load_test_queries_from_yaml(yaml_file_path):
    with open(yaml_file_path, 'r') as file:
        return yaml.safe_load(file)


def setup_logging():
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    output_dir = "logs/unit_tests_grun"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    _log_filename = f"{output_dir}/unit_tests_parse_results_{timestamp}.testresults"
    handler = logging.FileHandler(_log_filename)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return _log_filename


def update_grammar():
    antlr_jar_path = "utils/antlr-4.13.1-complete.jar"
    grammar_file = "lexers/speakQuery.g4"
    generated_dir = "lexers/antlr4_active"
    if os.path.exists(generated_dir):
        shutil.rmtree(generated_dir)
        logging.info(f"Deleted existing '{generated_dir}' directory.")
    antlr_command = [
        "java",
        "-jar",
        antlr_jar_path,
        "-Dlanguage=Python3",
        "-no-listener",
        "-visitor",
        f"-o {generated_dir}",
        grammar_file
    ]
    try:
        subprocess.run(antlr_command, check=True)  # nosec - dev-only generation of parser files
        logging.info("Regenerated the target antlr4 files successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to regenerate target files: {e}")


def run_unit_tests(_log_filename):
    global test_queries
    summary_data = {"passed": 0, "failed": 0, "by_category": {}, "by_complexity": {}}

    class ParsingErrorListener(ErrorListener):  # Custom error listener to detect parsing errors
        def __init__(self):
            self.hasError = False

        def syntaxError(self, recognizer, offending_symbol, line, column, msg, err):
            self.hasError = True
            logging.error(f"Syntax Error - line {line}:{column} {msg}")
    event_delimiter = '-' * 20
    for i, test_query in enumerate(test_queries):
        logging.info(event_delimiter)
        logging.info(f"Test {i}: CATEGORY: [{test_query['category']}], "
                     f"COMPLEXITY: [{test_query['complexity']}], TITLE: [{test_query['title']}]")
        input_stream = InputStream(test_query["query"])
        lexer = speakQueryLexer(input_stream)
        stream = CommonTokenStream(lexer)
        parser = speakQueryParser(stream)
        error_listener = ParsingErrorListener()
        parser.removeErrorListeners()
        parser.addErrorListener(error_listener)
        parser.speakQuery()  # Execute the test
        if error_listener.hasError:
            summary_data["failed"] += 1
            category = test_query["category"]
            complexity = test_query["complexity"]
            summary_data["by_category"].setdefault(category, {"passed": 0, "failed": 1})
            summary_data["by_category"][category]["failed"] += 1
            summary_data["by_complexity"].setdefault(complexity, {"passed": 0, "failed": 1})
            summary_data["by_complexity"][complexity]["failed"] += 1
            print(f"FAILED QUERY:\n{test_query['query']}\nCategory: {category}\nComplexity: {complexity}, id={i}")
            logging.error(f"FAILED QUERY:\n{test_query['query']}")
        else:
            summary_data["passed"] += 1
            category = test_query["category"]
            complexity = test_query["complexity"]
            summary_data["by_category"].setdefault(category, {"passed": 0, "failed": 0})
            summary_data["by_complexity"].setdefault(complexity, {"passed": 0, "failed": 0})
            summary_data["by_category"][category]["passed"] += 1
            summary_data["by_complexity"][complexity]["passed"] += 1
    generate_summary(_log_filename, summary_data)


def generate_summary(_log_filename, summary_data):
    categories_df = pd.DataFrame.from_dict(summary_data["by_category"], orient='index')
    complexities_df = pd.DataFrame.from_dict(summary_data["by_complexity"], orient='index')
    total_tests = summary_data['passed'] + summary_data['failed']
    passed = summary_data['passed']
    failed = summary_data['failed']
    pass_percentage = round(passed / total_tests, 2) * 100
    overall_summary = f"Total Tests: {total_tests}\nPassed: {passed}\nFailed: {failed}\nPass %: {pass_percentage} %\n"
    summary = f'{overall_summary}\nDetails by Category:\n{categories_df.to_string()}\n\nDetails of Complexity:\n{categories_df.to_string()}'
    with open(_log_filename, "a") as log_file:
        log_file.write(summary)
    print(summary)


if __name__ == "__main__":
    log_filename = setup_logging()  # Setup logging and get the log filename
    update_grammar()  # Update ANTLR4 components
    # Load the test queries from YAML file
    test_queries = load_test_queries_from_yaml('syntax_unit_tests.yaml')  # Path to your YAML file
    try:
        sys.path.append(os.path.abspath('lexers'))
        from lexers.antlr4_active.speakQueryLexer import speakQueryLexer  # Import the regenerated lexer
        from lexers.antlr4_active.speakQueryParser import speakQueryParser  # Import the regenerated parser
    except Exception as e:
        logging.error(f'[!] Failure to import the updated components\n\t{e}')
    run_unit_tests(log_filename)  # Run unit tests

#!/usr/bin/env python3

import yaml
from antlr4 import *
from antlr4.InputStream import InputStream
if "." in __name__:
    from .whereClauseLexer import whereClauseLexer
    from .whereClauseParser import whereClauseParser
else:
    from whereClauseLexer import whereClauseLexer
    from whereClauseParser import whereClauseParser
from whereClauseListener import whereClauseListener
import logging

logging.basicConfig(level=logging.INFO)


def load_yaml_tests(file_path):
    """
    Load and parse YAML file containing test cases.
    """
    with open(file_path, 'r') as file:
        test_cases = yaml.safe_load(file)
    return test_cases


def process_query(query, listener):
    """
    Parse a single query using ANTLR4 and process it with the provided listener.
    """
    input_stream = InputStream(query)
    lexer = whereClauseLexer(input_stream)
    stream = CommonTokenStream(lexer)
    parser = whereClauseParser(stream)
    tree = parser.whereClause()
    walker = ParseTreeWalker()
    walker.walk(listener, tree)


def main():
    # Example: Load and process multiple test cases from YAML
    # test_cases = load_yaml_tests('test_cases.yaml')
    # for test in test_cases:
    #     listener = whereClauseListener(test['where_clause'])
    #     process_query(test['where_clause'], listener)

    # Example: Process a single query
    single_query = 'where columnName == "value"'
    single_query_listener = whereClauseListener(single_query)
    process_query(single_query, single_query_listener)


if __name__ == "__main__":
    main()

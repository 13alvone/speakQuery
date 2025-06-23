#!/usr/bin/env python3
import os
import sys
import antlr4
import logging
import pytest

pytest.skip("cpp modules unavailable", allow_module_level=True)

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lexers.antlr4_active.speakQueryLexer import speakQueryLexer
from lexers.antlr4_active.speakQueryParser import speakQueryParser
from lexers.speakQueryListener import speakQueryListener

logger = logging.getLogger(__name__)

def test_process_index_calls():
    """
    This function runs a series of test cases, each containing:
      - 'description': a short label for the test
      - 'original_query': the actual Splunk-like query
      - 'tokens': how that query might be tokenized (fed to cpp_index_call)
      - 'expected_output': the rows that should be returned (as a list of dicts).
    """

    test_cases = [
        {
            'description': 'Test Case 1 (New): Single date, success logs from success2.parquet',
            'original_query': '''index="system_logs/successes/success2.parquet"
| search status="success" earliest="2024-01-07" latest="2024-01-07"''',
            'search_tokens': ['status', '=', '"success"', 'earliest', '=', '"2024-01-07"', 'latest', '=', '"2024-01-07"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-07',
                    'level': 'INFO',
                    'message': 'Operation completed successfully',
                    'userRole': 'guest',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 3,
                    'test': 8,
                    'z': 5
                }
            ]
        },
        {
            'description': 'Test Case 2 (New): Single date, userRole=admin from success1.parquet',
            'original_query': '''index="system_logs/successes/success1.parquet"
| search userRole="admin" earliest="2024-01-06" latest="2024-01-06"''',
            'search_tokens': ['userRole', '=', '"admin"', 'earliest', '=', '"2024-01-06"', 'latest', '=', '"2024-01-06"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-06',
                    'level': 'INFO',
                    'message': 'Data processed successfully',
                    'userRole': 'admin',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 5,
                    'test': 12,
                    'z': 6
                }
            ]
        },
        {
            'description': 'Test Case 3 (New): Complex boolean (status="error" AND userRole="ericadmin") OR (status="warning") on 2024-01-03 to 2024-01-04',
            'original_query': '''index="system_logs/error_tracking/error2.parquet"
| search (status="error" AND userRole="ericadmin") OR (status="warning") earliest="2024-01-03" latest="2024-01-04"''',
            'search_tokens': ['(', 'status', '=', '"error"', 'AND', 'userRole', '=', '"ericadmin"', ')', 'OR', '(', 'status', '=', '"warning"', ')', 'earliest', '=', '"2024-01-03"', 'latest', '=', '"2024-01-04"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-03',
                    'level': 'ERROR',
                    'message': 'Error occurred in module C',
                    'userRole': 'ericadmin',
                    'error_code': 12,
                    'status': 'error',
                    'errorCode': 403,
                    'x': 4,
                    'test': 11,
                    'z': 6
                },
                {
                    'timestamp': '2024-01-04',
                    'level': 'ERROR',
                    'message': 'Error occurred in module D',
                    'userRole': 'guest',
                    'error_code': 500,
                    'status': 'warning',
                    'errorCode': 500,
                    'x': 6,
                    'test': 14,
                    'z': 8
                }
            ]
        },
        {
            'description': 'Test Case 4 (New): Single day (2024-01-10) logs with level="INFO" from test1.parquet',
            'original_query': '''index="output_parquets/test1.parquet"
| search level="INFO" earliest="2024-01-10" latest="2024-01-10"''',
            'search_tokens': ['level', '=', '"INFO"', 'earliest', '=', '"2024-01-10"', 'latest', '=', '"2024-01-10"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-10',
                    'level': 'INFO',
                    'message': 'Info message',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'info',
                    'errorCode': 0,
                    'x': 4,
                    'test': 12,
                    'z': 6
                }
            ]
        },
        {
            'description': 'Test Case 5 (New): System alerts - status=critical with errorCode=503 or 404 from 2023-01-03 to 2023-01-08',
            'original_query': '''index="error_tracking/system_alerts.parquet"
| search status="critical" (errorCode=503 OR errorCode=404) earliest="2023-01-03" latest="2023-01-08"''',
            'search_tokens': ['status', '=', '"critical"', '(', 'errorCode', '=', '503', 'OR', 'errorCode', '=', '404', ')', 'earliest', '=', '"2023-01-03"', 'latest', '=', '"2023-01-08"'],
            'expected_output': [
                {
                    'timestamp': '2023-01-03',
                    'status': 'critical',
                    'errorCode': 503.0,
                    'warningCode': 200.0,
                    'userRole': 'admin',
                    'action': 'transaction',
                    'success': True
                    # other fields omitted for brevity
                }
            ]
        },
        {
            'description': 'Test Case 6 (New): System alerts action="login" success=False and (warningCode=200 OR warningCode=300)',
            'original_query': '''index="error_tracking/system_alerts.parquet"
| search action="login" success=False (warningCode=200 OR warningCode=300)''',
            'search_tokens': ['action', '=', '"login"', 'success', '=', 'False', '(', 'warningCode', '=', '200', 'OR', 'warningCode', '=', '300', ')'],
            'expected_output': [
                {
                    'timestamp': '2023-01-02',
                    'status': 'in_progress',
                    'errorCode': 403.0,
                    'warningCode': 300.0,
                    'userRole': 'superuser',
                    'action': 'login',
                    'success': False
                    # ... other fields omitted
                }
            ]
        },
        {
            'description': 'Test Case 7 (New): test0.parquet, (level="ERROR" OR level="CRITICAL"), x>4, earliest=2024-01-04, latest=2024-01-05',
            'original_query': '''index="output_parquets/test0.parquet"
| search (level="ERROR" OR level="CRITICAL") x>4 earliest="2024-01-04" latest="2024-01-05"''',
            'search_tokens': ['(', 'level', '=', '"ERROR"', 'OR', 'level', '=', '"CRITICAL"', ')', 'x', '>', '4', 'earliest', '=', '"2024-01-04"', 'latest', '=', '"2024-01-05"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-05',
                    'level': 'CRITICAL',
                    'message': 'Critical message',
                    'userRole': 'admin',
                    'error_code': 2,
                    'status': 'critical',
                    'errorCode': 500,
                    'x': 6,
                    'test': 14,
                    'z': 9
                }
            ]
        },
        {
            'description': 'Test Case 8 (New): system_logs/successes/*, level="INFO" test IN (8,12,13)',
            'original_query': '''index="system_logs/successes/*"
| search level="INFO" test IN (8 12 13)''',
            'search_tokens': ['level', '=', '"INFO"', 'test', 'IN', '(', '8', '12', '13', ')'],
            'expected_output': [
                {
                    'timestamp': '2024-01-06',
                    'level': 'INFO',
                    'message': 'Data processed successfully',
                    'userRole': 'admin',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 5,
                    'test': 12,
                    'z': 6
                },
                {
                    'timestamp': '2024-01-07',
                    'level': 'INFO',
                    'message': 'Operation completed successfully',
                    'userRole': 'guest',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 3,
                    'test': 8,
                    'z': 5
                },
                {
                    'timestamp': '2024-01-08',
                    'level': 'INFO',
                    'message': 'Data processed successfully',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 4,
                    'test': 13,
                    'z': 7
                }
            ]
        },
        {
            'description': 'Test Case 9 (New): Error tracking logs on exactly 2024-01-04',
            'original_query': '''index="system_logs/error_tracking/*"
| search earliest="2024-01-04" latest="2024-01-04"''',
            'search_tokens': ['earliest', '=', '"2024-01-04"', 'latest', '=', '"2024-01-04"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-04',
                    'level': 'ERROR',
                    'message': 'Error occurred in module D',
                    'userRole': 'guest',
                    'error_code': 500,
                    'status': 'warning',
                    'errorCode': 500,
                    'x': 6,
                    'test': 14,
                    'z': 8
                }
            ]
        },
        {
            'description': 'Test Case 10 (New): From test1, userRole=admin and errorCode=404',
            'original_query': '''index="output_parquets/test1.parquet"
| search userRole="admin" errorCode=404''',
            'search_tokens': ['userRole', '=', '"admin"', 'errorCode', '=', '404'],
            'expected_output': [
                {
                    'timestamp': '2024-01-11',
                    'level': 'ERROR',
                    'message': 'Error message',
                    'userRole': 'admin',
                    'error_code': 1,
                    'status': 'error',
                    'errorCode': 404,
                    'x': 4,
                    'test': 13,
                    'z': 6
                }
            ]
        }
    ]


    # Iterate over each test case, calling the C++ function.
    for case in test_cases:
        logging.info(f"[i] {case['description']}")
        logging.info(f"[i] Original Query: {case['original_query']}")
        try:
            execute_speak_query(case['original_query'])
        except Exception as ex:
            logging.error(f"[x] Exception while processing tokens: {ex}")


def execute_speak_query(query_str: str):
    logging.info("Starting the parsing process.")
    cleaned_query = query_str.strip('\n').strip(' ').strip('\n').strip(' ')
    input_stream = antlr4.InputStream(cleaned_query)  # Replace 'input_text' with your test query
    lexer = speakQueryLexer(input_stream)  # Set up the lexer and parser
    stream = antlr4.CommonTokenStream(lexer)
    parser = speakQueryParser(stream)
    tree = parser.speakQuery()  # Parse the input to obtain the parse tree
    listener = speakQueryListener(cleaned_query)  # Create an instance of your custom listener
    walker = antlr4.ParseTreeWalker()  # Walk the parse tree using the custom listener
    walker.walk(listener, tree)


if __name__ == "__main__":
    test_process_index_calls()

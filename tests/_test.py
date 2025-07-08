#!/usr/bin/env python3
import os
import sys
import logging
import pytest
pytest.importorskip("pyarrow")

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(message)s')

# ------------------------------------------------------------------------------------------------
# Function to dynamically find project root by climbing up from the current file
# ------------------------------------------------------------------------------------------------
def find_project_root(start_path: str, marker_files=('app.py', '.git')):
    logging.debug("[DEBUG] Attempting to find project root...")
    current = os.path.abspath(start_path)
    while current != os.path.dirname(current):  # while not at root
        if any(os.path.exists(os.path.join(current, marker)) for marker in marker_files):
            logging.debug(f"[DEBUG] Project root identified: {current}")
            return current
        current = os.path.dirname(current)
    raise RuntimeError("Project root not found")

# Determine the project root (this script is in <project_root>/tests)
root_dir = find_project_root(__file__)
os.chdir(root_dir)
logging.info(f"[i] Project root: {root_dir}")

# Define module build directories for the compiled shared libraries
index_call_dir = os.path.join(root_dir, 'functionality', 'cpp_index_call', 'build')
datetime_parser_dir = os.path.join(root_dir, 'functionality', 'cpp_datetime_parser', 'build')
logging.info(f"[i] CPP Index Call Build Directory: {index_call_dir}")
logging.info(f"[i] CPP Datetime Parser Build Directory: {datetime_parser_dir}")

# Insert directories into sys.path so that custom modules can be imported
sys.path.insert(0, index_call_dir)
sys.path.insert(0, datetime_parser_dir)
sys.path.insert(0, root_dir)

logging.info("Using Python: " + sys.version)
logging.info("Sys path: " + str(sys.path))
logging.info(f"[i] Current working directory: {os.getcwd()} - Contents: {os.listdir(os.getcwd())}")

# Import the secure dynamic loader for .so files
try:
    from functionality.so_loader import resolve_and_import_so
except Exception as e:
    pytest.skip(f"so_loader not available: {e}", allow_module_level=True)

# Dynamically load the cpp_index_call shared object
try:
    cpp_index_module = resolve_and_import_so(index_call_dir, "cpp_index_call")
    process_index_calls = cpp_index_module.process_index_calls
    logging.info("[i] Successfully loaded 'cpp_index_call' module.")
except Exception as e:
    pytest.skip(f"cpp_index_call not available: {e}", allow_module_level=True)

# Dynamically load the cpp_datetime_parser shared object
try:
    cpp_datetime_module = resolve_and_import_so(datetime_parser_dir, "cpp_datetime_parser")
    parse_dates_to_epoch = cpp_datetime_module.parse_dates_to_epoch
    logging.info("[i] Successfully loaded 'cpp_datetime_parser' module.")
except Exception as e:
    pytest.skip(f"cpp_datetime_parser not available: {e}", allow_module_level=True)

# Import process_index_calls from the loaded module (if needed)
try:
    from cpp_index_call import process_index_calls
except Exception as e:
    pytest.skip(f"process_index_calls import failed: {e}", allow_module_level=True)


def test_process_index_calls():
    test_cases = [
        {
            'description': 'Test Case 1 (New): Success logs between 2024-01-05 and 2024-01-06 with status="success"',
            'original_query': 'index="system_logs/successes/*" status="success" earliest="2024-01-05" latest="2024-01-06"',
            'tokens': ['index', '=', '"system_logs/successes/*"', 'status', '=', '"success"', 'earliest', '=', '"2024-01-05"', 'latest', '=', '"2024-01-06"'],
            'expected_output': [
                {
                    'timestamp': '2024-01-05',
                    'level': 'INFO',
                    'message': 'Operation completed successfully',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'success',
                    'errorCode': 0,
                    'x': 4,
                    'test': 9,
                    'z': 5
                },
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
            'description': 'Test Case 2 (New): From test0.parquet userRole IN (admin guest) and z>=7',
            'original_query': 'index="output_parquets/test0.parquet" userRole IN (admin guest) z>=7',
            'tokens': ['index', '=', '"output_parquets/test0.parquet"', 'userRole', 'IN', '(', 'admin', 'guest', ')', 'z', '>=', '7'],
            'expected_output': [
                {
                    'timestamp': '2024-01-03',
                    'level': 'WARNING',
                    'message': 'Warning message',
                    'userRole': 'guest',
                    'error_code': 0,
                    'status': 'warning',
                    'errorCode': 0,
                    'x': 5,
                    'test': 12,
                    'z': 7
                },
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
            'description': 'Test Case 3 (New): Error tracking logs between 2024-01-03 and 2024-01-04 with (status="error" AND userRole="ericadmin") OR (status="warning")',
            'original_query': '(index="system_logs/error_tracking/*" (status="error" AND userRole="ericadmin")) OR (status="warning") earliest="2024-01-03" latest="2024-01-04"',
            'tokens': ['(', 'index', '=', '"system_logs/error_tracking/*"', '(', 'status', '=', '"error"', 'AND', 'userRole', '=', '"ericadmin"', ')', ')', 'OR', '(', 'status', '=', '"warning"', ')', 'earliest', '=', '"2024-01-03"', 'latest', '=', '"2024-01-04"'],
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
            'description': 'Test Case 4 (New): From test1.parquet level="INFO" on exactly 2024-01-10',
            'original_query': 'index="output_parquets/test1.parquet" level="INFO" earliest="2024-01-10" latest="2024-01-10"',
            'tokens': ['index', '=', '"output_parquets/test1.parquet"', 'level', '=', '"INFO"', 'earliest', '=', '"2024-01-10"', 'latest', '=', '"2024-01-10"'],
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
            'description': 'Test Case 5 (New): System alerts critical with errorCode=503 or 404 between 2023-01-03 and 2023-01-08',
            'original_query': 'index="error_tracking/system_alerts.parquet" status="critical" (errorCode=503 OR errorCode=404) earliest="2023-01-03" latest="2023-01-08"',
            'tokens': ['index', '=', '"error_tracking/system_alerts.parquet"', 'status', '=', '"critical"', '(', 'errorCode', '=', '503', 'OR', 'errorCode', '=', '404', ')', 'earliest', '=', '"2023-01-03"', 'latest', '=', '"2023-01-08"'],
            'expected_output': [
                {
                    'timestamp': '2023-01-03',
                    'status': 'critical',
                    'errorCode': 503.0,
                    'warningCode': 200.0,
                    'userRole': 'admin',
                    'action': 'transaction',
                    'success': True
                    # ... other fields omitted
                }
            ]
        },
        {
            'description': 'Test Case 6 (New): System alerts action="login" success=False and (warningCode=200 OR warningCode=300)',
            'original_query': 'index="error_tracking/system_alerts.parquet" action="login" success=False (warningCode=200 OR warningCode=300)',
            'tokens': ['index', '=', '"error_tracking/system_alerts.parquet"', 'action', '=', '"login"', 'success', '=', 'False', '(', 'warningCode', '=', '200', 'OR', 'warningCode', '=', '300', ')'],
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
            'description': 'Test Case 7 (New): From test0 level=ERROR or CRITICAL, between 2024-01-04 and 2024-01-05, and x>4',
            'original_query': 'index="output_parquets/test0.parquet" (level="ERROR" OR level="CRITICAL") x>4 earliest="2024-01-04" latest="2024-01-05"',
            'tokens': ['index', '=', '"output_parquets/test0.parquet"', '(', 'level', '=', '"ERROR"', 'OR', 'level', '=', '"CRITICAL"', ')', 'x', '>', '4', 'earliest', '=', '"2024-01-04"', 'latest', '=', '"2024-01-05"'],
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
            'description': 'Test Case 8 (New): Success logs where level="INFO" and test IN (8,12,13)',
            'original_query': 'index="system_logs/successes/*" level="INFO" test IN (8 12 13)',
            'tokens': ['index', '=', '"system_logs/successes/*"', 'level', '=', '"INFO"', 'test', 'IN', '(', '8', '12', '13', ')'],
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
            'original_query': 'index="system_logs/error_tracking/*" earliest="2024-01-04" latest="2024-01-04"',
            'tokens': ['index', '=', '"system_logs/error_tracking/*"', 'earliest', '=', '"2024-01-04"', 'latest', '=', '"2024-01-04"'],
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
            'description': 'Test Case 10 (New): From test1 userRole="admin" and errorCode=404',
            'original_query': 'index="output_parquets/test1.parquet" userRole="admin" errorCode=404',
            'tokens': ['index', '=', '"output_parquets/test1.parquet"', 'userRole', '=', '"admin"', 'errorCode', '=', '404'],
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

    for case in test_cases:
        print("=" * 80)
        print(case['description'])
        df = process_index_calls(case['tokens'])
        if df.empty:
            print("DataFrame is empty.\n")
        else:
            print(df)
            print("\n")

if __name__ == '__main__':
    test_process_index_calls()


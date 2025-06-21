import os
import sys

# Insert paths for both modules
sys.path.insert(0, os.path.join(os.getcwd(), 'functionality', 'cpp_index_call', 'build'))
sys.path.insert(0, os.path.join(os.getcwd(), 'functionality', 'cpp_datetime_parser', 'build'))

try:
    from cpp_index_call import process_index_calls
except ImportError:
    print("Could not import cpp_index_call. Check build and placement of the .so file.")
    sys.exit(1)

try:
    from cpp_datetime_parser import parse_dates_to_epoch
except ImportError:
    print("Could not import cpp_datetime_parser. Check build and placement of the .so file.")


def test_process_index_calls():
    test_cases = [
        {
            'description': 'Test Case 1: Successes with test>=10 or userRole="admin"',
            'original_query': 'index="system_logs/successes/*" (test>=10 OR userRole="admin")',
            'tokens': ['index', '=', '"system_logs/successes/*"', '(', 'test', '>=', '10', 'OR', 'userRole', '=', '"admin"', ')'],
            'expected_output': [
                # Matches from success1.parquet row1:
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
                # Matches from success2.parquet row1:
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
            'description': 'Test Case 2: Error Tracking with status="error" in a time window',
            'original_query': 'index="system_logs/error_tracking/*" status="error" earliest="2024-01-02" latest="2024-01-04"',
            'tokens': ['index', '=', '"system_logs/error_tracking/*"', 'status', '=', '"error"', 'earliest', '=', '"2024-01-02"', 'latest', '=', '"2024-01-04"'],
            'expected_output': [
                # Matches from error2.parquet row0 (2024-01-03):
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
                }
            ]
        },
        {
            'description': 'Test Case 3: Test0 with x=4 and test IN (10,13)',
            'original_query': 'index="output_parquets/test0.parquet" x=4 test IN (10 13)',
            'tokens': ['index', '=', '"output_parquets/test0.parquet"', 'x', '=', '4', 'test', 'IN', '(', '10', '13', ')'],
            'expected_output': [
                # Matches from test0.parquet row0:
                {
                    'timestamp': '2024-01-01',
                    'level': 'DEBUG',
                    'message': 'Debug message',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'debug',
                    'errorCode': 0,
                    'x': 4,
                    'test': 10,
                    'z': 5
                },
                # Matches from test0.parquet row3:
                {
                    'timestamp': '2024-01-04',
                    'level': 'ERROR',
                    'message': 'Error message',
                    'userRole': 'user',
                    'error_code': 1,
                    'status': 'error',
                    'errorCode': 403,
                    'x': 4,
                    'test': 13,
                    'z': 8
                }
            ]
        },
        {
            'description': 'Test Case 4: Test1 with level="INFO" and z<=7',
            'original_query': 'index="output_parquets/test1.parquet" level="INFO" z<=7',
            'tokens': ['index', '=', '"output_parquets/test1.parquet"', 'level', '=', '"INFO"', 'z', '<=', '7'],
            'expected_output': [
                # Matches from test1.parquet row0:
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
                },
                # Matches from test1.parquet row2:
                {
                    'timestamp': '2024-01-12',
                    'level': 'INFO',
                    'message': 'Another info message',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'info',
                    'errorCode': 0,
                    'x': 4,
                    'test': 14,
                    'z': 7
                }
            ]
        },
        {
            'description': 'Test Case 5: Successes with x>=4 OR error_tracking with critical and x=5',
            'original_query': '(index="system_logs/successes/*" status="success" x>=4) OR (index="system_logs/error_tracking/*" status="critical" x=5)',
            'tokens': ['(', 'index', '=', '"system_logs/successes/*"', 'status', '=', '"success"', 'x', '>=', '4', ')', 'OR', '(', 'index', '=', '"system_logs/error_tracking/*"', 'status', '=', '"critical"', 'x', '=', '5', ')'],
            'expected_output': [
                # success1.parquet row0 (x=4), row1 (x=5), success2.parquet row1 (x=4)
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
                },
                # error1.parquet row1: critical x=5
                {
                    'timestamp': '2024-01-02',
                    'level': 'ERROR',
                    'message': 'Error occurred in module B',
                    'userRole': 'user',
                    'error_code': 404,
                    'status': 'critical',
                    'errorCode': 404,
                    'x': 5,
                    'test': 15,
                    'z': 7
                }
            ]
        },
        {
            'description': 'Test Case 6: Test0 with (status IN (debug warning) OR (errorCode=500 AND test=14))',
            'original_query': 'index="output_parquets/test0.parquet" (status IN (debug warning) OR (errorCode=500 AND test=14))',
            'tokens': ['index', '=', '"output_parquets/test0.parquet"', '(', 'status', 'IN', '(', 'debug', 'warning', ')', 'OR', '(', 'errorCode', '=', '500', 'AND', 'test', '=', '14', ')', ')'],
            'expected_output': [
                # Row0: debug
                {
                    'timestamp': '2024-01-01',
                    'level': 'DEBUG',
                    'message': 'Debug message',
                    'userRole': 'user',
                    'error_code': 0,
                    'status': 'debug',
                    'errorCode': 0,
                    'x': 4,
                    'test': 10,
                    'z': 5
                },
                # Row2: warning
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
                # Row4: critical with errorCode=500 and test=14
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
            'description': 'Test Case 7: System Alerts with critical admin in time range',
            'original_query': 'index="error_tracking/system_alerts.parquet" status="critical" userRole="admin" earliest="2023-01-01" latest="2023-01-11"',
            'tokens': ['index', '=', '"error_tracking/system_alerts.parquet"', 'status', '=', '"critical"', 'userRole', '=', '"admin"', 'earliest', '=', '"2023-01-01"', 'latest', '=', '"2023-01-11"'],
            'expected_output': [
                # Matches Row2 (2023-01-03) critical admin
                {
                    'timestamp': '2023-01-03',
                    'status': 'critical',
                    'errorCode': 503.0,
                    'warningCode': 200.0,
                    'userRole': 'admin',
                    'action': 'transaction',
                    'success': True,
                    # ... other fields omitted for brevity
                },
                # Matches Row10 (2023-01-11) critical admin
                {
                    'timestamp': '2023-01-11',
                    'status': 'critical',
                    'errorCode': None,
                    'warningCode': 200.0,
                    'userRole': 'admin',
                    'action': 'backup',
                    'success': False,
                    # ... other fields omitted for brevity
                }
            ]
        },
        {
            'description': 'Test Case 8: System Alerts (errorCode=500 OR warningCode=300) and success=True',
            'original_query': 'index="error_tracking/system_alerts.parquet" (errorCode=500 OR warningCode=300) success=True',
            'tokens': ['index', '=', '"error_tracking/system_alerts.parquet"', '(', 'errorCode', '=', '500', 'OR', 'warningCode', '=', '300', ')', 'success', '=', 'True'],
            'expected_output': [
                # Row6: errorCode=500.0, warningCode=300.0, success=True
                {
                    'timestamp': '2023-01-07',
                    'status': 'failed',
                    'errorCode': 500.0,
                    'warningCode': 300.0,
                    'userRole': 'admin',
                    'action': 'login',
                    'success': True,
                    # ... other fields omitted for brevity
                }
            ]
        },
        {
            'description': 'Test Case 9: (test1 level=ERROR OR test0 errorCode=0) in date range',
            'original_query': '(index="output_parquets/test1.parquet" level="ERROR" OR index="output_parquets/test0.parquet" errorCode=0) earliest="2024-01-10" latest="2024-01-11"',
            'tokens': ['(', 'index', '=', '"output_parquets/test1.parquet"', 'level', '=', '"ERROR"', 'OR', 'index', '=', '"output_parquets/test0.parquet"', 'errorCode', '=', '0', ')', 'earliest', '=', '"2024-01-10"', 'latest', '=', '"2024-01-11"'],
            'expected_output': [
                # Matches test1.parquet row1: level=ERROR within date range
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
        },
        {
            'description': 'Test Case 10: System Alerts with nested conditions and date range',
            'original_query': 'index="error_tracking/system_alerts.parquet" ((status="info" AND success=False) OR (status="critical" AND action="backup")) earliest="2023-01-01" latest="2023-01-04"',
            'tokens': ['index', '=', '"error_tracking/system_alerts.parquet"', '(', '(', 'status', '=', '"info"', 'AND', 'success', '=', 'False', ')', 'OR', '(', 'status', '=', '"critical"', 'AND', 'action', '=', '"backup"', ')', ')', 'earliest', '=', '"2023-01-01"', 'latest', '=', '"2023-01-04"'],
            'expected_output': [
                # Row0: 2023-01-01 status=info success=False matches first condition
                {
                    'timestamp': '2023-01-01',
                    'status': 'info',
                    'errorCode': None,
                    'warningCode': None,
                    'userRole': 'manager',
                    'action': 'logout',
                    'success': False,
                    # ... other fields omitted for brevity
                }
            ]
        }
    ]

    for case in test_cases:
        print(f"{case['description']}")
        df = process_index_calls(case['tokens'])
        if df.empty:
            print("DataFrame is empty.\n")
        else:
            print(df)
            print("\n")


if __name__ == '__main__':
    test_process_index_calls()

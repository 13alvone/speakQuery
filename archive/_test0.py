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
            'description': 'Test Case 1: Basic Query with No Filters',
            'tokens': ['index', '=', '"system_logs/*"'],
        },
        {
            'description': 'Test Case 2: Query with OR Filter',
            'tokens': ['(', 'userRole', '=', '"admin"', 'OR', 'userRole', '=', '"ericadmin"', ')', 'index', '=', '"system_logs/*"'],
        },
        {
            'description': 'Test Case 3: Multiple Index Calls with Filters',
            'tokens': ['(', 'index', '=', '"system_logs/error_tracking/*"', 'error_code', '=', '12', ')', 'OR', '(', 'index', '=', '"system_logs/successes/*"', 'earliest', '=', '"2024-01-01"', 'latest', '=', '"2024-10-11"', ')'],
        },
        {
            'description': 'Test Case 4: Complex Query with AND/OR Filters and IN Clause',
            'tokens': ['(', 'status', '=', '"error"', 'OR', 'status', '=', '"critical"', ')', 'AND', 'errorCode', 'IN', '(', '403', '404', ')', 'index', '=', '"system_logs/error_tracking/*"'],
        },
        {
            'description': 'Test Case 5: Nested Parentheses and Complex Filters',
            'tokens': ['earliest', '=', '"2024-01-01"', 'latest', '=', '"2024-10-11"', '(', 'x', '=', '4', 'AND', 'test', '<=', '12', ')', 'OR', 'z', '=', '6', 'index', '=', '"output_parquets/test0.parquet"'],
        },
        {
            'description': 'Test Case 6: Implied AND between Filters',
            'tokens': ['earliest', '=', '"2024-01-01"', 'latest', '=', '"2024-10-11"', 'x', '=', '4', 'y', '=', '5', 'OR', 'z', '=', '6', 'index', '=', '"output_parquets/test1.parquet"'],
        },
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
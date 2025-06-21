import logging
import sys
import re
from pyparsing import (
    infixNotation, opAssoc, Keyword, Word, alphas, alphanums, nums, quotedString,
    removeQuotes, oneOf, ParserElement, Group, delimitedList, Suppress, Forward
)
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import numpy as np
import os

# Import your custom Rust datetime parser
import r_datetime_parser

# Enable packrat parsing for better performance with pyparsing
ParserElement.enablePackrat()


class IndexFilterHandler:
    def __init__(self, query):
        self.short_circuit = None
        self.query_str = query
        self.parsed_query = None
        self.parsed_filter = None
        self.pyarrow_filter = None
        self.remaining_query = ''
        # Use sys.path[0] to get the script's directory
        script_dir = os.path.abspath(sys.path[0])
        # Assuming your script is in 'lexers/' and the project root is one level up
        project_root = os.path.abspath(os.path.join(script_dir, '..'))
        self.indexes_dir = os.path.abspath(os.path.join(project_root, 'indexes'))

        # Debug statements to verify paths (you can remove these after testing)
        print(f"script_dir: {script_dir}")
        print(f"project_root: {project_root}")
        print(f"indexes_dir: {self.indexes_dir}")

    def parse_query(self):
        query_text = self.query_str.strip()
        result = {
            'earliest': None,
            'latest': None,
            'indexes': [],
            'filter_expression': None
        }

        # Split the query at the first pipe '|'
        parts = query_text.split('|', 1)
        initial_part = parts[0].strip()
        remaining_part = '|' + parts[1] if len(parts) > 1 else ''

        self.remaining_query = remaining_part.strip()  # Store the remaining query including the '|'

        earliest_str = None
        latest_str = None

        # Updated regex patterns to handle quoted strings and spaces
        earliest_pattern = r'earliest\s*=\s*(?:"([^"]+)"|\'([^\']+)\'|(\S+))'
        latest_pattern = r'latest\s*=\s*(?:"([^"]+)"|\'([^\']+)\'|(\S+))'
        index_pattern = r'index\s*=\s*\"([^\"]+)\"'

        # Extract earliest
        earliest_match = re.search(earliest_pattern, initial_part)
        if earliest_match:
            earliest_str = earliest_match.group(1) or earliest_match.group(2) or earliest_match.group(3)
            earliest_str = earliest_str.strip('"').strip("'")

        # Extract latest
        latest_match = re.search(latest_pattern, initial_part)
        if latest_match:
            latest_str = latest_match.group(1) or latest_match.group(2) or latest_match.group(3)
            latest_str = latest_str.strip('"').strip("'")

        # Extract indexes
        index_match = re.search(index_pattern, initial_part)
        if index_match:
            indexes = index_match.group(1)
            index_list = [idx.strip() for idx in indexes.split(',')]
            sanitized_indexes = []
            for idx in index_list:
                # Remove any leading slashes
                idx = idx.lstrip('/\\')
                # Check for directory traversal attempts
                if '..' in idx or idx.startswith('/'):
                    raise ValueError("Invalid index path: directory traversal is not allowed.")
                # Prepend indexes directory
                idx_path = os.path.join(self.indexes_dir, idx)
                # Resolve to absolute path
                idx_realpath = os.path.realpath(idx_path)

                # Debug statements to verify paths (you can remove these after testing)
                print(f"idx: {idx}")
                print(f"idx_path: {idx_path}")
                print(f"idx_realpath: {idx_realpath}")

                # Ensure the path is within 'indexes/' directory
                if not idx_realpath.startswith(self.indexes_dir):
                    raise ValueError("Invalid index path: must be within 'indexes/' directory.")
                sanitized_indexes.append(idx_realpath)
            result['indexes'] = sanitized_indexes

        # Remove the extracted parts from the initial_part to isolate the filter expression
        filter_part = initial_part
        filter_part = re.sub(earliest_pattern, '', filter_part)
        filter_part = re.sub(latest_pattern, '', filter_part)
        filter_part = re.sub(index_pattern, '', filter_part)

        # The remaining text is the filter expression
        filter_expression = filter_part.strip()
        result['filter_expression'] = filter_expression if filter_expression else None

        # Store the remaining query
        self.remaining_query = remaining_part.strip()

        # Convert earliest and latest to epoch timestamps
        if earliest_str:
            try:
                if earliest_str.isdigit():
                    # If it's already a number, use it directly
                    result['earliest'] = int(earliest_str)
                else:
                    # Attempt to parse using r_datetime_parser
                    earliest_epoch_list = r_datetime_parser.parse_dates_to_epoch([earliest_str])
                    result['earliest'] = earliest_epoch_list[0]  # Get the first (and only) item
            except Exception as e:
                print(f"Error parsing earliest date: {e}")
                result['earliest'] = None
        else:
            result['earliest'] = None

        if latest_str:
            try:
                if latest_str.isdigit():
                    result['latest'] = int(latest_str)
                else:
                    latest_epoch_list = r_datetime_parser.parse_dates_to_epoch([latest_str])
                    result['latest'] = latest_epoch_list[0]
            except Exception as e:
                print(f"Error parsing latest date: {e}")
                result['latest'] = None
        else:
            result['latest'] = None

        self.parsed_query = result

    def parse_filter_expression(self):
        # [Method remains unchanged]
        expression = self.parsed_query['filter_expression']

        if not expression:
            self.parsed_filter = None
            return

        # Define the grammar
        AND = Keyword("AND", caseless=True)
        OR = Keyword("OR", caseless=True)
        NOT = Keyword("NOT", caseless=True)
        IN = Keyword("IN", caseless=True)
        EQ = oneOf("== =")
        NEQ = oneOf("!= <>")
        GT = ">"
        LT = "<"
        GE = ">="
        LE = "<="
        comparison_ops = EQ | NEQ | GE | LE | GT | LT

        identifier = Word(alphas + "_", alphanums + "_")
        integer = Word(nums)
        quoted_str = quotedString.setParseAction(removeQuotes)
        value = quoted_str | integer | identifier

        expr = Forward()

        # Use delimitedList to parse values in IN operator
        value_list = Suppress('(') + delimitedList(value) + Suppress(')')

        in_operation = Group(identifier + IN + Group(value_list))

        comparison = Group(
            (identifier + comparison_ops + value) |
            in_operation
        )

        # Suppress parentheses in atoms
        atom = comparison | (Suppress("(") + expr + Suppress(")"))

        expr <<= infixNotation(
            atom,
            [
                (NOT, 1, opAssoc.RIGHT),
                (AND, 2, opAssoc.LEFT),
                (OR, 2, opAssoc.LEFT),
            ],
        )

        # Parse the expression
        try:
            parsed = expr.parseString(expression, parseAll=True)
            self.parsed_filter = parsed.asList()[0]
        except Exception as e:
            print(f"Error parsing filter expression: {e}")
            self.parsed_filter = None

    def build_pyarrow_filter(self):
        # [Method remains unchanged]
        parsed_expression = self.parsed_filter
        earliest = self.parsed_query['earliest']
        latest = self.parsed_query['latest']

        def convert_value(value):
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    return value  # Keep as string

        def recurse(expr, schema_fields):
            # Flatten any unnecessary nesting
            while isinstance(expr, list) and len(expr) == 1:
                expr = expr[0]

            if isinstance(expr, str):
                return expr

            elif isinstance(expr, list):
                if len(expr) == 3 and isinstance(expr[1], str):
                    # Binary operation or comparison
                    if expr[1].upper() in ["AND", "OR"]:
                        left = recurse(expr[0], schema_fields)
                        operator = expr[1]
                        right = recurse(expr[2], schema_fields)

                        if operator.upper() == "AND":
                            return left & right
                        elif operator.upper() == "OR":
                            return left | right
                    else:
                        # Comparison operator
                        field = expr[0]
                        op = expr[1]
                        value = expr[2]

                        # Check if field exists in schema
                        if field not in schema_fields:
                            print(f"Warning: Field '{field}' does not exist in the dataset schema. Skipping condition.")
                            return pa.scalar(True)
                        field_ref = ds.field(field)
                        if isinstance(value, list):
                            # Handle IN operator
                            values = [convert_value(v) for v in value]
                            return field_ref.isin(values)
                        else:
                            scalar_value = convert_value(value)
                            if op in ("==", "="):
                                return field_ref == scalar_value
                            elif op in ("!=", "<>"):
                                return field_ref != scalar_value
                            elif op == ">=":
                                return field_ref >= scalar_value
                            elif op == "<=":
                                return field_ref <= scalar_value
                            elif op == ">":
                                return field_ref > scalar_value
                            elif op == "<":
                                return field_ref < scalar_value
                            else:
                                raise ValueError(f"Unknown operator: {op}")

                elif len(expr) == 2:
                    # Unary operation (e.g., NOT)
                    operator = expr[0]
                    operand = recurse(expr[1], schema_fields)
                    if operator.upper() == "NOT":
                        return ~operand
                    else:
                        raise ValueError(f"Unknown unary operator: {operator}")

                else:
                    # Complex nested expression
                    if len(expr) % 2 == 1:
                        res_filter = recurse(expr[0], schema_fields)
                        i = 1
                        while i < len(expr):
                            operator = expr[i]
                            next_operand = recurse(expr[i + 1], schema_fields)

                            if operator.upper() == "AND":
                                res_filter = res_filter & next_operand
                            elif operator.upper() == "OR":
                                res_filter = res_filter | next_operand
                            else:
                                raise ValueError(f"Unknown operator in expression: {operator}")

                            i += 2
                        return res_filter
                    else:
                        raise ValueError(f"Invalid expression format: {expr}")

            else:
                raise ValueError(f"Unknown expression type: {expr}")

        # Build the filter from the parsed expression
        result_filter = None
        if parsed_expression:
            # Get the schema fields from the dataset
            if self.parsed_query['indexes']:
                dataset_paths = self.parsed_query['indexes']
                dataset = ds.dataset(dataset_paths, format="system4.system4.parquet")
                schema_fields = [field.name for field in dataset.schema]
            else:
                schema_fields = []

            # If _epoch doesn't exist in the target schema and earliest and/or latest is set, return nothing
            if '_epoch' not in schema_fields:
                if earliest and isinstance(earliest, int) and earliest > 0:
                    self.short_circuit = True
                    return
                elif latest and isinstance(latest, int) and latest > 0:
                    self.short_circuit = True
                    return

            result_filter = recurse(parsed_expression, schema_fields)

        # Add earliest and latest epoch conditions if provided
        if earliest is not None:
            earliest_condition = ds.field('_epoch') >= earliest
            if result_filter is not None:
                result_filter = result_filter & earliest_condition
            else:
                result_filter = earliest_condition

        if latest is not None:
            latest_condition = ds.field('_epoch') <= latest
            if result_filter is not None:
                result_filter = result_filter & latest_condition
            else:
                result_filter = latest_condition

        self.pyarrow_filter = result_filter

    def get_filtered_data(self):
        # Ensure that parsing and filter building steps have been completed
        if self.parsed_query is None:
            self.parse_query()
        if self.parsed_filter is None:
            self.parse_filter_expression()
        if self.pyarrow_filter is None:
            self.build_pyarrow_filter()

        if self.short_circuit:
            self.short_circuit = False
            return f'[USER SYNTAX ERROR] - _epoch field is missing from target index file(s). ' \
                   f'Add _epoch field to index file{"s" if len(self.parsed_query["indexes"]) else " "}' \
                   f'\'{",".join(x.split("/indexes/")[-1] for x in self.parsed_query["indexes"])}\' ' \
                   f'and try to run the query again.'

        indexes = self.parsed_query['indexes']
        pyarrow_filter = self.pyarrow_filter

        if not indexes:
            print("No indexes specified.")
            return pd.DataFrame()

        # Create a dataset from the list of system4.system4.parquet files
        dataset = ds.dataset(indexes, format="system4.system4.parquet")

        # Read the data with the filter applied
        try:
            if pyarrow_filter is not None:
                table = dataset.to_table(filter=pyarrow_filter)
            else:
                table = dataset.to_table()
        except Exception as e:
            print(f"Error applying filter: {e}")
            table = dataset.to_table()

        # Return the pandas DataFrame
        return table.to_pandas()

    def get_remaining_query(self):
        """
        Returns the remaining part of the query after extracting the filters.

        Returns:
            str: The remaining query string.
        """
        return self.remaining_query


# Main execution
if __name__ == "__main__":
    # Create sample data and save as system4.system4.parquet files
    output_dir = ''
    os.makedirs(output_dir, exist_ok=True)

    # Create two sample dataframes
    def create_sample_dataframe():
        data = {
            'x': np.random.randint(1, 10, size=100),
            'y': np.random.randint(1, 10, size=100),
            'z': np.random.randint(1, 10, size=100),
            'a': np.random.randint(1, 10, size=100),
            'test': np.random.choice(['something', 'something_else', 'test', 'other'], size=100),
            'timestamp': pd.date_range(start='2024-01-01', periods=100, freq='D'),
            'header_0': np.random.choice(['idea', 'concept', 'plan'], size=100),
            'header_1': np.random.randint(1, 100, size=100),
            'header_2': np.random.choice(['alpha', 'beta', 'gamma'], size=100),
            'header_3': np.random.choice(['pass', 'fail', 'success'], size=100)
        }
        df_sample = pd.DataFrame(data)
        # Add _epoch column
        df_sample['_epoch'] = df_sample['timestamp'].astype(np.int64) // 10 ** 9
        return df_sample

    def save_dataframe_as_parquet(df, filename):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)

    df1 = create_sample_dataframe()
    df2 = create_sample_dataframe()

    # File paths
    file1 = os.path.join(output_dir, 'output_2024-04-04_13-09-19.system4.system4.parquet')
    file2 = os.path.join(output_dir, 'output_2024-04-04_13-40-44.system4.system4.parquet')

    # Save dataframes as Parquet files
    save_dataframe_as_parquet(df1, file1)
    save_dataframe_as_parquet(df2, file2)

    # Define the query
    query_string = '''
    earliest="2024-01-01" latest="2024-10-11"
    (( x==4 AND y==5 ) OR z==6) AND a>=5 OR test IN ("something", "something_else", "test")
    index="output_parquets/output_2024-04-04_13-09-19.system4.system4.parquet,output_parquets/output_2024-04-04_13-40-44.system4.system4.parquet"
    | eval mv_value_plus_numeric_concat_test=concat(header_0, header_2)  # Concat 1: Concatenate 2 entries, one a mv field and the other a single string
    | eval three_entry_concat_test=concat(header_3, header_2, header_1)  # Concat 2: Concatenate 3 entries with strings and integers
    | eval replace_test=replace(header_0, "idea", "DUMBASS idea")  # Replace 1: Replace full word with other word for each
    | eval replace_test2=replace(header_3, "ss", "DUMBASS idea")  # Replace 2: Replace substring in each string provided.
    '''

    # Create an instance of IndexFilterHandler
    index_filter_handler = IndexFilterHandler(query_string)

    # Parse the query
    index_filter_handler.parse_query()

    # Print parsed components
    print("Earliest:", index_filter_handler.parsed_query['earliest'])
    print("Latest:", index_filter_handler.parsed_query['latest'])
    print("Indexes:", index_filter_handler.parsed_query['indexes'])
    print("Filter Expression:", index_filter_handler.parsed_query['filter_expression'])
    print("Remaining Query:", index_filter_handler.get_remaining_query())

    # Get the filtered data as a pandas DataFrame
    filtered_data = index_filter_handler.get_filtered_data()

    # Now you can process filtered_data further with your existing functionality
    # For demonstration, we print the DataFrame
    print(filtered_data.head())

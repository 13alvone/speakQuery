import logging
import sys
import glob
import pyparsing as pp
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
import numpy as np
import os

# Enable packrat parsing for better performance with pyparsing
pp.ParserElement.enablePackrat()

# Function to parse dates into epoch times (you can replace this with your Rust datetime parser)
def parse_dates_to_epoch(dates):
    epochs = pd.to_datetime(dates).astype(np.int64) // 10 ** 9
    return epochs.tolist()

class IndexFilterHandler:
    def __init__(self, query):
        self.query_str = query
        self.parsed_query = []  # List to hold multiple filter blocks
        self.pyarrow_filters = []
        self.remaining_query = ''
        self.short_circuit = False

        # Determine the base index directory
        script_dir = os.path.abspath(sys.path[0])
        project_root = os.path.abspath(os.path.pardir)
        self.indexes_dir = os.path.abspath(os.path.join(project_root, 'indexes'))

        # Configure logging
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    def parse_query(self):
        query_text = self.query_str.strip()

        # Remove comments (lines starting with '#') and excess whitespace
        query_lines = query_text.splitlines()
        cleaned_lines = []
        for line in query_lines:
            line = line.strip()
            if line.startswith('#') or line == '':
                continue
            cleaned_lines.append(line)
        cleaned_query = ' '.join(cleaned_lines)

        # Remove the part after the first pipe '|' to focus on the initial query
        parts = cleaned_query.split('|', 1)
        query_text = parts[0].strip()
        self.remaining_query = '|' + parts[1].strip() if len(parts) > 1 else ''

        # Parse the query text into filter blocks
        self.parsed_query = self.parse_filter_blocks(query_text)

    def parse_filter_blocks(self, text):
        # Define grammar for parsing the query
        lparen = pp.Suppress('(')
        rparen = pp.Suppress(')')
        equals = pp.Suppress('=')

        # Keywords
        EARLIEST = pp.Keyword("earliest", caseless=True)
        LATEST = pp.Keyword("latest", caseless=True)
        INDEX = pp.Keyword("index", caseless=True)
        AND = pp.Keyword("AND", caseless=True)
        OR = pp.Keyword("OR", caseless=True)
        IN = pp.Keyword("IN", caseless=True)
        NOT = pp.Keyword("NOT", caseless=True)

        # Identifiers and values
        identifier = pp.Word(pp.alphas + "_", pp.alphanums + "_")
        comparison_op = pp.oneOf("== != >= <= > <", caseless=True)
        integer = pp.Word(pp.nums)
        float_number = pp.Regex(r'\d+\.\d*')
        boolean = pp.oneOf("true false", caseless=True)
        quoted_str = pp.quotedString.setParseAction(pp.removeQuotes)
        value = quoted_str | float_number | integer | boolean

        # Time clauses
        time_value = quoted_str | pp.Word(pp.alphanums + "-_:")
        earliest_clause = EARLIEST + equals + time_value('earliest')
        latest_clause = LATEST + equals + time_value('latest')

        # Index clause
        index_clause = INDEX + equals + quoted_str('index')

        # Comparison and expressions
        expr = pp.Forward()
        comparison = pp.Group(identifier('field') + comparison_op('op') + value('value'))
        in_expression = pp.Group(identifier('field') + IN + lparen + pp.delimitedList(value)('values') + rparen)
        parens = pp.Group(lparen + expr + rparen)
        not_expr = pp.Group(NOT + expr)
        atom = comparison | in_expression | parens | not_expr

        expr <<= pp.infixNotation(atom,
                                  [
                                      (NOT, 1, pp.opAssoc.RIGHT),
                                      (AND, 2, pp.opAssoc.LEFT),
                                      (OR, 2, pp.opAssoc.LEFT),
                                  ])

        # Filter block
        filter_block = pp.Group(
            pp.Optional(lparen) +
            pp.ZeroOrMore(earliest_clause('earliest_clause') | latest_clause('latest_clause') | index_clause('index_clause') | expr('filter_expression')) +
            pp.Optional(rparen)
        )

        # Top-level filter expression
        filter_expr = pp.infixNotation(filter_block,
                                       [
                                           (NOT, 1, pp.opAssoc.RIGHT),
                                           (AND, 2, pp.opAssoc.LEFT),
                                           (OR, 2, pp.opAssoc.LEFT),
                                       ])

        # Overall grammar
        grammar = filter_expr

        # Parse the text
        try:
            parsed = grammar.parseString(text, parseAll=True)
            # Process the parsed expression recursively
            filter_blocks = self.process_parsed_expression(parsed[0])
            return filter_blocks
        except pp.ParseException as pe:
            logging.error(f"Failed to parse query: {pe}")
            return []
        except Exception as e:
            logging.error(f"An unexpected error occurred while parsing the query: {e}")
            return []

    def process_parsed_expression(self, parsed_expr):
        filter_blocks = []
        if isinstance(parsed_expr, pp.ParseResults):
            parsed_expr = parsed_expr.asList()
        if isinstance(parsed_expr, list):
            if len(parsed_expr) == 1:
                # Single filter block
                filter_block = self.extract_filter_block(parsed_expr[0])
                filter_blocks.append(filter_block)
            elif len(parsed_expr) == 3:
                # Binary operator between filter blocks
                left = self.process_parsed_expression(parsed_expr[0])
                operator = parsed_expr[1]
                right = self.process_parsed_expression(parsed_expr[2])
                # Combine the filter blocks
                filter_blocks.extend(left)
                filter_blocks.extend(right)
            else:
                # Complex expression, process each element
                for elem in parsed_expr:
                    blocks = self.process_parsed_expression(elem)
                    filter_blocks.extend(blocks)
        return filter_blocks

    def extract_filter_block(self, block):
        block_dict = {}
        if 'earliest_clause' in block:
            block_dict['earliest'] = block.earliest_clause.earliest
        else:
            block_dict['earliest'] = None
        if 'latest_clause' in block:
            block_dict['latest'] = block.latest_clause.latest
        else:
            block_dict['latest'] = None
        if 'filter_expression' in block:
            block_dict['filter_expression'] = block.filter_expression
        else:
            block_dict['filter_expression'] = None
        if 'index_clause' in block:
            block_dict['indexes'] = [block.index_clause.index]
        else:
            block_dict['indexes'] = []
        return block_dict

    def build_pyarrow_filters(self):
        if not self.parsed_query:
            self.parse_query()

        self.pyarrow_filters = []

        for filter_block in self.parsed_query:
            expr = filter_block['filter_expression']
            block_earliest = filter_block['earliest']
            block_latest = filter_block['latest']
            indexes = [os.path.join(self.indexes_dir, idx + '.parquet') for idx in filter_block.get('indexes', [])]

            if not indexes:
                logging.warning("No indexes specified for a filter block.")
                continue

            # Expand index paths with glob
            expanded_indexes = []
            for idx in indexes:
                matched_files = glob.glob(idx)
                if matched_files:
                    expanded_indexes.extend(matched_files)
                else:
                    logging.error(f"Index path '{idx}' does not match any files.")
            indexes = expanded_indexes

            if not indexes:
                logging.error("No valid index files found for this filter block.")
                continue

            # Get the schema fields from the first index
            dataset = ds.dataset(indexes, format="parquet")
            schema_fields = [field.name for field in dataset.schema]

            # Check for _epoch field if time filters are present
            if '_epoch' not in schema_fields and (block_earliest or block_latest):
                logging.error("_epoch field is missing from target index file(s).")
                self.short_circuit = True
                continue

            # Parse the filter expression into a PyArrow filter
            parsed_filter = None
            if expr:
                # Convert the parsed expression to PyArrow filter
                try:
                    parsed_filter = self.convert_to_pyarrow(expr, schema_fields)
                except Exception as e:
                    logging.error(f"Error converting filter expression to PyArrow: {e}")
                    parsed_filter = None

            # Apply earliest and latest if present
            if block_earliest is not None:
                try:
                    earliest_epoch_list = parse_dates_to_epoch([block_earliest])
                    block_earliest_epoch = earliest_epoch_list[0]
                    earliest_condition = ds.field('_epoch') >= block_earliest_epoch
                    if parsed_filter is not None:
                        parsed_filter = parsed_filter & earliest_condition
                    else:
                        parsed_filter = earliest_condition
                except Exception as e:
                    logging.error(f"Error parsing earliest date '{block_earliest}': {e}")

            if block_latest is not None:
                try:
                    latest_epoch_list = parse_dates_to_epoch([block_latest])
                    block_latest_epoch = latest_epoch_list[0]
                    latest_condition = ds.field('_epoch') <= block_latest_epoch
                    if parsed_filter is not None:
                        parsed_filter = parsed_filter & latest_condition
                    else:
                        parsed_filter = latest_condition
                except Exception as e:
                    logging.error(f"Error parsing latest date '{block_latest}': {e}")

            self.pyarrow_filters.append({
                'indexes': indexes,
                'filter': parsed_filter
            })

    def convert_to_pyarrow(self, parsed_expr, schema_fields):
        if isinstance(parsed_expr, pp.ParseResults):
            parsed_expr = parsed_expr.asList()

        if isinstance(parsed_expr, list):
            if len(parsed_expr) == 1:
                return self.convert_to_pyarrow(parsed_expr[0], schema_fields)
            elif len(parsed_expr) == 2:
                # Unary operator (NOT)
                operator, operand = parsed_expr
                operand_filter = self.convert_to_pyarrow(operand, schema_fields)
                if operator.upper() == "NOT":
                    return ~operand_filter
                else:
                    raise ValueError(f"Unknown unary operator: {operator}")
            elif len(parsed_expr) == 3:
                left, operator, right = parsed_expr
                if operator.upper() in ["AND", "OR"]:
                    left_filter = self.convert_to_pyarrow(left, schema_fields)
                    right_filter = self.convert_to_pyarrow(right, schema_fields)
                    if operator.upper() == "AND":
                        return left_filter & right_filter
                    elif operator.upper() == "OR":
                        return left_filter | right_filter
                else:
                    # Handle comparison or IN expressions
                    return self.handle_expression(parsed_expr, schema_fields)
            else:
                # Handle comparison or IN expressions
                return self.handle_expression(parsed_expr, schema_fields)
        else:
            # Handle atomic expressions
            return pa.scalar(True)

    def handle_expression(self, expr, schema_fields):
        if isinstance(expr, list) and len(expr) >= 2:
            if expr[0] == 'NOT':
                operand_filter = self.convert_to_pyarrow(expr[1], schema_fields)
                return ~operand_filter
            elif expr[1] in ['==', '!=', '>=', '<=', '>', '<']:
                field = expr[0]
                op = expr[1]
                value = expr[2]
                if field not in schema_fields:
                    logging.warning(f"Field '{field}' does not exist in the dataset schema. Skipping condition.")
                    return pa.scalar(True)
                field_ref = ds.field(field)
                # Convert value to appropriate type
                value = self.convert_value(value)
                # Build expression
                if op == '==':
                    return field_ref == value
                elif op == '!=':
                    return field_ref != value
                elif op == '>':
                    return field_ref > value
                elif op == '<':
                    return field_ref < value
                elif op == '>=':
                    return field_ref >= value
                elif op == '<=':
                    return field_ref <= value
            elif expr[1].upper() == 'IN':
                field = expr[0]
                values = expr[2]
                if field not in schema_fields:
                    logging.warning(f"Field '{field}' does not exist in the dataset schema. Skipping condition.")
                    return pa.scalar(True)
                field_ref = ds.field(field)
                # Convert values to appropriate types
                converted_values = [self.convert_value(v) for v in values]
                return field_ref.isin(converted_values)
            else:
                raise ValueError(f"Unsupported operator in expression: {expr}")
        else:
            raise ValueError(f"Unsupported expression format: {expr}")

    def convert_value(self, value):
        if isinstance(value, str):
            if value.lower() == 'true':
                return True
            elif value.lower() == 'false':
                return False
            else:
                # Try to convert to integer or float
                try:
                    if '.' in value:
                        return float(value)
                    else:
                        return int(value)
                except ValueError:
                    return value  # Keep as string
        else:
            return value

    def get_filtered_data(self):
        if self.short_circuit:
            self.short_circuit = False
            return pd.DataFrame()

        if not self.pyarrow_filters:
            self.build_pyarrow_filters()

        if not self.pyarrow_filters:
            logging.error("No valid filters to apply.")
            return pd.DataFrame()

        dataframes = []

        for filter_block in self.pyarrow_filters:
            indexes = filter_block['indexes']
            pyarrow_filter = filter_block['filter']

            if not indexes:
                logging.warning("No indexes specified for a filter block.")
                continue

            # Create a dataset from the list of Parquet files
            dataset = ds.dataset(indexes, format="parquet")

            # Read the data with the filter applied
            try:
                if pyarrow_filter is not None:
                    table = dataset.to_table(filter=pyarrow_filter)
                else:
                    table = dataset.to_table()
            except Exception as e:
                logging.error(f"Error applying filter: {e}")
                continue  # Skip this filter block

            # Convert to pandas DataFrame
            df = table.to_pandas()
            dataframes.append(df)

        if not dataframes:
            logging.warning("No dataframes were loaded from the filter blocks.")
            return pd.DataFrame()

        # Concatenate all dataframes into a single dataframe
        final_df = pd.concat(dataframes, ignore_index=True)

        return final_df

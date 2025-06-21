import os
import glob
import re
from datetime import datetime
import pandas as pd

# Define token types
TOKEN_TYPES = {
    'LPAREN': '(',
    'RPAREN': ')',
    'OPERATOR': {'AND', 'OR', 'IN', '=', '!=', '<', '>', '<=', '>=', 'EXISTS'},
    'KEYWORD': {'index', 'earliest', 'latest'},
    'LOGICAL_OPERATOR': {'AND', 'OR'},
    'COMPARISON_OPERATOR': {'=', '!=', '<', '>', '<=', '>=', 'EXISTS'},
    'FUNCTION': {'IN'},
}

class ASTNode:
    pass

class OperatorNode(ASTNode):
    def __init__(self, operator, left, right=None):
        self.operator = operator
        self.left = left
        self.right = right

class OperandNode(ASTNode):
    def __init__(self, value):
        self.value = value

class FunctionNode(ASTNode):
    def __init__(self, function, arguments):
        self.function = function
        self.arguments = arguments

class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.position = 0

    def peek(self):
        if self.position < len(self.tokens):
            return self.tokens[self.position]
        return None

    def consume(self):
        token = self.peek()
        self.position += 1
        return token

    def parse(self):
        return self.parse_expression()

    def parse_expression(self, min_precedence=1):
        left = self.parse_term()

        while True:
            token = self.peek()
            # Handle closing parentheses and end of tokens
            if token == ')' or token is None:
                break
            if token in TOKEN_TYPES['LOGICAL_OPERATOR']:
                precedence = self.get_operator_precedence(token)
                if precedence < min_precedence:
                    break
                operator = self.consume()
            else:
                # Implicit 'AND' if no operator is specified
                precedence = self.get_operator_precedence('AND')
                if precedence < min_precedence:
                    break
                operator = 'AND'

            right = self.parse_expression(precedence + 1)
            left = OperatorNode(operator, left, right)

        return left

    def get_operator_precedence(self, operator):
        precedences = {'OR': 1, 'AND': 2}
        return precedences.get(operator, 0)

    def parse_term(self):
        token = self.peek()
        if token == '(':
            self.consume()  # consume '('
            node = self.parse_expression()
            if self.consume() != ')':
                raise SyntaxError("Missing closing parenthesis")
            return node
        elif token in TOKEN_TYPES['KEYWORD'] or re.match(r'\w+', token):
            return self.parse_comparison_or_operand()
        else:
            raise SyntaxError(f"Unexpected token: {token}")

    def parse_comparison_or_operand(self):
        left = self.consume()
        token = self.peek()
        if token in TOKEN_TYPES['COMPARISON_OPERATOR']:
            operator = self.consume()
            right = self.consume()
            return OperatorNode(operator, OperandNode(left), OperandNode(right))
        elif token == 'IN':
            self.consume()  # consume 'IN'
            if self.consume() != '(':
                raise SyntaxError("Expected '(' after IN")
            args = []
            while True:
                arg = self.consume()
                if arg == ')':
                    break
                args.append(arg.strip(','))
            return FunctionNode('IN', [left] + args)
        else:
            # It's an operand without a comparison operator
            return OperandNode(left)

def resolve_index_paths(index_paths):
    files = []
    for path in index_paths:
        # Prepend 'indexes/' to the path
        full_path = os.path.abspath(os.path.join('indexes', path.strip('"')))
        # Use glob to handle wildcards
        matched_files = glob.glob(full_path)
        files.extend(matched_files)
    return files

def parse_time_constraints(constraints):
    earliest = None
    latest = None
    if 'earliest' in constraints:
        earliest = datetime.strptime(constraints['earliest'], '%Y-%m-%d')
    if 'latest' in constraints:
        latest = datetime.strptime(constraints['latest'], '%Y-%m-%d')
    return earliest, latest

def build_filter_function(ast):
    def filter_row(row):
        return evaluate_ast(ast, row)
    return filter_row

def evaluate_ast(node, row):
    if isinstance(node, OperandNode):
        value = node.value.strip('"')
        if value in row:
            result = row[value]
            # Return True if result is not null or NaN
            return pd.notnull(result)
        else:
            # Column not in row, return False
            return False
    elif isinstance(node, OperatorNode):
        if node.operator == 'EXISTS':
            field = node.left.value.strip('"')
            return field in row and pd.notnull(row[field])
        elif node.operator in TOKEN_TYPES['LOGICAL_OPERATOR']:
            left = evaluate_ast(node.left, row)
            right = evaluate_ast(node.right, row)
            if node.operator == 'AND':
                return bool(left) and bool(right)
            elif node.operator == 'OR':
                return bool(left) or bool(right)
        elif node.operator in TOKEN_TYPES['COMPARISON_OPERATOR']:
            left = evaluate_ast(node.left, row)
            right = evaluate_ast(node.right, row)
            # Ensure both left and right are of comparable types
            try:
                left = float(left)
            except (ValueError, TypeError):
                pass
            try:
                right = float(right)
            except (ValueError, TypeError):
                pass
            # Perform comparison
            if node.operator == '=':
                return left == right
            elif node.operator == '!=':
                return left != right
            elif node.operator == '<':
                return left < right
            elif node.operator == '>':
                return left > right
            elif node.operator == '<=':
                return left <= right
            elif node.operator == '>=':
                return left >= right
    elif isinstance(node, FunctionNode):
        if node.function == 'IN':
            field = node.arguments[0]
            values = [arg.strip('"') for arg in node.arguments[1:]]
            field_value = row.get(field)
            return field_value in values
    return False

def load_and_filter_data(files, filter_func, earliest, latest):
    data_frames = []
    for file in files:
        df = pd.read_parquet(file)
        # Convert columns to appropriate types
        if '_epoch' in df.columns:
            df['_epoch'] = pd.to_datetime(df['_epoch'])
            if earliest:
                df = df[df['_epoch'] >= earliest]
            if latest:
                df = df[df['_epoch'] <= latest]
        # Convert relevant columns to numeric if they exist
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except Exception:
                    pass
        if filter_func:
            df_before_filter = df.copy()
            df = df[df.apply(filter_func, axis=1)]
            # Uncomment the next line for debugging
            # print(f"Filtered out {len(df_before_filter) - len(df)} rows")
        data_frames.append(df)
    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    else:
        return pd.DataFrame()

def combine_data_frames(operator_tree):
    def recursive_combine(node):
        if isinstance(node, tuple):
            left = recursive_combine(node[0])
            op = node[1]
            right = recursive_combine(node[2])
            if op == 'OR':
                return pd.concat([left, right], ignore_index=True).drop_duplicates()
            elif op == 'AND':
                # Attempt to merge on common columns
                common_columns = set(left.columns).intersection(set(right.columns))
                if common_columns:
                    return pd.merge(left, right, how='inner', on=list(common_columns))
                else:
                    # Perform a cross join (Cartesian product)
                    left['_tmpkey'] = 1
                    right['_tmpkey'] = 1
                    merged_df = pd.merge(left, right, on='_tmpkey').drop('_tmpkey', axis=1)
                    return merged_df
        else:
            return node  # It's a DataFrame

    return recursive_combine(operator_tree)

def validate_tokens(tokens):
    # Basic validation for matching parentheses
    stack = []
    for token in tokens:
        if token == '(':
            stack.append(token)
        elif token == ')':
            if not stack or stack.pop() != '(':
                raise SyntaxError("Unmatched parentheses")
    if stack:
        raise SyntaxError("Unmatched parentheses")
    # Additional validations can be added here

def process_query(tokens):
    validate_tokens(tokens)
    parser = Parser(tokens)
    ast = parser.parse()

    # Extract index calls and operator tree
    operator_tree = extract_index_calls(ast)

    # Process index calls and build data frames
    def process_node(node):
        if isinstance(node, tuple):
            left = process_node(node[0])
            op = node[1]
            right = process_node(node[2])
            if op == 'OR':
                return pd.concat([left, right], ignore_index=True).drop_duplicates()
            elif op == 'AND':
                # Attempt to merge DataFrames on common columns
                common_columns = set(left.columns).intersection(set(right.columns))
                if common_columns:
                    return pd.merge(left, right, how='inner', on=list(common_columns))
                else:
                    # Perform a cross join (Cartesian product)
                    left['_tmpkey'] = 1
                    right['_tmpkey'] = 1
                    merged_df = pd.merge(left, right, on='_tmpkey').drop('_tmpkey', axis=1)
                    return merged_df
        else:
            index_paths = node.get('index_paths', [])
            constraints = node.get('constraints', {})
            earliest, latest = parse_time_constraints(constraints)
            filter_ast = node.get('filter_ast')
            filter_func = build_filter_function(filter_ast) if filter_ast else None

            files = resolve_index_paths(index_paths)
            if files:
                readable_paths = '\n\t'.join(files)
                print(f'[i] Files detected:\n{readable_paths}')
            else:
                print('[!] No files matched the index path pattern.')
            df = load_and_filter_data(files, filter_func, earliest, latest)
            return df

    result_df = process_node(operator_tree)
    return result_df

def extract_index_calls(ast):
    def traverse(node):
        if isinstance(node, OperatorNode) and node.operator in TOKEN_TYPES['LOGICAL_OPERATOR']:
            left = traverse(node.left)
            right = traverse(node.right)
            return (left, node.operator, right)
        else:
            index_call = {'index_paths': [], 'constraints': {}, 'filter_ast': None}
            collect_details(node, index_call)
            return index_call

    def collect_details(node, index_call):
        if isinstance(node, OperatorNode):
            if node.operator in TOKEN_TYPES['COMPARISON_OPERATOR']:
                field = node.left.value.strip('"')
                value = node.right.value.strip('"')
                if field == 'index':
                    index_call['index_paths'].append(value)
                elif field in {'earliest', 'latest'}:
                    index_call['constraints'][field] = value
                else:
                    # Combine multiple filters using 'AND'
                    existing_filter = index_call.get('filter_ast')
                    if existing_filter:
                        index_call['filter_ast'] = OperatorNode('AND', existing_filter, node)
                    else:
                        index_call['filter_ast'] = node
            elif node.operator in TOKEN_TYPES['LOGICAL_OPERATOR']:
                # Logical operators within filters
                left_filter = {'index_paths': [], 'constraints': {}, 'filter_ast': None}
                collect_details(node.left, left_filter)
                right_filter = {'index_paths': [], 'constraints': {}, 'filter_ast': None}
                collect_details(node.right, right_filter)
                # Combine filters
                if left_filter.get('filter_ast') and right_filter.get('filter_ast'):
                    combined_filter_ast = OperatorNode(node.operator, left_filter['filter_ast'], right_filter['filter_ast'])
                elif left_filter.get('filter_ast'):
                    combined_filter_ast = left_filter['filter_ast']
                elif right_filter.get('filter_ast'):
                    combined_filter_ast = right_filter['filter_ast']
                else:
                    combined_filter_ast = None
                index_call['filter_ast'] = combined_filter_ast
                # Merge index paths and constraints
                index_call['index_paths'].extend(left_filter.get('index_paths', []))
                index_call['index_paths'].extend(right_filter.get('index_paths', []))
                index_call['constraints'].update(left_filter.get('constraints', {}))
                index_call['constraints'].update(right_filter.get('constraints', {}))
        elif isinstance(node, OperandNode):
            value = node.value.strip('"')
            # Create a condition that the field exists and is not null
            condition_node = OperatorNode('EXISTS', OperandNode(value))
            existing_filter = index_call.get('filter_ast')
            if existing_filter:
                index_call['filter_ast'] = OperatorNode('AND', existing_filter, condition_node)
            else:
                index_call['filter_ast'] = condition_node
        elif isinstance(node, FunctionNode):
            index_call['filter_ast'] = node

    operator_tree = traverse(ast)
    return operator_tree

def main():
    tokens = [
        'index', '=', '"error_tracking/*"', 'errorCode', '>', '400'
    ]
    result_df = process_query(tokens)
    print(result_df)

if __name__ == "__main__":
    main()

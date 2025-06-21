#!/usr/bin/env python3
import ast

import pandas as pd
import logging
import antlr4

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')  # Set to DEBUG for detailed output


class DataFrameQueryProcessor:
    def __init__(self, df, expression):
        self.main_df = df
        self.final_df = self.query_dataframe(expression)

    @staticmethod
    def try_ast(_obj):
        try:
            return ast.literal_eval(_obj)
        except Exception as e:
            try:
                return float(_obj)
            except Exception as e:
                try:
                    return int(_obj)
                except Exception as e:
                    try:
                        return str(_obj)
                    except Exception as e:
                        return _obj

    @staticmethod
    def _convert_antlr_object(obj):
        """Converts an antlr4 object to a string or numeric value."""
        # Convert antlr4 objects correctly handling their textual representation
        if hasattr(obj, 'getText'):
            value = obj.getText()
        else:
            value = str(obj)

        # Attempt to convert numeric values
        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return value

    def _apply_single_condition(self, _df, condition):
        """Applies a single condition to the dataframe and returns the filtered dataframe."""
        header, operation, value = map(self._convert_antlr_object, condition)
        if operation == '==':
            return _df[_df[header] == value]
        elif operation == '!=':
            return _df[_df[header] != value]
        elif operation == '>=':
            return _df[_df[header] >= value]
        elif operation == '>':
            return _df[_df[header] > value]
        elif operation == '<=':
            return _df[_df[header] <= value]
        elif operation == '<':
            return _df[_df[header] < value]
        elif operation == '~=':
            # Assuming '~=' means 'contains' for strings
            return _df[_df[header].str.contains(value, na=False, case=False)]
        else:
            logging.error("[x] Unsupported operation: %s", operation)
            return pd.DataFrame()

    def _evaluate_expression(self, _df, _expression):
        """Recursively evaluates the expression against the dataframe."""
        if isinstance(_expression, antlr4.tree.Tree.TerminalNodeImpl) or not isinstance(_expression, list):
            _expression = [self._convert_antlr_object(_expression)]  # Convert antlr object directly

        if len(_expression) == 3 and isinstance(_expression[0], (antlr4.tree.Tree.TerminalNodeImpl, str)):
            # Base case: directly apply the condition
            return self._apply_single_condition(_df, _expression)
        else:
            # Recursive case: complex expression
            result = pd.DataFrame()
            operation = None
            for elem in _expression:
                if isinstance(elem, (list, antlr4.tree.Tree.TerminalNodeImpl)):
                    temp_result = self._evaluate_expression(_df, elem)
                    if operation == 'AND':
                        result = result.merge(temp_result, how='inner') if not result.empty else temp_result
                    elif operation == 'OR':
                        result = pd.concat([result, temp_result]).drop_duplicates().reset_index(drop=True)
                    else:
                        result = temp_result
                elif isinstance(elem, str) and elem in ['AND', 'OR']:
                    operation = self.try_ast(elem)
                else:
                    logging.error("[x] Unexpected element in expression: %s", elem)
            return result if not result.empty else _df

    def query_dataframe(self, _expression):
        """Public method to query the dataframe using the complex logical expression."""
        return self._evaluate_expression(self.main_df, _expression)

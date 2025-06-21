#!/usr/bin/env python3

import logging
import importlib
import antlr4.tree.Tree
import numpy
import ast
import os
import re
from collections import OrderedDict

from tradeQueryParser import tradeQueryParser
from tradeQueryParser import tradeQueryParser as TQP
from tradeQueryListener import tradeQueryListener
from ParaquetHandler import ParquetHandler
from StringHandler import StringHandler
from TimeHandler import TimeHandler
from MathematicOperations import MathHandler
from GeneralHandler import GeneralHandler

logging.basicConfig(level=logging.INFO, format='%(message)s')


class CustomTradeQueryListener(tradeQueryListener):
    def __init__(self, original_query):
        self.original_query = original_query.strip('\n').strip(' ').strip('\n').strip(' ')
        self.script_directory = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.target_index_uri = 'indexes/test_parquets'
        self.current_directive = 'table'
        self.parquet_handler = ParquetHandler()
        self.string_handler = StringHandler()
        self.math_handler = MathHandler()
        self.time_handler = TimeHandler()
        self.general_handler = GeneralHandler()
        self.comments = OrderedDict()
        self.main_df = None
        self.table_cmd = None
        self.current_value = None
        self.contextStack = []
        self.expression_string = None
        self.resolved_lines = []
        self.first_directive = True
        self.type_check = importlib.import_module('tradeQueryParser')
        self.directive = None
        self.current_item = None
        self.entries = []
        self.values = OrderedDict()
        self.concat_children = None
        self.ctx_obj_str = None
        self.last_expression = None
        self.last_entry = ''
        self.original_query = original_query
        self.known_directives = [
            'file', 'eval', 'stats', 'where', 'rename', 'fields', 'search', 'reverse', 'head', 'rex', 'regex', 'dedup']
        self.all_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'concat',
            'replace', 'upper', 'lower', 'capitalize', 'trim', 'ltrim', 'rtrim', 'substr', 'len', 'tostring', 'match',
            'urlencode', 'defang', 'type']
        self.all_numeric_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'len']
        self.all_string_funcs = [
            "concat", "replace", "upper", "lower", "capitalize", "trim", "rtrim", "ltrim", "substr", "len",
            "tostring", "match", "urlencode", "urldecode", "defang", "type"]

    # Enter a parse tree produced by tradeQueryParser#program.
    def enterProgram(self, ctx: tradeQueryParser.ProgramContext):
        self.validate_exceptions(ctx, 'enterProgram')

    # Exit a parse tree produced by tradeQueryParser#program.
    def exitProgram(self, ctx: tradeQueryParser.ProgramContext):
        self.validate_exceptions(ctx, 'exitProgram')

    # Enter a parse tree produced by tradeQueryParser#tradeQuery.
    def enterTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.validate_exceptions(ctx, 'enterTradeQuery')

    # Exit a parse tree produced by tradeQueryParser#tradeQuery.
    def exitTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.validate_exceptions(ctx, 'exitTradeQuery')

    # Enter a parse tree produced by tradeQueryParser#validLine.
    def enterValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'enterValidLine')

    # Exit a parse tree produced by tradeQueryParser#validLine.
    def exitValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'exitValidLine')

    # Enter a parse tree produced by tradeQueryParser#directive.
    def enterDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'enterDirective')

    # Exit a parse tree produced by tradeQueryParser#directive.
    def exitDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'exitDirective')

    # Enter a parse tree produced by tradeQueryParser#expression.
    def enterExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'enterExpression')

    # Exit a parse tree produced by tradeQueryParser#expression.
    def exitExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'exitExpression')

    # Enter a parse tree produced by tradeQueryParser#expressionString.
    def enterExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'enterExpressionString')

    # Exit a parse tree produced by tradeQueryParser#expressionString.
    def exitExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'exitExpressionString')

    # Enter a parse tree produced by tradeQueryParser#logicalStringExpr.
    def enterLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'enterLogicalStringExpr')

    # Exit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def exitLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'exitLogicalStringExpr')

    # Enter a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def enterArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticStringExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def exitArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticStringExpr')

    # Enter a parse tree produced by tradeQueryParser#factorStringExpr.
    def enterFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'enterFactorStringExpr')

    # Exit a parse tree produced by tradeQueryParser#factorStringExpr.
    def exitFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'exitFactorStringExpr')

    # Enter a parse tree produced by tradeQueryParser#expressionNumeric.
    def enterExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'enterExpressionNumeric')

    # Exit a parse tree produced by tradeQueryParser#expressionNumeric.
    def exitExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'exitExpressionNumeric')

    # Enter a parse tree produced by tradeQueryParser#compareNumericExpr.
    def enterCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'enterCompareNumericExpr')

    # Exit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def exitCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'exitCompareNumericExpr')

    # Enter a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def enterArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def exitArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticNumericResultOnlyExpr')

    # Enter a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def enterTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterTermNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def exitTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitTermNumericResultOnlyExpr')

    # Enter a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def enterFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterFactorNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def exitFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitFactorNumericResultOnlyExpr')

    # Enter a parse tree produced by tradeQueryParser#expressionAny.
    def enterExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'enterExpressionAny')

    # Exit a parse tree produced by tradeQueryParser#expressionAny.
    def exitExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'exitExpressionAny')

    # Enter a parse tree produced by tradeQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'enterLogicalExpr')

    # Exit a parse tree produced by tradeQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'exitLogicalExpr')

    # Enter a parse tree produced by tradeQueryParser#compareExpr.
    def enterCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'enterCompareExpr')

    # Exit a parse tree produced by tradeQueryParser#compareExpr.
    def exitCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'exitCompareExpr')

    # Enter a parse tree produced by tradeQueryParser#arithmeticExpr.
    def enterArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def exitArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticExpr')

    # Enter a parse tree produced by tradeQueryParser#termExpr.
    def enterTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'enterTermExpr')

    # Exit a parse tree produced by tradeQueryParser#termExpr.
    def exitTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'exitTermExpr')

    # Enter a parse tree produced by tradeQueryParser#factorExpr.
    def enterFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'enterFactorExpr')

    # Exit a parse tree produced by tradeQueryParser#factorExpr.
    def exitFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'exitFactorExpr')

    # Enter a parse tree produced by tradeQueryParser#value.
    def enterValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'enterValue')

    # Exit a parse tree produced by tradeQueryParser#value.
    def exitValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'exitValue')

    # Enter a parse tree produced by tradeQueryParser#valueStringOnly.
    def enterValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'enterValueStringOnly')

    # Exit a parse tree produced by tradeQueryParser#valueStringOnly.
    def exitValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'exitValueStringOnly')

    # Enter a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def enterValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'enterValueNumericResultOnly')

    # Exit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def exitValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'exitValueNumericResultOnly')

    # Enter a parse tree produced by tradeQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'enterUnaryExpr')

    # Exit a parse tree produced by tradeQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'exitUnaryExpr')

    # Enter a parse tree produced by tradeQueryParser#numericValue.
    def enterNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'enterNumericValue')

    # Exit a parse tree produced by tradeQueryParser#numericValue.
    def exitNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'exitNumericValue')

    # Enter a parse tree produced by tradeQueryParser#booleanValue.
    def enterBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'enterBooleanValue')

    # Exit a parse tree produced by tradeQueryParser#booleanValue.
    def exitBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'exitBooleanValue')

    # Enter a parse tree produced by tradeQueryParser#stringValue.
    def enterStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'enterStringValue')

    # Exit a parse tree produced by tradeQueryParser#stringValue.
    def exitStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'exitStringValue')

    # Enter a parse tree produced by tradeQueryParser#arrayValue.
    def enterArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'enterArrayValue')

    # Exit a parse tree produced by tradeQueryParser#arrayValue.
    def exitArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'exitArrayValue')

    # Enter a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def enterArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'enterArrayValueNumeric')

    # Exit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def exitArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'exitArrayValueNumeric')

    # Enter a parse tree produced by tradeQueryParser#variableValue.
    def enterVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'enterVariableValue')

    # Exit a parse tree produced by tradeQueryParser#variableValue.
    def exitVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'exitVariableValue')

    # Enter a parse tree produced by tradeQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'enterComparisonOperator')

    # Exit a parse tree produced by tradeQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'exitComparisonOperator')

    # Enter a parse tree produced by tradeQueryParser#logicalOperator.
    def enterLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'enterLogicalOperator')

    # Exit a parse tree produced by tradeQueryParser#logicalOperator.
    def exitLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'exitLogicalOperator')

    # Enter a parse tree produced by tradeQueryParser#functionCall.
    def enterFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'enterFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#functionCall.
    def exitFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'exitFunctionCall')

    # Enter a parse tree produced by tradeQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'enterNumericFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'exitNumericFunctionCall')

    # Enter a parse tree produced by tradeQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'enterStringFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'exitStringFunctionCall')

    # Enter a parse tree produced by tradeQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'enterSpecificFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'exitSpecificFunctionCall')

    # *******************************************************
    # Custom Functions
    # *******************************************************
    @staticmethod
    def concatenate_children(_children_list):
        return f"{' '.join([str(x) for x in _children_list])}"

    def validate_exceptions(self, ctx_obj, obj_identifier):
        if ctx_obj.exception:
            logging.error(f'[!] CUSTOM {obj_identifier} Error: {ctx_obj.exception}')
        if not ctx_obj.children:
            logging.error(f'[!] CUSTOM {obj_identifier}: NO CHILDREN!')
        if len(ctx_obj.children) <= 0:
            logging.error(f'[!] CUSTOM {obj_identifier}: CHILDREN ENTRIES BLANK!')

        self.ctx_obj_str = str(ctx_obj)
        self.bubble_up_resolution(ctx_obj)

        # Tracing stack operations and resolutions as they happen via custom stack.
        if obj_identifier.startswith('enter'):
            self.contextStack.append((obj_identifier.lstrip('enter'), ctx_obj))
        elif obj_identifier.startswith('exit'):
            pass

        else:
            logging.error(f'[x] Exiting due to object identifier being neither enter nor exit.')
            exit(1)

        # Initial Start Point Everytime
        if isinstance(ctx_obj, TQP.TradeQueryContext):
            self.last_entry = ctx_obj.children

        # New Line Event
        elif isinstance(ctx_obj, TQP.DirectiveContext):
            if obj_identifier.startswith('enter'):
                if self.first_directive:
                    self.first_directive = False
                    x = self.collapse(self.values)
                    self.table_cmd = [x for x in self.last_entry if str(x) != '\n']
                    self.entries.append(self.table_cmd)
            elif obj_identifier.startswith('exit'):
                collapsed_values = self.collapse(self.values)
                current_directive = [_tuple[-1] for _tuple in collapsed_values
                                     if isinstance(_tuple[0], TQP.DirectiveContext)]
                if len(current_directive) == 1:
                    self.entries.append([x for x in current_directive[0] if str(x) != '\n'])
                else:
                    self.entries.append([x for x in current_directive.pop(0) if str(x) != '\n'])
            else:
                pass

        # Terminal Resolutions
        elif isinstance(ctx_obj, TQP.StringValueContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.VariableValueContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.NumericValueContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        # Function Call Lineup
        elif isinstance(ctx_obj, TQP.SpecificFunctionCallContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.NumericFunctionCallContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.StringFunctionCallContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        # Expression Lineup
        elif isinstance(ctx_obj, TQP.ExpressionStringContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.ExpressionNumericContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

        elif isinstance(ctx_obj, TQP.ExpressionAnyContext):
            if obj_identifier.startswith('enter'):
                pass
            elif obj_identifier.startswith('exit'):
                pass

    def init_table_query(self, table_line):
        tables = self.general_handler.extract_db_names(table_line)
        if tables:
            tables = [f'{self.script_directory}/../{self.target_index_uri}/{x}.parquet' for x in tables]
            for table in tables:
                self.main_df = self.parquet_handler.read_parquet_file(table)
            print(self.main_df)

    @staticmethod
    def update_last_entry(object_list, target, replacement):
        """
        In-place replacement of an object in a list with another object if it exists.

        Parameters:
        objects (list): The list of objects to modify.
        target: The object to find in the list.
        replacement: The object to replace the target with.
        """
        for i, obj in enumerate(object_list):
            if obj == target:
                object_list[i] = replacement

    def getCurrentContext(self):
        return self.contextStack[-1][-1] if self.contextStack else None

    @staticmethod
    def resolve_terminal_nodes(antlr_objs):
        """
        Accepts a list of ANTLR4 objects and resolves each object of type
        antlr4.tree.Tree.TerminalNodeImpl to its ast.literal_eval value.

        Parameters:
        antlr_objs (list): The list of ANTLR4 objects to resolve.

        Returns:
        list: A list of resolved values.
        """
        resolved_values = []
        for obj in antlr_objs:
            # Check if the object is an instance of TerminalNodeImpl
            if isinstance(obj, antlr4.tree.Tree.TerminalNodeImpl):
                try:
                    # Attempt to resolve the terminal node's text content
                    value = ast.literal_eval(obj.getText())
                    resolved_values.append(value)
                except (ValueError, SyntaxError) as e:
                    logging.info(f"[i] SKIPPING: unable to resolve TerminalNodeImpl with value {obj.getText()}: {e}")
                    try:
                        resolved_values.append(float(obj.getText()))
                    except Exception as e:
                        logging.info(f"[i] SKIPPING: Not a float value either {obj.getText()}: {e}")
                        resolved_values.append(str(obj.getText()))
            else:
                logging.debug(f"[DEBUG] Object {obj} is not of type TerminalNodeImpl and was skipped.")
        return resolved_values

    def bubble_up_resolution(self, ctx_obj):
        """
        Performs a bubble-up resolution on the given stack of contexts.
        Each context is resolved, and the result is used to update the parent context.

        Parameters:
        context_stack (list): The stack of contexts to resolve.

        Returns:
        list: A list of contexts with resolved values.
        """
        for i in range(len(self.contextStack) - 1, -1, -1):  # Start from the last item to the first
            ctx, antlr_objs = self.contextStack[i]
            resolved_values = self.resolve_terminal_nodes(antlr_objs.children)
            last_stack_object = self.contextStack.pop()[-1]
            self.values[last_stack_object] = self.flatten_outer_lists(last_stack_object.children)

            if self.contextStack:
                for _index, entry in enumerate(self.contextStack[-1][-1].children):
                    if entry == antlr_objs:
                        self.contextStack[-1][-1].children[_index] = resolved_values

            if len(self.contextStack) >= 2:
                for index in reversed(range(len(self.contextStack))):
                    key, val = self.contextStack[-1][-1], self.contextStack[-1][-1].children
                    for _index, child in enumerate(self.contextStack[index][-1].children):
                        if child == key:
                            self.contextStack[index][-1].children[_index] = val

    @staticmethod
    def flatten_outer_lists(nested_list):
        """
        Recursively flattens a list by removing nested outer lists if they are unnecessary.

        Parameters:
        nested_list (list): The nested list to flatten.

        Returns:
        The flattened list or the original element.
        """
        try:
            while isinstance(nested_list, list) and len(nested_list) == 1:
                nested_list = nested_list[0]
            return nested_list
        except TypeError as e:
            logging.error(f"[x] TypeError encountered in flatten_outer_lists: {e}")
            return nested_list
        except Exception as e:
            logging.error(f"[x] Unexpected error encountered in flatten_outer_lists: {e}")
            return nested_list

    @staticmethod
    def collapse(ordered_dict):
        def collapse_recursive(_last_key, _last_val, _od, _collapsed_output):
            for key, val in _od.items():
                if _last_key == val:
                    _last_key = _od.popitem(last=False)[0]
                    if len(_od) > 0:
                        collapse_recursive(_last_key, _last_val, _od, _collapsed_output)
                if isinstance(val, (antlr4.tree.Tree.TerminalNodeImpl, str, int, float, list)):
                    _collapsed_output.append((_last_key, _last_val))
                    if isinstance(val, list):
                        for _index, _val in enumerate(val):
                            for _tuple in _collapsed_output:
                                try:
                                    _key, _tuple_val = _tuple
                                    if _key == _val:
                                        val[_index] = _tuple_val
                                except Exception as e:
                                    logging.info(f'[i] Shit {e}')

                    _last_key, _last_val = _od.popitem(last=False)
                    if len(_od) > 0:
                        collapse_recursive(_last_key, _last_val, _od, _collapsed_output)
                break

        reverse_od = OrderedDict(reversed(list(ordered_dict.items())))
        last_key, last_val = reverse_od.popitem(last=False)
        collapsed_output = []
        collapse_recursive(last_key, last_val, reverse_od, collapsed_output)

        return collapsed_output


del tradeQueryParser

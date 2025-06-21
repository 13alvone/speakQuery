#!/usr/bin/env python3

import antlr4
import logging
from collections import OrderedDict
import ast

from antlr4 import *

if "." in __name__:
    from .whereClauseParser import whereClauseParser
else:
    from whereClauseParser import whereClauseParser

from handlers.archive.StackHandler import StackHandler
from handlers.GeneralHandler import GeneralHandler


class whereClauseListener(ParseTreeListener):
    def __init__(self, where_clause):
        self.parsed_or_expressions = OrderedDict()
        self.parsed_and_expressions = OrderedDict()
        self.parsed_single_expressions = OrderedDict()
        self.original_where_clause = where_clause
        self.stack_handler = StackHandler()
        self.general_handler = GeneralHandler()
        self.values = OrderedDict()
        self.contextStack = []

    def enterWhereClause(self, ctx: whereClauseParser.WhereClauseContext):
        self.validate_exceptions(ctx, 'enterWhereClause')

    def exitWhereClause(self, ctx: whereClauseParser.WhereClauseContext):
        self.validate_exceptions(ctx, 'exitWhereClause')

    def enterComplexComparison(self, ctx: whereClauseParser.ComplexComparisonContext):
        self.validate_exceptions(ctx, 'enterComplexComparison')

    def exitComplexComparison(self, ctx: whereClauseParser.ComplexComparisonContext):
        self.validate_exceptions(ctx, 'exitComplexComparison')

    def enterSingleLogicalExpression(self, ctx: whereClauseParser.SingleLogicalExpressionContext):
        self.validate_exceptions(ctx, 'enterSingleLogicalExpression')

    def exitSingleLogicalExpression(self, ctx: whereClauseParser.SingleLogicalExpressionContext):
        self.validate_exceptions(ctx, 'exitSingleLogicalExpression')

    def enterSingleComparison(self, ctx: whereClauseParser.SingleComparisonContext):
        self.validate_exceptions(ctx, 'enterSingleComparison')

    def exitSingleComparison(self, ctx: whereClauseParser.SingleComparisonContext):
        self.validate_exceptions(ctx, 'exitSingleComparison')

    def enterHeaderName(self, ctx: whereClauseParser.HeaderNameContext):
        self.validate_exceptions(ctx, 'enterHeaderName')

    def exitHeaderName(self, ctx: whereClauseParser.HeaderNameContext):
        self.validate_exceptions(ctx, 'exitHeaderName')

    def enterComparisonSingleValue(self, ctx: whereClauseParser.ComparisonSingleValueContext):
        self.validate_exceptions(ctx, 'enterComparisonSingleValue')

    def exitComparisonSingleValue(self, ctx: whereClauseParser.ComparisonSingleValueContext):
        self.validate_exceptions(ctx, 'exitComparisonSingleValue')

    def enterComparisonMultiValue(self, ctx: whereClauseParser.ComparisonMultiValueContext):
        self.validate_exceptions(ctx, 'enterComparisonMultiValue')

    def exitComparisonMultiValue(self, ctx: whereClauseParser.ComparisonMultiValueContext):
        self.validate_exceptions(ctx, 'exitComparisonMultiValue')

    def enterMvString(self, ctx: whereClauseParser.MvStringContext):
        self.validate_exceptions(ctx, 'enterMvString')

    def exitMvString(self, ctx: whereClauseParser.MvStringContext):
        self.validate_exceptions(ctx, 'exitMvString')

    def enterMvNumber(self, ctx: whereClauseParser.MvNumberContext):
        self.validate_exceptions(ctx, 'enterMvNumber')

    def exitMvNumber(self, ctx: whereClauseParser.MvNumberContext):
        self.validate_exceptions(ctx, 'exitMvNumber')

    def enterStringValue(self, ctx: whereClauseParser.StringValueContext):
        self.validate_exceptions(ctx, 'enterStringValue')

    def exitStringValue(self, ctx: whereClauseParser.StringValueContext):
        self.validate_exceptions(ctx, 'exitStringValue')

    def enterNumericValue(self, ctx: whereClauseParser.NumericValueContext):
        self.validate_exceptions(ctx, 'enterNumericValue')

    def exitNumericValue(self, ctx: whereClauseParser.NumericValueContext):
        self.validate_exceptions(ctx, 'exitNumericValue')

    def enterComparisonOperand(self, ctx: whereClauseParser.ComparisonOperandContext):
        self.validate_exceptions(ctx, 'enterComparisonOperand')

    def exitComparisonOperand(self, ctx: whereClauseParser.ComparisonOperandContext):
        self.validate_exceptions(ctx, 'exitComparisonOperand')

    def enterInOperand(self, ctx: whereClauseParser.InOperandContext):
        self.validate_exceptions(ctx, 'enterInOperand')

    def exitInOperand(self, ctx: whereClauseParser.InOperandContext):
        self.validate_exceptions(ctx, 'exitInOperand')

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************

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

        self.bubble_up_resolution(ctx_obj)

        # Tracing stack operations and resolutions as they happen via custom stack.
        if obj_identifier.startswith('enter'):
            self.contextStack.append((obj_identifier.lstrip('enter'), ctx_obj))

        elif obj_identifier.startswith('exit'):

            last_key, last_val = next(reversed(self.values.items()))

            # Add to Known Clauses and determine if OR is included or not:
            if obj_identifier == 'exitSingleLogicalExpression':
                collapsed_expression_string = self.general_handler.join_values(last_val).lstrip('where ')
                if ' or ' in collapsed_expression_string or ' OR ' in collapsed_expression_string:
                    self.parsed_or_expressions[self.general_handler.join_values(last_val)] = 'OR'
                elif ' and ' in collapsed_expression_string or ' AND ' in collapsed_expression_string:
                    self.parsed_and_expressions[self.general_handler.join_values(last_val)] = 'AND'
                else:
                    self.parsed_single_expressions[self.general_handler.join_values(last_val)] = 'SINGLE_COMPARISON'

            # Collapse the stack found at self.values if terminal nodes only are found in last_val:
            if self.values:
                if len(self.values) > 1:
                    if isinstance(last_val, antlr4.tree.Tree.TerminalNodeImpl):
                        while isinstance(last_val, antlr4.tree.Tree.TerminalNodeImpl):
                            self.collapse(self, self.values)
                            self.values.popitem(last=True)
                            self.stack_handler.trim_stack_to_last_list(self.values)
                            last_key, last_val = next(reversed(self.values.items()))
                    elif isinstance(last_val, list):
                        if self.general_handler.are_all_terminal_instances(last_val):
                            self.collapse(self, self.values)
                            self.values.popitem(last=True)
                            self.stack_handler.trim_stack_to_last_list(self.values)

    def bubble_up_resolution(self, ctx_obj):
        """
        Performs a bubble-up resolution that manages self.values on each entry and exit procedure

        Parameters:
        context_stack (list): The stack of contexts to resolve.
        """
        for i in range(len(self.contextStack) - 1, -1, -1):  # Start from the last item to the first
            ctx, antlr_objs = self.contextStack[i]
            resolved_values = self.resolve_terminal_nodes(antlr_objs.children)
            last_stack_object = self.contextStack.pop()[-1]
            self.values[last_stack_object] = self.flatten_outer_lists(last_stack_object.children)

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
                    # logging.info(f"[i] SKIPPING: unable to resolve TerminalNodeImpl with value {obj.getText()}: {e}")
                    try:
                        resolved_values.append(float(obj.getText()))
                    except Exception as e:
                        # logging.info(f"[i] SKIPPING: Not a float value either {obj.getText()}: {e}")
                        resolved_values.append(str(obj.getText()))
            else:
                logging.debug(f"[DEBUG] Object {obj} is not of type TerminalNodeImpl and was skipped.")
        return resolved_values

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
    def collapse(_self, ordered_dict):
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


del whereClauseParser

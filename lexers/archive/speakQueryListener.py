#!/usr/bin/env python3
import logging
import inspect
import sys
import os

from antlr4.tree.Tree import TerminalNodeImpl, ParseTree
from antlr4 import *

from handlers.GeneralHandler import GeneralHandler

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser
project_root = os.path.abspath('.')

sys.path.insert(0, os.path.join(project_root, 'functionality', 'cpp_index_call', 'build'))
sys.path.insert(0, os.path.join(project_root, 'functionality', 'cpp_datetime_parser', 'build'))

try:
    from cpp_index_call import process_index_calls
except ImportError:
    print("Could not import cpp_index_call. Check build and placement of the .so file.")
    sys.exit(1)

try:
    from cpp_datetime_parser import parse_dates_to_epoch
except ImportError:
    print("Could not import cpp_datetime_parser. Check build and placement of the .so file.")


# This class defines a complete listener for a parse tree produced by speakQueryParser.
class speakQueryListener(ParseTreeListener):
    def __init__(self, cleaned_query):
        self.main_df = None
        self.root_ctx = None
        self.earliest_clause = None
        self.earliest_time = None
        self.latest_clause = None
        self.latest_time = None
        self.target_index = None
        self.initial_sequence_enabled = False
        self.original_query = cleaned_query.strip()
        self.original_index_call = ''.join(self.original_query.split('|')[0].strip()).replace(' ', '')
        self.general_handler = GeneralHandler

    # Enter a parse tree produced by speakQueryParser#speakQuery.
    def enterSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        pass

    # Exit a parse tree produced by speakQueryParser#speakQuery.
    def exitSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#initialSequence.
    def enterInitialSequence(self, ctx: speakQueryParser.InitialSequenceContext):
        self.initial_sequence_enabled = True

    # Exit a parse tree produced by speakQueryParser#initialSequence.
    def exitInitialSequence(self, ctx: speakQueryParser.InitialSequenceContext):
        self.initial_sequence_enabled = False
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#expression.
    def enterExpression(self, ctx: speakQueryParser.ExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#expression.
    def exitExpression(self, ctx: speakQueryParser.ExpressionContext):
        current_parsed_index_call = self.ctx_flatten(ctx)
        current_index_call = ''.join(current_parsed_index_call).replace(' ', '')
        if current_index_call == self.original_index_call:
            self.main_df = process_index_calls(current_parsed_index_call)

        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#conjunction.
    def enterConjunction(self, ctx: speakQueryParser.ConjunctionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#conjunction.
    def exitConjunction(self, ctx: speakQueryParser.ConjunctionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#comparison.
    def enterComparison(self, ctx: speakQueryParser.ComparisonContext):
        pass

    # Exit a parse tree produced by speakQueryParser#comparison.
    def exitComparison(self, ctx: speakQueryParser.ComparisonContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#additiveExpr.
    def enterAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#additiveExpr.
    def exitAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#multiplicativeExpr.
    def enterMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#multiplicativeExpr.
    def exitMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        pass

    # Exit a parse tree produced by speakQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#primary.
    def enterPrimary(self, ctx: speakQueryParser.PrimaryContext):
        pass

    # Exit a parse tree produced by speakQueryParser#primary.
    def exitPrimary(self, ctx: speakQueryParser.PrimaryContext):
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#timeClause.
    def enterTimeClause(self, ctx: speakQueryParser.TimeClauseContext):
        pass

    # Exit a parse tree produced by speakQueryParser#timeClause.
    def exitTimeClause(self, ctx: speakQueryParser.TimeClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#earliestClause.
    def enterEarliestClause(self, ctx: speakQueryParser.EarliestClauseContext):
        pass

    # Exit a parse tree produced by speakQueryParser#earliestClause.
    def exitEarliestClause(self, ctx: speakQueryParser.EarliestClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#latestClause.
    def enterLatestClause(self, ctx: speakQueryParser.LatestClauseContext):
        pass

    # Exit a parse tree produced by speakQueryParser#latestClause.
    def exitLatestClause(self, ctx: speakQueryParser.LatestClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#indexClause.
    def enterIndexClause(self, ctx: speakQueryParser.IndexClauseContext):
        pass

    # Exit a parse tree produced by speakQueryParser#indexClause.
    def exitIndexClause(self, ctx: speakQueryParser.IndexClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by speakQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#inExpression.
    def enterInExpression(self, ctx: speakQueryParser.InExpressionContext):
        pass

    # Exit a parse tree produced by speakQueryParser#inExpression.
    def exitInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#inputlookupInit.
    def enterInputlookupInit(self, ctx: speakQueryParser.InputlookupInitContext):
        pass

    # Exit a parse tree produced by speakQueryParser#inputlookupInit.
    def exitInputlookupInit(self, ctx: speakQueryParser.InputlookupInitContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#validLine.
    def enterValidLine(self, ctx: speakQueryParser.ValidLineContext):
        pass

    # Exit a parse tree produced by speakQueryParser#validLine.
    def exitValidLine(self, ctx: speakQueryParser.ValidLineContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#directive.
    def enterDirective(self, ctx: speakQueryParser.DirectiveContext):
        pass

    # Exit a parse tree produced by speakQueryParser#directive.
    def exitDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#macro.
    def enterMacro(self, ctx: speakQueryParser.MacroContext):
        pass

    # Exit a parse tree produced by speakQueryParser#macro.
    def exitMacro(self, ctx: speakQueryParser.MacroContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#statsAgg.
    def enterStatsAgg(self, ctx: speakQueryParser.StatsAggContext):
        pass

    # Exit a parse tree produced by speakQueryParser#statsAgg.
    def exitStatsAgg(self, ctx: speakQueryParser.StatsAggContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#variableList.
    def enterVariableList(self, ctx: speakQueryParser.VariableListContext):
        pass

    # Exit a parse tree produced by speakQueryParser#variableList.
    def exitVariableList(self, ctx: speakQueryParser.VariableListContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#subsearch.
    def enterSubsearch(self, ctx: speakQueryParser.SubsearchContext):
        pass

    # Exit a parse tree produced by speakQueryParser#subsearch.
    def exitSubsearch(self, ctx: speakQueryParser.SubsearchContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#functionCall.
    def enterFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#functionCall.
    def exitFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#statsFunctionCall.
    def enterStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        pass

    # Exit a parse tree produced by speakQueryParser#statsFunctionCall.
    def exitStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#regexTarget.
    def enterRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        pass

    # Exit a parse tree produced by speakQueryParser#regexTarget.
    def exitRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvfindObject.
    def enterMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvfindObject.
    def exitMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvindexIndex.
    def enterMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvindexIndex.
    def exitMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvDelim.
    def enterMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        pass

    # Exit a parse tree produced by speakQueryParser#mvDelim.
    def exitMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#inputCron.
    def enterInputCron(self, ctx: speakQueryParser.InputCronContext):
        pass

    # Exit a parse tree produced by speakQueryParser#inputCron.
    def exitInputCron(self, ctx: speakQueryParser.InputCronContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#cronformat.
    def enterCronformat(self, ctx: speakQueryParser.CronformatContext):
        pass

    # Exit a parse tree produced by speakQueryParser#cronformat.
    def exitCronformat(self, ctx: speakQueryParser.CronformatContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#timespan.
    def enterTimespan(self, ctx: speakQueryParser.TimespanContext):
        pass

    # Exit a parse tree produced by speakQueryParser#timespan.
    def exitTimespan(self, ctx: speakQueryParser.TimespanContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#variableName.
    def enterVariableName(self, ctx: speakQueryParser.VariableNameContext):
        pass

    # Exit a parse tree produced by speakQueryParser#variableName.
    def exitVariableName(self, ctx: speakQueryParser.VariableNameContext):
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass
        self.validate_exceptions(ctx)

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************
    # CRITICAL COMPONENT
    def validate_exceptions(self, ctx_obj):
        """
        Validates exceptions by checking the current context and handling errors gracefully.

        This method inspects the current parse tree context to identify any syntactical or logical
        errors. Depending on the type of directive or expression being processed, it performs
        necessary validations and updates the internal state of the DataFrame accordingly.

        Args:
            ctx_obj (ParseTree): The current parse tree context from the ANTLR4 parser.

        Raises:
            RuntimeError: If validation fails or if unexpected syntax is detected.
        """
        obj_identifier = inspect.currentframe().f_back.f_code.co_name
        if not obj_identifier or not isinstance(obj_identifier, str):
            self.generic_processing_exit('validate_exceptions', 'General Syntax Failure.')

    # CRITICAL COMPONENT
    @staticmethod
    def generic_processing_exit(obj_failure, err_msg):
        """
        Logs an error message and raises a RuntimeError to allow for graceful error handling.

        Args:
            obj_failure (str): Identifier for where the failure occurred.
            err_msg (str): Detailed error message.

        Raises:
            RuntimeError: With the provided error message.
        """
        logging.error(f'[x] Failure at "{obj_failure}". {err_msg}')
        raise RuntimeError(f'Failure at "{obj_failure}". {err_msg}')

    # CRITICAL COMPONENT
    def ctx_flatten(self, ctx):
        return self.flatten_with_parens(self.extract_screenshot_of_ctx(ctx))

    # CRITICAL COMPONENT
    def extract_screenshot_of_ctx(self, ctx):
        """
        This function recursively processes the context tree and generates a list representing
        all terminal nodes, without handling parentheses nesting.
        """
        tokens_to_skip = {'\n', '\r', '\t', ' ', '', ','}

        if ctx is None:
            return None

        # Base case: If the context is a terminal node, return its text
        if isinstance(ctx, TerminalNodeImpl):
            text = ctx.getText()
            if text.strip() in tokens_to_skip:
                return None  # Skip empty or unwanted tokens
            else:
                return text

        rule_name = type(ctx).__name__
        children_results = []  # List to hold the final results

        # Traverse the children of the context node
        if hasattr(ctx, 'children') and ctx.children:
            idx = 0
            while idx < len(ctx.children):
                child = ctx.children[idx]
                child_result = self.extract_screenshot_of_ctx(child)  # Recursively process each child

                if child_result is not None:
                    children_results.append(child_result)  # Just append each result flatly

                idx += 1

            # Remove any unwanted tokens (None values)
            children_results = [child for child in children_results if child is not None]

            # Return the flattened list of results
            return self.flatten_list(children_results)
        else:
            return None

    # CRITICAL COMPONENT
    @staticmethod
    def flatten_list(result):
        """
        Flatten lists that have only one element to avoid unnecessary nesting.
        """
        if isinstance(result, list):
            flat_result = []
            for item in result:
                if isinstance(item, list):
                    flat_result.extend(item)  # Flatten deeper levels
                else:
                    flat_result.append(item)
            # If the list has only one item, return that item directly
            if len(flat_result) == 1:
                return flat_result[0]
            return flat_result
        else:
            return result

    # CRITICAL COMPONENT
    @staticmethod
    def flatten_with_parens(input_list):
        """
        Flattens a nested list while preserving parentheses as lists.
        """

        def flatten_recursive(element):
            # If the element is a list, recursively flatten its children
            if isinstance(element, list):
                # If it's an empty list, return it as is
                if not element:
                    return []

                result = []
                for item in element:
                    # Flatten each child, preserving the parentheses structure
                    if item == '(':
                        result.append('(')  # Preserve parentheses as separate items
                    elif item == ')':
                        result.append(')')  # Preserve parentheses as separate items
                    else:
                        # Recursively flatten nested lists
                        result.extend(flatten_recursive(item))
                return result
            # If the element is a string, return it directly
            elif isinstance(element, str):
                return [element]
            else:
                return []

        # Call the recursive flatten function
        return flatten_recursive(input_list)


del speakQueryParser

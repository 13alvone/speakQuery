#!/usr/bin/env python3
import logging
import inspect
import glob
import os
from typing import List, Optional
import r_datetime_parser

from antlr4.tree.Tree import ParseTree
import antlr4.tree.Tree
from antlr4 import *
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
from pyarrow.dataset import Expression

from handlers.GeneralHandler import GeneralHandler

logging.basicConfig(level=logging.DEBUG, format='%(message)s')

if "." in __name__:
    from .speakQueryParser import speakQueryParser
else:
    from speakQueryParser import speakQueryParser


# This class defines a complete listener for a parse tree produced by speakQueryParser.
class speakQueryListener(ParseTreeListener):
    def __init__(self, original_query, base_index_url=None):
        self.condition_stack = []
        self.main_df = None
        self.original_query = original_query.strip()
        self.filter_blocks = []
        self.current_filter_block = None
        self.filter_block_stack = []
        self.latest_times = []
        self.earliest_times = []
        self.indexes = []
        self.general_handler = GeneralHandler()
        self.split_query()
        self.current_filter_block = None

        # Determine the base index URL
        if base_index_url is None:
            # Set to the 'indexes' directory relative to the script location
            self.target_index_uri = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'indexes'))
        else:
            self.target_index_uri = os.path.abspath(base_index_url)

    # Enter a parse tree produced by speakQueryParser#query.
    def enterQuery(self, ctx: speakQueryParser.QueryContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#query.
    def exitQuery(self, ctx: speakQueryParser.QueryContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#speakQuery.
    def enterSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#speakQuery.
    def exitSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#filterExpression.
    def enterFilterExpression(self, ctx: speakQueryParser.FilterExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#filterExpression.
    def exitFilterExpression(self, ctx: speakQueryParser.FilterExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#filterTerm.
    def enterFilterTerm(self, ctx: speakQueryParser.FilterTermContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#filterTerm.
    def exitFilterTerm(self, ctx: speakQueryParser.FilterTermContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#preTimeFilter.
    def enterPreTimeFilter(self, ctx: speakQueryParser.PreTimeFilterContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#preTimeFilter.
    def exitPreTimeFilter(self, ctx: speakQueryParser.PreTimeFilterContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#earliestClause.
    def enterEarliestClause(self, ctx: speakQueryParser.EarliestClauseContext):
        self.validate_exceptions(ctx)
        time_value = ctx.getChild(2).getText().strip('"')
        epoch_time = r_datetime_parser.parse_dates_to_epoch([time_value])[0]
        # Update the current filter block
        if self.filter_block_stack:
            self.filter_block_stack[-1].earliest = epoch_time
        else:
            # Handle the case where no filter block is active
            self.generic_processing_exit('enterEarliestClause', 'No active filter block.')

    # Exit a parse tree produced by speakQueryParser#earliestClause.
    def exitEarliestClause(self, ctx: speakQueryParser.EarliestClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#latestClause.
    def enterLatestClause(self, ctx: speakQueryParser.LatestClauseContext):
        self.validate_exceptions(ctx)
        time_value = ctx.getChild(2).getText().strip('"')
        epoch_time = r_datetime_parser.parse_dates_to_epoch([time_value])[0]
        if self.filter_block_stack:
            self.filter_block_stack[-1].latest = epoch_time
        else:
            self.generic_processing_exit('enterLatestClause', 'No active filter block.')

    # Exit a parse tree produced by speakQueryParser#latestClause.
    def exitLatestClause(self, ctx: speakQueryParser.LatestClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx)
        # Initialize a new FilterBlock and push it onto the stack
        new_filter_block = FilterBlock()
        self.filter_block_stack.append(new_filter_block)

    # Exit a parse tree produced by speakQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx)
        # Pop the current filter block from the stack
        current_filter_block = self.filter_block_stack.pop()
        # Assign the constructed expression
        current_filter_block.conditions = ctx.logicalOrExpr().expression

        if not self.filter_block_stack:
            # This is the outermost filter block
            if current_filter_block.indexes:
                self.filter_blocks.append(current_filter_block)
            else:
                logging.debug("Skipping filter block without indexes.")
        else:
            # Merge with the parent filter block
            parent_filter_block = self.filter_block_stack[-1]
            # Combine conditions using logical AND
            if parent_filter_block.conditions is not None and current_filter_block.conditions is not None:
                parent_filter_block.conditions = parent_filter_block.conditions & current_filter_block.conditions
            elif parent_filter_block.conditions is None:
                parent_filter_block.conditions = current_filter_block.conditions
            # Else, keep parent_filter_block.conditions as is
            # Merge indexes
            parent_filter_block.indexes.extend(current_filter_block.indexes)
            # Merge earliest and latest times appropriately
            if current_filter_block.earliest:
                if parent_filter_block.earliest is None or current_filter_block.earliest > parent_filter_block.earliest:
                    parent_filter_block.earliest = current_filter_block.earliest
            if current_filter_block.latest:
                if parent_filter_block.latest is None or current_filter_block.latest < parent_filter_block.latest:
                    parent_filter_block.latest = current_filter_block.latest

        logging.debug(f"Exiting LogicalExpr, current_filter_block conditions: {current_filter_block.conditions}")

    # Enter a parse tree produced by speakQueryParser#logicalOrExpr.
    def enterLogicalOrExpr(self, ctx: speakQueryParser.LogicalOrExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalOrExpr.
    def exitLogicalOrExpr(self, ctx: speakQueryParser.LogicalOrExprContext):
        self.validate_exceptions(ctx)
        expressions = [child.expression for child in ctx.logicalAndExpr()]
        # Filter out None expressions
        expressions = [expr for expr in expressions if expr is not None]
        if expressions:
            expr = expressions[0]
            for next_expr in expressions[1:]:
                expr = expr | next_expr
            ctx.expression = expr
        else:
            # No valid expressions to combine
            ctx.expression = None

    # Enter a parse tree produced by speakQueryParser#logicalAndExpr.
    def enterLogicalAndExpr(self, ctx: speakQueryParser.LogicalAndExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalAndExpr.
    def exitLogicalAndExpr(self, ctx: speakQueryParser.LogicalAndExprContext):
        self.validate_exceptions(ctx)
        expressions = [child.expression for child in ctx.logicalNotExpr()]
        # Filter out None expressions
        expressions = [expr for expr in expressions if expr is not None]
        if expressions:
            expr = expressions[0]
            for next_expr in expressions[1:]:
                expr = expr & next_expr
            ctx.expression = expr
        else:
            # No valid expressions to combine
            ctx.expression = None

    # Enter a parse tree produced by speakQueryParser#logicalNotExpr.
    def enterLogicalNotExpr(self, ctx: speakQueryParser.LogicalNotExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalNotExpr.
    def exitLogicalNotExpr(self, ctx: speakQueryParser.LogicalNotExprContext):
        self.validate_exceptions(ctx)
        if ctx.NOT():
            child_expr = ctx.logicalNotExpr().expression
            if child_expr is not None:
                ctx.expression = ~child_expr
            else:
                ctx.expression = None
        else:
            ctx.expression = ctx.logicalPrimaryExpr().expression

    # Enter a parse tree produced by speakQueryParser#logicalPrimaryExpr.
    def enterLogicalPrimaryExpr(self, ctx: speakQueryParser.LogicalPrimaryExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalPrimaryExpr.
    def exitLogicalPrimaryExpr(self, ctx: speakQueryParser.LogicalPrimaryExprContext):
        self.validate_exceptions(ctx)
        if ctx.comparisonExpr():
            ctx.expression = ctx.comparisonExpr().expression
        elif ctx.inExpression():
            ctx.expression = ctx.inExpression().expression
        elif ctx.logicalExpr():
            ctx.expression = ctx.logicalExpr().expression
        else:
            # For earliestClause, latestClause, indexClause
            # Set expression to None
            ctx.expression = None

    # Enter a parse tree produced by speakQueryParser#indexClause.
    def enterIndexClause(self, ctx: speakQueryParser.IndexClauseContext):
        self.validate_exceptions(ctx)
        index_value = ctx.getChild(2).getText().strip('"')
        if self.filter_block_stack:
            self.filter_block_stack[-1].indexes.append(index_value)
        else:
            self.generic_processing_exit('enterIndexClause', 'No active filter block.')

    # Exit a parse tree produced by speakQueryParser#indexClause.
    def exitIndexClause(self, ctx: speakQueryParser.IndexClauseContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#comparisonExpr.
    def enterComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#comparisonExpr.
    def exitComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx)
        # Extract operands and operator
        left = ctx.additiveExpr(0).getText()
        operator = ctx.comparisonOperator().getText()
        right = ctx.additiveExpr(1).getText().strip('"')

        # Convert right operand to appropriate type
        if right.isdigit():
            right_value = int(right)
        elif right.lower() in ['true', 'false']:
            right_value = right.lower() == 'true'
        else:
            right_value = right.strip('"')

        # Build expression
        field_ref = ds.field(left)

        field_ref = ds.field(left)
        if operator in ['==', '=']:
            expr = field_ref.is_valid() & (field_ref == right_value)
        elif operator == '!=':
            expr = field_ref.is_null() | (field_ref != right_value)
        elif operator in ['>=', '<=', '>', '<']:
            expr = field_ref.is_valid() & (getattr(field_ref, operator)(right_value))
        else:
            self.generic_processing_exit('exitComparisonExpr', f'Unknown operator: {operator}')
        ctx.expression = expr
        logging.debug(f"Built comparison expression: {expr}")
        logging.debug(f"Built comparison expression: {field_ref} {operator} {right_value}")

    # Enter a parse tree produced by speakQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#additiveExpr.
    def enterAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#additiveExpr.
    def exitAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#multiplicativeExpr.
    def enterMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#multiplicativeExpr.
    def exitMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#primaryExpr.
    def enterPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#primaryExpr.
    def exitPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#booleanExpr.
    def enterBooleanExpr(self, ctx: speakQueryParser.BooleanExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#booleanExpr.
    def exitBooleanExpr(self, ctx: speakQueryParser.BooleanExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#inExpression.
    def enterInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#inExpression.
    def exitInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx)
        field = ctx.variableName().getText()
        # Access the expressions directly
        expressions = ctx.expression()
        values = [expr.getText().strip('"') for expr in expressions]
        converted_values = []
        for v in values:
            if v.isdigit():
                converted_values.append(int(v))
            elif v.lower() in ['true', 'false']:
                converted_values.append(v.lower() == 'true')
            else:
                converted_values.append(v.strip('"'))

        field_ref = ds.field(field)
        expr = field_ref.is_valid() & field_ref.isin(converted_values)
        ctx.expression = expr
        logging.debug(f"Built IN expression: {expr}")
        logging.debug(f"Built IN expression: {field} IN {converted_values}")

    # Enter a parse tree produced by speakQueryParser#alternateStartingCall.
    def enterAlternateStartingCall(self, ctx: speakQueryParser.AlternateStartingCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#alternateStartingCall.
    def exitAlternateStartingCall(self, ctx: speakQueryParser.AlternateStartingCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#validLine.
    def enterValidLine(self, ctx: speakQueryParser.ValidLineContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#validLine.
    def exitValidLine(self, ctx: speakQueryParser.ValidLineContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#directive.
    def enterDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#directive.
    def exitDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#statsAgg.
    def enterStatsAgg(self, ctx: speakQueryParser.StatsAggContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#statsAgg.
    def exitStatsAgg(self, ctx: speakQueryParser.StatsAggContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#variableList.
    def enterVariableList(self, ctx: speakQueryParser.VariableListContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#variableList.
    def exitVariableList(self, ctx: speakQueryParser.VariableListContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#subsearchList.
    def enterSubsearchList(self, ctx: speakQueryParser.SubsearchListContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#subsearchList.
    def exitSubsearchList(self, ctx: speakQueryParser.SubsearchListContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#subsearch.
    def enterSubsearch(self, ctx: speakQueryParser.SubsearchContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#subsearch.
    def exitSubsearch(self, ctx: speakQueryParser.SubsearchContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#sharedField.
    def enterSharedField(self, ctx: speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#sharedField.
    def exitSharedField(self, ctx: speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#filename.
    def enterFilename(self, ctx: speakQueryParser.FilenameContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#filename.
    def exitFilename(self, ctx: speakQueryParser.FilenameContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#specialFunctionName.
    def enterSpecialFunctionName(self, ctx: speakQueryParser.SpecialFunctionNameContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#specialFunctionName.
    def exitSpecialFunctionName(self, ctx: speakQueryParser.SpecialFunctionNameContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#expression.
    def enterExpression(self, ctx: speakQueryParser.ExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#expression.
    def exitExpression(self, ctx: speakQueryParser.ExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#ifExpression.
    def enterIfExpression(self, ctx: speakQueryParser.IfExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#ifExpression.
    def exitIfExpression(self, ctx: speakQueryParser.IfExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#caseExpression.
    def enterCaseExpression(self, ctx: speakQueryParser.CaseExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#caseExpression.
    def exitCaseExpression(self, ctx: speakQueryParser.CaseExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#caseMatch.
    def enterCaseMatch(self, ctx: speakQueryParser.CaseMatchContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#caseMatch.
    def exitCaseMatch(self, ctx: speakQueryParser.CaseMatchContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#caseTrue.
    def enterCaseTrue(self, ctx: speakQueryParser.CaseTrueContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#caseTrue.
    def exitCaseTrue(self, ctx: speakQueryParser.CaseTrueContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#catchAllExpression.
    def enterCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#catchAllExpression.
    def exitCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#functionCall.
    def enterFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#functionCall.
    def exitFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#statsFunctionCall.
    def enterStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#statsFunctionCall.
    def exitStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#staticNumber.
    def enterStaticNumber(self, ctx: speakQueryParser.StaticNumberContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#staticNumber.
    def exitStaticNumber(self, ctx: speakQueryParser.StaticNumberContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#staticString.
    def enterStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#staticString.
    def exitStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#multivalueField.
    def enterMultivalueField(self, ctx: speakQueryParser.MultivalueFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#multivalueField.
    def exitMultivalueField(self, ctx: speakQueryParser.MultivalueFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#multivalueStringField.
    def enterMultivalueStringField(self, ctx: speakQueryParser.MultivalueStringFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#multivalueStringField.
    def exitMultivalueStringField(self, ctx: speakQueryParser.MultivalueStringFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#multivalueNumericField.
    def enterMultivalueNumericField(self, ctx: speakQueryParser.MultivalueNumericFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#multivalueNumericField.
    def exitMultivalueNumericField(self, ctx: speakQueryParser.MultivalueNumericFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#staticMultivalueStringField.
    def enterStaticMultivalueStringField(self, ctx: speakQueryParser.StaticMultivalueStringFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#staticMultivalueStringField.
    def exitStaticMultivalueStringField(self, ctx: speakQueryParser.StaticMultivalueStringFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#staticMultivalueNumericField.
    def enterStaticMultivalueNumericField(self, ctx: speakQueryParser.StaticMultivalueNumericFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#staticMultivalueNumericField.
    def exitStaticMultivalueNumericField(self, ctx: speakQueryParser.StaticMultivalueNumericFieldContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#regexTarget.
    def enterRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#regexTarget.
    def exitRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvfindObject.
    def enterMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#mvfindObject.
    def exitMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvindexIndex.
    def enterMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#mvindexIndex.
    def exitMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#mvDelim.
    def enterMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#mvDelim.
    def exitMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#inputCron.
    def enterInputCron(self, ctx: speakQueryParser.InputCronContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#inputCron.
    def exitInputCron(self, ctx: speakQueryParser.InputCronContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#cronformat.
    def enterCronformat(self, ctx: speakQueryParser.CronformatContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#cronformat.
    def exitCronformat(self, ctx: speakQueryParser.CronformatContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#executionMacro.
    def enterExecutionMacro(self, ctx: speakQueryParser.ExecutionMacroContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#executionMacro.
    def exitExecutionMacro(self, ctx: speakQueryParser.ExecutionMacroContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#timeStringValue.
    def enterTimeStringValue(self, ctx: speakQueryParser.TimeStringValueContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#timeStringValue.
    def exitTimeStringValue(self, ctx: speakQueryParser.TimeStringValueContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#timespan.
    def enterTimespan(self, ctx: speakQueryParser.TimespanContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#timespan.
    def exitTimespan(self, ctx: speakQueryParser.TimespanContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#variableName.
    def enterVariableName(self, ctx: speakQueryParser.VariableNameContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#variableName.
    def exitVariableName(self, ctx: speakQueryParser.VariableNameContext):
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

        # resolved_nodes = self.resolve_terminal_nodes(ctx_obj)
        # cleaned_expression = self.general_handler.convert_nested_list(resolved_nodes)
        pass  # Placeholder for any exception validation or common processing

    @staticmethod
    def resolve_terminal_nodes(ctx_obj):
        """
        Resolves terminal nodes from the given context object iteratively.

        Args:
            ctx_obj (ParseTree): The context object from which to resolve nodes.

        Returns:
            list: A list of resolved nodes.
        """
        resolved_list = []
        stack = [ctx_obj]

        while stack:
            current = stack.pop()
            if isinstance(current, antlr4.tree.Tree.TerminalNodeImpl):
                resolved_list.append(current)
            elif hasattr(current, 'children') and current.children:
                stack.extend(current.children[::-1])  # Add children to stack in reverse order to maintain order

        return resolved_list

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

    def resolve_index_paths(self, index_list):
        index_paths = []
        allowed_base = self.target_index_uri
        logging.debug(f"Resolving index paths with base: {allowed_base}")
        for idx in index_list:
            idx = idx.strip()
            pattern = os.path.join(allowed_base, idx)
            logging.debug(f"Pattern to glob: {pattern}")
            matched_paths = glob.glob(pattern)
            if matched_paths:
                logging.debug(f"Matched paths: {matched_paths}")
            else:
                logging.warning(f"No files matched the index pattern: {pattern}")
            index_paths.extend(matched_paths)
        return index_paths

    def process_filter_blocks(self):
        dataframes = []
        for block in self.filter_blocks:
            # Resolve index paths for the current block
            index_paths = self.resolve_index_paths(block.indexes)
            if not index_paths:
                logging.warning("No index paths found for the current filter block.")
                continue

            # Build filter expressions
            filters = []
            if block.earliest is not None:
                filters.append(ds.field('_epoch') >= block.earliest)
            if block.latest is not None:
                filters.append(ds.field('_epoch') <= block.latest)
            if block.conditions is not None:
                filters.append(block.conditions)

            # Combine filters
            if filters:
                combined_filter = filters[0]
                for f in filters[1:]:
                    combined_filter = combined_filter & f
            else:
                combined_filter = None

            logging.debug(f"Combined filter for block: {combined_filter}")

            # Apply filters to the dataset
            filtered_table = self.apply_combined_filter(index_paths, combined_filter)
            if filtered_table is not None and len(filtered_table) > 0:
                df = filtered_table.to_pandas()
                dataframes.append(df)
            else:
                logging.info("No data returned after applying filters to the dataset.")

        # Combine all dataframes
        if dataframes:
            self.main_df = pd.concat(dataframes, ignore_index=True)
        else:
            self.main_df = pd.DataFrame()

    @staticmethod
    def apply_combined_filter(dataset_paths, combined_filter):
        """
        Applies the combined filter to multiple PyArrow datasets efficiently.

        Args:
            dataset_paths (List[str]): List of paths to the PyArrow datasets.
            combined_filter (Optional[Expression]): The combined PyArrow filter expression.

        Returns:
            Optional[pa.Table]: The concatenated filtered PyArrow Table if successful, otherwise None.
        """
        try:
            dataset = ds.dataset(dataset_paths, format="parquet")
            # Use 'is not None' to avoid evaluating the Expression as a boolean
            if combined_filter is not None:
                filtered_table = dataset.to_table(filter=combined_filter)
            else:
                filtered_table = dataset.to_table()
            logging.info(f"[i] Successfully filtered {len(dataset_paths)} datasets.")
            return filtered_table
        except FileNotFoundError:
            logging.error(f"[x] File(s) do not exist: {', '.join(dataset_paths)}")
        except TypeError as e:
            logging.error(f"[x] Type error reported: {e}")
        except Exception as e:
            logging.error(f"[x] Error processing datasets: {e}")
        return None

    @staticmethod
    def get_child_context(ctx, method_name, index=0):
        method = getattr(ctx, method_name, None)
        if method is None:
            return None
        if callable(method):
            result = method(index)
            return result
        return None

    def remove_nan_values(self):
        """
        Removes rows with NaN values from the main DataFrame.
        """
        # if self.main_df is not None and not self.main_df.empty:
        #     self.main_df = self.main_df.dropna(how='any')
        #     logging.info("[i] Removed NaN values from the DataFrame.")
        # THIS IS CURRENTLY DISABLED BECAUSE IT REMOVES ENTIRE DATAFRAMES IF IT CONTAINS NANS RATHER THAN REPLACE THEM WHERE FOUND, IN PLACE.
        pass

    def split_query(self):
        parts = self.original_query.split('|', 1)
        self.initial_query = parts[0].strip()
        self.post_pipe_commands = parts[1].strip() if len(parts) > 1 else ''

    def process_post_pipe_commands(self):
        if self.post_pipe_commands:
            commands = self.post_pipe_commands.split('|')
            for cmd in commands:
                cmd = cmd.strip()
                if cmd.startswith('eval '):
                    self.handle_eval_command(cmd[5:].strip())
                elif cmd.startswith('lookup '):
                    self.handle_lookup_command(cmd[7:].strip())
                elif cmd.startswith('table '):
                    self.handle_table_command(cmd[6:].strip())
                else:
                    logging.warning(f"Unknown command: {cmd}")

    def handle_eval_command(self, expression):
        # Parse and evaluate the expression, e.g., test="This is a test query."
        var_name, value = expression.split('=', 1)
        var_name = var_name.strip()
        value = value.strip().strip('"')
        self.main_df[var_name] = value

    def handle_lookup_command(self, expression):
        # Implement lookup logic
        pass

    def handle_table_command(self, fields):
        field_list = [field.strip() for field in fields.split(',')]
        self.main_df = self.main_df[field_list]


class FilterBlock:
    def __init__(self):
        self.conditions = None  # PyArrow expression
        self.earliest = None    # Epoch timestamp
        self.latest = None      # Epoch timestamp
        self.indexes = []       # List of index paths


del speakQueryParser

#!/usr/bin/env python3
import logging
import inspect
import time
import os
import re
from collections import OrderedDict
from typing import List, Optional
import r_datetime_parser

from antlr4.tree.Tree import ParseTree
import antlr4.tree.Tree
from antlr4 import *
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
import pyarrow as pa
from pyarrow.dataset import Expression

from handlers.archive.ParquetHandler import ParquetHandler
from handlers.StringHandler import StringHandler
from handlers.archive.TimeHandler import TimeHandler
from handlers.MathematicOperations import MathHandler
from handlers.GeneralHandler import GeneralHandler
from handlers.archive.StackHandler import StackHandler
from handlers.LookupHandler import LookupHandler
from handlers.output_parquets._IndexFilterHandler import IndexFilterHandler

from functionality.archive.call_stats import CallStats
from lexers.speakQueryParser import speakQueryParser

logging.basicConfig(level=logging.INFO, format='%(message)s')


# This class defines a complete listener for a parse tree produced by speakQueryParser.
class speakQueryListener(ParseTreeListener):
    def __init__(self, original_query):
        self.base_app_dir = os.path.abspath(os.path.pardir)
        self.prefilters = None
        self.prefilter_enabled = False
        self.target_tables_paths = []
        self.current_string_function = None
        self.single_existing_variable_eval = False
        self.logical_operators = ('AND', 'OR', 'NOT')
        self.earliest_time = None
        self.latest_time = None
        self.prefilter_conditions = []
        self.temporary_storage = []
        self.first_eval_variable = True
        self.add_to_storage = False
        self.early_exit = False
        self.is_loadjob = False
        self.start_time = time.time()
        self.current_inputlookup = None
        self.drill_sql_cmd = ''
        self.current_comparisonExpr = None
        self.current_staticNumber = {}
        self.current_variable = None
        self.eval_cache = []
        self.lost_expression = None
        self.current_arithmeticExr = None
        self.unary_resolutions = OrderedDict()
        self.last_numeric_value = OrderedDict()
        self.current_unary_expression = None
        self.eval_var = None
        self.last_variable_value = None
        self.target_tables = []
        self.resolved_query = ''
        self.variables = []
        self.last_expression_result = {}  # List of tuples
        self.last_expression = ''
        self.original_query = original_query.strip().lstrip('\n')
        self.query_body = None
        self.script_directory = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.lookup_path = f'{self.script_directory}/../lookups'
        self.jdbc_jar_path = f'{self.script_directory}/../apache_drill/apache-drill-1.21.1/jars/jdbc-driver'
        self.target_index_uri = 'indexes/'
        self.current_directive = None
        self.parquet_handler = ParquetHandler()
        self.string_handler = StringHandler()
        self.math_handler = MathHandler()
        self.time_handler = TimeHandler()
        self.general_handler = GeneralHandler()
        self.stack_handler = StackHandler()
        self.lookup_handler = LookupHandler()
        self.index_filter_handler = None
        self.comments = OrderedDict()
        self.literal_names = []
        self.main_df = None
        self.table_cmd = None
        self.current_value = None
        self.contextStack = []
        self.expression_string = None
        self.resolved_lines = []
        self.first_directive = True
        self.directive = None
        self.current_item = None
        self.entries = []
        self.values = OrderedDict()
        self.concat_children = None
        self.ctx_obj_str = None
        self.last_entry = ''
        self.prefilter_grammar = None
        self.comparison_operators = ('>', '<', '>=', '<=', '!=', '==')
        self.known_directives = [
            'file', 'eval', 'limit', 'streamstats', 'eventstats', 'stats', 'where', 'rename', 'fields', 'search',
            'reverse', 'head', 'rex', 'regex', 'dedup', 'maketable', 'timechart', 'lookup', 'inputlookup', 'fillnull',
            'outputlookup', 'fieldsummary', 'special_func', 'spath', 'base64', 'loadjob', 'join', 'appendpipe', 'bin',
            'sort', 'to_epoch']
        self.all_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'concat',
            'replace', 'upper', 'lower', 'capitalize', 'trim', 'ltrim', 'rtrim', 'substr', 'len', 'tostring', 'match',
            'urlencode', 'defang', 'type', 'timerange', 'fang']
        self.all_numeric_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'len']
        self.all_string_funcs = [
            "concat", "replace", "upper", "lower", "capitalize", "trim", "rtrim", "ltrim", "substr", "len",
            "tostring", "match", "urlencode", "urldecode", "defang", "fang", "type"]

    # Enter a parse tree produced by speakQueryParser#program.
    def enterProgram(self, ctx: speakQueryParser.ProgramContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#program.
    def exitProgram(self, ctx: speakQueryParser.ProgramContext):
        logging.info(f'[+] Successfully processed valid speakQuery syntax and collapsed expressions.\n')
        logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n'
                     f'[+] RESULT:\n{self.main_df}\n')
        logging.info(f'[+] Transposed Summary of Fields: (First 2 row entries shown from each column)'
                     f'{self.general_handler.get_main_df_overview(self.main_df)}')
        logging.info(f"[i] Parsing process completed. {ctx.__str__}")
        logging.info("[i] Duration: {:.2f} seconds".format(time.time() - self.start_time))
        return self.main_df

    # Enter a parse tree produced by speakQueryParser#speakQuery.
    def enterSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#speakQuery.
    def exitSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#preFilter.
    def enterPreFilter(self, ctx: speakQueryParser.PreFilterContext):
        self.prefilter_enabled = True
        # self.prefilters = PrefilterHandler(self.flatten_ctx(ctx))
        self.validate_exceptions(ctx)
        self.index_filter_handler = IndexFilterHandler(self.original_query)
        self.main_df = self.index_filter_handler.get_filtered_data()
        self.query_body = self.index_filter_handler.get_remaining_query()
        y = 'debug'

    # Exit a parse tree produced by speakQueryParser#preFilter.
    def exitPreFilter(self, ctx: speakQueryParser.PreFilterContext):
        self.validate_exceptions(ctx)
        # condition_text = ctx.getText()
        # self.prefilter_conditions.append(condition_text)
        self.prefilter_enabled = False

    # Enter a parse tree produced by speakQueryParser#preTimeFilter.
    def enterPreTimeFilter(self, ctx: speakQueryParser.PreTimeFilterContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#preTimeFilter.
    def exitPreTimeFilter(self, ctx: speakQueryParser.PreTimeFilterContext):
        self.validate_exceptions(ctx)
        tokens = [child.getText() for child in ctx.getChildren()]
        if tokens[0] == 'earliest':
            self.earliest_time = tokens[2].strip('"')
        elif tokens[0] == 'latest':
            self.latest_time = tokens[2].strip('"')

    # Enter a parse tree produced by speakQueryParser#logicalOrComparisonExpr.
    def enterLogicalOrComparisonExpr(self, ctx: speakQueryParser.LogicalOrComparisonExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalOrComparisonExpr.
    def exitLogicalOrComparisonExpr(self, ctx: speakQueryParser.LogicalOrComparisonExprContext):
        self.validate_exceptions(ctx)

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
        self.current_directive = f'{ctx.children[0]}'
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#directive.
    def exitDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.validate_exceptions(ctx)
        last_val = self.resolve_terminal_nodes(ctx)

        directive_parts = last_val
        if not self.current_directive:
            self.generic_processing_exit('exitDirective', f'Potential Malformed Directive Call: Missing directive.')
        if isinstance(last_val, list):
            if self.current_directive != 'reverse':
                directive_parts = [entry for entry in last_val if isinstance(entry, list)]
        elif isinstance(last_val, antlr4.tree.Tree.TerminalNodeImpl):
            directive_parts = str(last_val)

        if directive_parts:
            if isinstance(directive_parts, list) and self.current_directive != 'reverse':
                directive_parts = directive_parts[0]
        else:
            directive_parts = last_val[1:]

        # STREAMSTATS Clause Handling
        if self.current_directive == 'streamstats':
            self.validate_exceptions(ctx)

        # EVENTSTATS Clause Handling
        elif self.current_directive == 'eventstats':
            if isinstance(directive_parts, list):
                if len(directive_parts) >= 3:
                    sq_stats = CallStats(self.main_df)
                    self.main_df = sq_stats.execute_eventstats(
                        self.general_handler.convert_nested_list(directive_parts))

        # STATS Clause Handling
        elif self.current_directive == 'stats':
            if isinstance(directive_parts, list):
                if len(directive_parts) >= 3:
                    sq_stats = CallStats(self.main_df)
                    self.main_df = sq_stats.execute_stats(self.general_handler.convert_nested_list(directive_parts))

        # TIMECHART Clause Handling
        elif self.current_directive == 'timechart':
            self.validate_exceptions(ctx)

        # RENAME Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'rename':
            if isinstance(directive_parts, list):
                if len(directive_parts) >= 3:
                    renames = {}
                    as_flag_enabled = False
                    current_target_field = None
                    for entry in directive_parts:
                        entry = str(entry)
                        if entry == ',':
                            continue
                        if entry in self.main_df.columns and not as_flag_enabled:
                            current_target_field = entry
                        elif entry.lower() == 'as':
                            as_flag_enabled = True
                        elif current_target_field and as_flag_enabled:
                            renames[current_target_field] = entry.strip(' ').strip('"')
                            as_flag_enabled = False
                            current_target_field = None

                    for target_field, rename_to in renames.items():
                        self.general_handler.rename_column(self.main_df, target_field, rename_to)

        # FIELDS Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'fields':
            if isinstance(last_val, list):
                if len(last_val) >= 2:
                    fields_is_set, return_fields, _mode = False, [], '+'
                    for index, entry in enumerate(last_val):
                        if isinstance(entry, list):
                            self.generic_processing_exit('exitDirective', 'Nested list found in fields request.')
                        entry = str(entry)
                        if entry == 'fields' and index == 0:
                            fields_is_set = True
                            continue
                        if fields_is_set and index >= 1:
                            if index == 1:
                                if entry in ('+', '-'):
                                    _mode = entry
                            if _mode and entry not in return_fields:
                                return_fields.append(entry)

                    if return_fields and _mode:
                        if self.current_directive == 'fields':
                            self.main_df = self.general_handler.filter_df_columns(self.main_df, return_fields, _mode)
                        elif self.current_directive == 'maketable':
                            self.main_df = self.general_handler.filter_df_columns(self.main_df, return_fields, '+')
                    else:
                        self.generic_processing_exit('exitDirective', 'FIELDS requires an input. List was empty.')
                else:
                    self.generic_processing_exit('exitDirective', 'List is not appropriate length.')
            else:
                self.generic_processing_exit('exitDirective', 'Fields command missing list type')

        # LOOKUP Clause Handling
        elif self.current_directive == 'lookup':
            if len(directive_parts) == 2:
                if self.general_handler.are_all_terminal_instances(directive_parts):
                    self.main_df = self.execute_lookup(str(directive_parts[0]), str(directive_parts[-1]))
                else:
                    self.generic_processing_exit('exitDirective', 'LOOKUP clause is malformed.')
            else:
                self.generic_processing_exit('exitDirective', f'LOOKUP directive is malformed. '
                                                              f'Expected 3 entries, received {len(last_val)}')

        # FIELDSUMMARY Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'fieldsummary':
            self.main_df = self.general_handler.execute_fieldsummary(self.main_df)

        # HEAD Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'head':
            if isinstance(last_val, list):
                if len(last_val) == 2:
                    self.main_df = self.general_handler.head_call(self.main_df, int(str(last_val[-1])), 'head')
                else:
                    self.generic_processing_exit('exitDirective', 'HEAD list must be of length 2.')
            else:
                self.generic_processing_exit('exitDirective', 'HEAD must accept a list, which was not provided.')

        # LIMIT Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'limit':
            if isinstance(last_val, list):
                if len(last_val) == 2:
                    self.main_df = self.general_handler.head_call(self.main_df, int(str(last_val[-1])), 'tail')
                else:
                    self.generic_processing_exit('exitDirective', 'LIMIT list must be of length 2.')
            else:
                self.generic_processing_exit('exitDirective', 'LIMIT must accept a list, which was not provided.')

        # BIN Clause Handling
        elif self.current_directive == 'bin':
            if isinstance(last_val, list):
                if len(last_val) >= 2:
                    self.main_df = self.time_handler.bin_time_data(
                        self.main_df, self.general_handler.convert_nested_list(last_val))

        # REVERSE Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'reverse':
            self.main_df = self.general_handler.reverse_df_rows(self.main_df)

        # DEDUP Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'dedup':
            if isinstance(last_val, list):
                if len(last_val) >= 2:
                    self.main_df = \
                        self.general_handler.execute_dedup(self.main_df,
                                                           self.general_handler.convert_nested_list(directive_parts))

        # SORT Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'sort':
            if last_val:
                if isinstance(last_val, list) and len(last_val) >= 3:
                    has_sort_directive, sort_direction, columns = False, '+', []
                    for index, entry in enumerate(last_val):
                        entry = str(entry)
                        if entry == 'sort' and index == 0:
                            has_sort_directive = True
                            continue
                        if index == 1 and entry in ('+', '-'):
                            sort_direction = entry
                            continue
                        if index != 0 and sort_direction and has_sort_directive and entry not in ('+', '-'):
                            columns.append(entry)
                    self.main_df = self.general_handler.sort_df_by_columns(self.main_df, columns, sort_direction)

        # REX Clause Handling (VALIDATED Extraction and SED modes: 04/26/2024)
        # REX requires the following considerations:
        #    - List extractions must be done with the following syntax: (?P<variable_name>regex_expression)
        #    - SED option input must follow this schema: | rex field=header_3 mode=sed "s/(?si)E/REPLACE_LETTER_E/g"
        elif self.current_directive == 'rex':
            self.main_df = self.general_handler.execute_rex(
                self.main_df, self.general_handler.convert_nested_list(last_val))

        # REGEX Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'regex':
            if last_val:
                if isinstance(last_val, list):
                    if len(last_val) >= 3:
                        has_regex_clause, regex_string, target_var = False, None, None
                        for index, entry in enumerate(last_val):
                            entry = str(entry)
                            if index == 0 and entry == 'regex':
                                has_regex_clause = True
                                continue
                            if index == 1 and has_regex_clause and entry not in ('=', '!='):
                                target_var = entry
                                continue
                            if index >= 2 and has_regex_clause and target_var and entry not in ('=', '!='):
                                regex_string = entry

                        self.main_df = self.general_handler.filter_df_by_regex(self.main_df, target_var, regex_string)

        # BASE64 Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'base64':
            self.main_df = self.general_handler.handle_base64(
                self.main_df, self.general_handler.convert_nested_list(last_val))

        # SPECIAL_FUNC Clause Handling
        elif self.current_directive == 'special_func':
            self.validate_exceptions(ctx)

        # FILLNULL Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'fillnull':
            self.general_handler.execute_fillnull(self.main_df, self.general_handler.convert_nested_list(last_val))

        # TO_EPOCH Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'to_epoch':
            out = self.general_handler.extract_outermost_parentheses(self.general_handler.convert_nested_list(last_val))
            date_list = self.get_column_entries(str(out[0]))
            self.add_or_update_column(self.current_variable, r_datetime_parser.parse_dates_to_epoch(date_list))

        # OUTPUTLOOKUP Clause Handling (VALIDATED 04/26/2024)
        elif self.current_directive == 'outputlookup':
            if len(directive_parts) == 1:
                if isinstance(directive_parts[0], antlr4.tree.Tree.TerminalNodeImpl):
                    kwargs = {
                        'filename':
                            f'{self.lookup_path}/{self.general_handler.parse_outputlookup_args(directive_parts)}'}
                    self.general_handler.execute_outputlookup(self.main_df, **kwargs)
            elif len(directive_parts) > 1:
                if self.general_handler.are_all_terminal_instances(directive_parts):
                    directive_parts = self.general_handler.convert_nested_list(directive_parts)
                    kwargs = self.general_handler.parse_outputlookup_args(directive_parts)
                    kwargs['filename'] = f'{self.lookup_path}/{directive_parts[-1]}'
                    try:
                        self.general_handler.execute_outputlookup(self.main_df, **kwargs)
                    except Exception as e:
                        self.generic_processing_exit('exitDirective', e)
                else:
                    self.generic_processing_exit('exitDirective', 'OUTPUTLOOKUP components are malformed.')
            else:
                self.generic_processing_exit('exitDirective', 'OUTPUTLOOKUP components are malformed.')

        # SPATH Clause Handling
        elif self.current_directive == 'spath':
            self.validate_exceptions(ctx)

        # JOIN Clause Handling
        elif self.current_directive == 'join':
            self.validate_exceptions(ctx)

        # APPENDPIPE Clause Handling
        elif self.current_directive == 'appendpipe':
            self.validate_exceptions(ctx)

        self.current_directive = None
        self.first_eval_variable = True

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

    # Enter a parse tree produced by speakQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#logicalTerm.
    def enterLogicalTerm(self, ctx: speakQueryParser.LogicalTermContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#logicalTerm.
    def exitLogicalTerm(self, ctx: speakQueryParser.LogicalTermContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#primaryLogicalExpr.
    def enterPrimaryLogicalExpr(self, ctx: speakQueryParser.PrimaryLogicalExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#primaryLogicalExpr.
    def exitPrimaryLogicalExpr(self, ctx: speakQueryParser.PrimaryLogicalExprContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#comparisonExpr.
    def enterComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#comparisonExpr.
    def exitComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx)

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

    # Enter a parse tree produced by speakQueryParser#inExpression.
    def enterInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#inExpression.
    def exitInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#catchAllExpression.
    def enterCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#catchAllExpression.
    def exitCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
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

    # Enter a parse tree produced by speakQueryParser#doubleQuotedStringList.
    def enterDoubleQuotedStringList(self, ctx: speakQueryParser.DoubleQuotedStringListContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#doubleQuotedStringList.
    def exitDoubleQuotedStringList(self, ctx: speakQueryParser.DoubleQuotedStringListContext):
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

    # Enter a parse tree produced by speakQueryParser#stringFunctionTarget.
    def enterStringFunctionTarget(self, ctx: speakQueryParser.StringFunctionTargetContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#stringFunctionTarget.
    def exitStringFunctionTarget(self, ctx: speakQueryParser.StringFunctionTargetContext):
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#httpStringField.
    def enterHttpStringField(self, ctx: speakQueryParser.HttpStringFieldContext):
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#httpStringField.
    def exitHttpStringField(self, ctx: speakQueryParser.HttpStringFieldContext):
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
        if self.first_eval_variable:
            self.current_variable = self.general_handler.validate_ast(str(ctx.children[0]))
            self.first_eval_variable = False
        self.validate_exceptions(ctx)

    # Exit a parse tree produced by speakQueryParser#variableName.
    def exitVariableName(self, ctx: speakQueryParser.VariableNameContext):
        self.validate_exceptions(ctx)

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************
    def process_table_cmd(self, input_list):
        input_list = [str(x) for x in input_list if str(x) not in '\n']  # Remove newlines

        processed_list = []
        is_file = False
        for index, entry in enumerate(input_list):
            test_entry = str(entry)
            processed_list.append(test_entry)

            if test_entry.lower() == 'file':
                is_file = True
                continue

            if is_file and test_entry == '=':
                continue

            if is_file and test_entry != '=':
                # Capture the file or directory paths
                file_paths = test_entry.strip('"').strip("'")
                if file_paths:
                    # Split the file paths by commas
                    paths = [path.strip() for path in file_paths.split(',')]
                    self.target_tables.extend(paths)
                    for file_path in paths:
                        logging.info(f"Added path to target_tables: {file_path}")
                break  # Assuming only one file= parameter
        return processed_list

    def extract_parameters(self, ctx_list):
        parameters = []
        paren_open = False
        for _index, entry in enumerate(ctx_list):
            if f"'{entry}'" not in self.literal_names and str(entry) not in self.literal_names:
                if _index == 0:
                    continue
                else:
                    if entry == '(':
                        paren_open = True
                        continue
                    if paren_open and entry not in (')', ',', '\n'):
                        parameters.append(entry)

        return parameters

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

    def get_column_entries(self, column_name):
        """
        Retrieve the entries of a specified column as a numpy array.

        Args:
            column_name (str): The name of the column to retrieve.

        Returns:
            np.ndarray: The entries of the column as a numpy array, or an empty array if the column doesn't exist.
        """
        try:
            if column_name in self.main_df.columns:
                logging.info(f"[i] Extracting entries from column '{column_name}'")
                return self.main_df[column_name].to_numpy()
            else:
                logging.error(f"[x] The specified column '{column_name}' does not exist in the DataFrame.")
                return np.array([])
        except Exception as e:
            logging.error(f"[x] An error occurred while extracting entries from column '{column_name}': {e}")
            return np.array([])

    def evaluate_condition(self, column_name, operator, comparison_value):
        if column_name not in self.main_df.columns:
            raise ValueError(f"[x] Column '{column_name}' does not exist in the DataFrame.")

        column_data = self.main_df[column_name]

        if isinstance(comparison_value, str) and comparison_value.startswith('"') and comparison_value.endswith('"'):
            comparison_value = comparison_value.strip('"')

        if isinstance(column_data.iloc[0], (int, float)):
            try:
                comparison_value = float(comparison_value)
            except ValueError:
                raise ValueError(f"[x] Cannot convert comparison value '{comparison_value}' to a number.")
        elif isinstance(column_data.iloc[0], str):
            comparison_value = str(comparison_value)

        if operator == '==':
            return (column_data == comparison_value).values
        elif operator == '!=':
            return (column_data != comparison_value).values
        elif operator == '>':
            return (column_data > comparison_value).values
        elif operator == '<':
            return (column_data < comparison_value).values
        elif operator == '>=':
            return (column_data >= comparison_value).values
        elif operator == '<=':
            return (column_data <= comparison_value).values
        else:
            raise ValueError(f"[x] Invalid operator '{operator}' in the search expression.")

    @staticmethod
    def evaluate_logical_expr(operator, logical_expr_node):
        left_result = logical_expr_node[0]
        # operator = logical_expr_node[1]
        right_result = logical_expr_node[2]

        if operator == 'AND':
            return np.logical_and(left_result, right_result)
        elif operator == 'OR':
            return np.logical_or(left_result, right_result)
        else:
            raise ValueError(f"[x] Invalid logical operator '{operator}'.")

    def evaluate_comparison_expr(self, comparison_expr_node):
        return self.evaluate_condition(comparison_expr_node[0], comparison_expr_node[1], comparison_expr_node[2])

    def evaluate_expression(self, _operator, _expression):
        if _operator == 'logicalExpr':
            return self.evaluate_logical_expr(_operator, _expression)
        elif _operator == 'comparisonExpr':
            return self.evaluate_comparison_expr(_expression)
        else:
            if isinstance(_expression, list):
                if len(_expression) == 1:

                    # When the _expression is a single value list and contains a numpy ndarray of entries to be placed
                    if isinstance(_expression[0], np.ndarray):
                        self.single_existing_variable_eval = False  # Change to false, because this was a sv up to this.

                    # When an eval statement is set to a single existing column from the current self.main_df dataframe
                    elif isinstance(_expression[0], str) and \
                            _expression[0] in self.main_df.columns and \
                            self.current_variable not in self.main_df.columns:
                        self.place_in_df_or_temp_storage(_expression, self.get_column_entries(_expression[0]))
                        self.single_existing_variable_eval = True
                else:
                    pass  # I'm not quite sure what should be processed here, so this is a stub with a breakpoint.
            else:
                # self.generic_processing_exit('exitExpression', f"[x] Invalid expression type '{_operator}'.")
                pass

    def process_search_directive(self, _expression):
        """
        Processes a search directive by applying logical operators to filter the DataFrame.

        Args:
            _expression (List[Any]): The list representing the search expression.

        Returns:
            pd.DataFrame: The filtered DataFrame based on the search criteria.

        Raises:
            ValueError: If an unknown operator is encountered or if the expression is malformed.
        """

        def apply_operator(_op, a, b):
            if _op == 'AND':
                return np.logical_and(a, b)
            elif _op == 'OR':
                return np.logical_or(a, b)
            else:
                raise ValueError(f"[x] Unknown operator: {_op}")

        operators = []
        operands = []

        for token in _expression:
            if isinstance(token, np.ndarray):
                logging.debug("[DEBUG] Adding ndarray to operands stack.")
                operands.append(token)
            elif token in ('AND', 'OR'):
                logging.debug(f"[DEBUG] Adding operator {token} to operators stack.")
                while (operators and operators[-1] in ('AND', 'OR') and
                       operators[-1] == 'AND' and token == 'OR'):
                    op = operators.pop()
                    if len(operands) < 2:
                        self.generic_processing_exit('process_search_directive', 'Insufficient operands for operator.')
                    right = operands.pop()
                    left = operands.pop()
                    logging.debug(f"[DEBUG] Applying operator {op}.")
                    operands.append(apply_operator(op, left, right))
                operators.append(token)
            else:
                self.generic_processing_exit('process_search_directive', f"Invalid token in expression: {token}")

        while operators:
            op = operators.pop()
            if len(operands) < 2:
                self.generic_processing_exit('process_search_directive', 'Insufficient operands for operator.')
            right = operands.pop()
            left = operands.pop()
            logging.debug(f"[DEBUG] Applying final operator {op}.")
            operands.append(apply_operator(op, left, right))

        if not operands:
            self.generic_processing_exit('process_search_directive', 'No operands found after processing.')

        final_mask = operands[0]
        logging.info("[i] Final mask created.")
        return self.main_df[final_mask]

    def add_epoch_time_column(self, df, target_time_field):
        """
        Convert each entry in the TIMESTAMP column of the DataFrame using the time_handler's
        parse_input_date function and store the results in a new column '_epoch'.

        Args:
            df (pd.DataFrame): DataFrame containing the target column.
            target_time_field (str): Target column containing dates to convert.

        Returns:
            pd.DataFrame: Updated DataFrame with an additional '_epoch' column.
        """
        if target_time_field not in df.columns:
            logging.error(f"'{target_time_field}' column not found in the DataFrame.")
            return df

        def parse_date_to_epoch(date_str):  # Define a helper function to parse and convert dates to epoch
            if pd.isna(date_str) or date_str == "":
                return 0
            return self.time_handler.parse_input_date(date_str)

        # Apply the conversion function to each entry in target column
        df['_epoch'] = df[target_time_field].apply(parse_date_to_epoch)

        return df

    def add_or_update_column(self, field_name, values):
        """
        Adds or updates a column in self.main_df with the given list of values.

        Args:
            field_name (str): The name of the column to add or update.
            values (Union[list, any]): A list of values or a single value to add to the column.
        """
        try:
            if isinstance(values, list):
                if len(values) != len(self.main_df):
                    logging.warning(f"[!] List length for '{field_name}' doesn't match DataFrame length. Skipping.")
                    return
                self.main_df[field_name] = values
            else:
                cleaned_vals = [str(values).strip('"')] * len(self.main_df)
                self.main_df[field_name] = cleaned_vals

            logging.info(f"[i] Column '{field_name}' added/updated successfully.")
        except Exception as e:
            logging.error(f"[x] Failed to add/update '{field_name}' as a column: {e}")

    def execute_lookup(self, filename, shared_field):
        """
        Joins data from a file to the main DataFrame based on a shared field.

        Args:
        filename (str): The filename of a file in the "lookups" directory
        shared_field (str): The column name on which to join the DataFrames.

        Returns:
        pandas.DataFrame: The updated DataFrame after the join.
        """

        lookup_df = self.lookup_handler.load_data(f'{self.script_directory}/../lookups/{filename}')
        if lookup_df is None:
            logging.error("[x] Failed to load data for lookup.")
            return self.main_df  # Return the unchanged main DataFrame in case of failure

        if shared_field not in self.main_df.columns or shared_field not in lookup_df.columns:
            logging.error("[x] Shared field '{}' not found in both DataFrames.".format(shared_field))
            return self.main_df  # Return the unchanged main DataFrame in case of missing shared field

        try:
            updated_df = pd.merge(self.main_df, lookup_df, on=shared_field, how='left')  # Perform join operation
            logging.info("[i] Lookup join completed successfully.")
            return updated_df
        except Exception as e:
            logging.error("[x] Error during DataFrame join: {}".format(str(e)))
            return self.main_df  # Return the unchanged main DataFrame in case of join error

    def resolve_terminal_nodes(self, ctx_obj):
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

    def create_ordered_array(self, value):
        """
        Create a numpy array filled with the given value and with the same number of elements as rows in self.main_df.
        Args:
            value (int): The integer to fill the array with.

        Returns:
            np.ndarray: A numpy array filled with the specified integer.
        """
        ordered_array = np.full(self.main_df.shape[0], value)
        logging.info(f"[i] Created an array of size {self.main_df.shape[0]} filled with value {value}")
        return ordered_array

    def exit_if_contains_any_unexpected_operations(self, _cleaned_expression, _option):
        for entry in _cleaned_expression:
            if isinstance(entry, (np.ndarray, list, set)):
                continue
            else:
                if _option == 'exitAdditiveExpr':
                    if entry in ('*', '/'):  # Ensure the opposite operators don't exist, report otherwise
                        self.generic_processing_exit('exitAdditiveExpr:validate_exceptions()',
                                                     f'Mult/Div operatives found in Add/Sub-only operation.\n'
                                                     f'{_cleaned_expression}')
                elif _option == 'exitMultiplicativeExpr':
                    if entry in ('+', '-'):  # Ensure the opposite operators don't exist, report otherwise
                        self.generic_processing_exit('exitMultiplicativeExpr:validate_exceptions()',
                                                     f'Add/Sub operatives found in Mult/Div-only operation.\n'
                                                     f'{_cleaned_expression}')

    def contains_operator(self, expression_list, operators):
        for item in expression_list:
            if isinstance(item, np.ndarray):
                if any(op in item for op in operators):
                    return True
            elif isinstance(item, str):
                if any(op == item for op in operators):
                    return True
            elif isinstance(item, list):
                if self.contains_operator(item, operators):
                    return True
        return False

    def remove_list_params(self, data):
        """
        Recursively filters out elements that are equal to '(' or ')'
        in a list which may contain sub-lists, numpy arrays, or other mixed entries.

        :param data: The input data to filter (can be a list, numpy array, or other types)
        :return: A new list with the specified elements removed
        """
        if isinstance(data, list):
            filtered_data = []
            for item in data:
                filtered_item = self.remove_list_params(item)
                if isinstance(filtered_item, list):
                    filtered_data.extend(filtered_item)
                elif isinstance(filtered_item, np.ndarray):
                    filtered_data.append(filtered_item)
                elif filtered_item not in ('(', ')'):
                    filtered_data.append(filtered_item)
            return filtered_data
        elif isinstance(data, np.ndarray):
            return np.array([self.remove_list_params(item) for item in data if item not in ('(', ')')])
        else:
            return data

    def evaluate_case_statement(self, expression_list):
        """
        Evaluates a case statement represented by a list and returns a numpy array
        with the results of the comparison.

        Args:
            expression_list (list): The list representing the case statement.
        Returns:
            numpy.ndarray: The resulting values of the comparison.
        """
        # Clean the input list to remove newlines and commas
        cleaned_expression_list = [expr for expr in expression_list if expr not in ('\n', ',')]

        # Initialize the result array with None values
        result = np.full(self.main_df.shape[0], 0, dtype=object)

        def evaluate_condition(_condition):
            column_name, operator, comparison_value = _condition
            if column_name not in self.main_df.columns:
                raise ValueError(f"[x] Column '{column_name}' does not exist in the DataFrame.")
            column_data = self.main_df[column_name].values
            if operator == '==':
                return column_data == comparison_value
            elif operator == '!=':
                return column_data != comparison_value
            elif operator == '>':
                return column_data > comparison_value
            elif operator == '<':
                return column_data < comparison_value
            elif operator == '>=':
                return column_data >= comparison_value
            elif operator == '<=':
                return column_data <= comparison_value
            else:
                raise ValueError(f"[x] Invalid operator '{operator}' in the case statement.")

        i = 0
        while i < len(cleaned_expression_list) - 1:
            condition = cleaned_expression_list[i:i + 3]
            case_result = cleaned_expression_list[i + 3]
            mask = evaluate_condition(condition)

            # Update the result array where the condition is true and the result is still None
            result = np.where(np.logical_and(mask, result == ''), case_result, result)
            i += 4

        # Handle the else part (catch-all)
        if i == len(cleaned_expression_list) - 1:
            case_result = cleaned_expression_list[i]
            result = np.where(result == '', case_result, result)

        return result

    @staticmethod
    def extract_operator(_cleaned_expression):
        operator = None
        for entry in _cleaned_expression:
            if not isinstance(entry, (np.ndarray, list, tuple)):
                if entry in ('AND', 'OR'):
                    operator = 'logicalExpr'
                elif entry in ('>', '<', '>=', '<=', '~', '!=', '=='):
                    operator = 'comparisonExpr'
        return operator

    def evaluate_if_then_clause(self, expression_list):
        """
        Evaluates an if-then clause represented by a list and returns a numpy array
        with the results of the comparison.

        Args:
            expression_list (list): The list representing the if-then clause.
        Returns:
            numpy.ndarray: The resulting values of the comparison.
        """
        if len(expression_list) != 7 or expression_list[3] != ',' or expression_list[5] != ',':
            raise ValueError("[x] Invalid if-then clause format.")

        column_name, operator, comparison_value, _, true_value, _, false_value = expression_list

        if column_name not in self.main_df.columns:
            raise ValueError(f"[x] Column '{column_name}' does not exist in the DataFrame.")

        column_data = self.main_df[column_name].values

        if operator == '==':
            result = np.where(column_data == comparison_value, true_value, false_value)
        elif operator == '!=':
            result = np.where(column_data != comparison_value, true_value, false_value)
        elif operator == '>':
            result = np.where(column_data > comparison_value, true_value, false_value)
        elif operator == '<':
            result = np.where(column_data < comparison_value, true_value, false_value)
        elif operator == '>=':
            result = np.where(column_data >= comparison_value, true_value, false_value)
        elif operator == '<=':
            result = np.where(column_data <= comparison_value, true_value, false_value)
        else:
            raise ValueError(f"[x] Invalid operator '{operator}' in the if-then clause.")

        return result

    def replace_column_names_with_values(self, columns):
        if not isinstance(columns, list):
            logging.error("[x] The input should be a list of strings.")
            return

        for i, column in enumerate(columns):
            if not isinstance(column, str):
                logging.error(f"[x] The column name at index {i} is not a string: {column}")
                continue

            if column in self.main_df.columns:
                logging.info(f"[i] Replacing column name '{column}' with its values.")
                columns[i] = self.main_df[column].to_numpy()
            else:
                logging.warning(f"[!] Column name '{column}' not found in the dataframe.")

        return columns

    # CRITICAL COMPONENT
    def perform_math_operation_and_update_df_with_result(self, cleaned_expression, operation_type):
        key = cleaned_expression.copy()
        for index, x in enumerate(cleaned_expression):
            if isinstance(x, str):
                if x in self.main_df.columns:
                    cleaned_expression[index] = self.get_column_entries(x)
                elif x in ('+', '-', '*', '/', '(', '[', ']', ')', '%'):
                    pass  # Pass all expected operators and have a catch-all for those that miss both.
                else:
                    self.generic_processing_exit('exitAdditiveExpr:validate_exceptions',
                                                 f'Variable {x} does not exist.')
            elif isinstance(x, (int, float)):
                cleaned_expression[index] = self.create_ordered_array(x)

        # CREATE data and route it based on whether variable is in self.main_df or not...
        out = self.math_handler.complex_math_operation(cleaned_expression, operation_type, self.main_df.shape[0])

        # Update or create the target column OR store the value if column is taken already...
        self.place_in_df_or_temp_storage(key, out)

    # CRITICAL COMPONENT
    def place_in_df_or_temp_storage(self, _key, _out):
        if self.general_handler.check_empty(_out):
            return
        if self.current_variable in self.main_df.columns:  # Hold RESULTING VALUES, not expressions along with key.

            if self.general_handler.contains_sublist(_key):
                self.temporary_storage.append((self.current_variable, _out))
                logging.info(f"[i] Stored values in temp storage under key '{self.current_variable}'.")
            else:
                self.temporary_storage.append((_key, _out))
                logging.info(f"[i] Stored values in temp storage under key '{_key}'.")

            # self.main_df[self.current_variable] = _out

        else:
            self.main_df[self.current_variable] = _out.tolist()  # Update main_df with RESULTING VALUES
            logging.info(f"[i] Updated DataFrame with new/updated column '{_key}'.")

            # Add variable name tied to original expression for future EXPRESSION replacements
            target = (_key, self.current_variable)
            if target not in self.variables:
                self.variables.append(target)

    # CRITICAL COMPONENT
    def replace_variables(self, expression_list):
        """Replace parts of the expression list with corresponding variables
        from self.variables and self.temporary_storage."""
        changes_were_made = False
        if not self.variables and not self.temporary_storage:
            return expression_list, False

        def replace_in_expression(self, expr_list, replacements):
            """
            Replace parts of the expression list with corresponding variables from replacements.

            Args:
                expr_list (list): The original expression list.
                replacements (list): List of tuples representing replacements (expression, variable).

            Returns:
                list: The modified expression list.
                bool: Flag indicating if a replacement occurred.
            """
            i = 0
            changes_made = False
            while i < len(expr_list):
                replaced = False
                for expr, var in replacements:
                    expr_len = len(expr)
                    if expr_list[i:i + expr_len] == expr:
                        expr_list = expr_list[:i] + [var] + expr_list[i + expr_len:]
                        replaced = True
                        changes_made = True
                        break
                if replaced:
                    # Restart from the beginning after a replacement to ensure all are caught
                    i = 0
                else:
                    i += 1
            return self.remove_list_params(expr_list), changes_made

        def lists_are_equal(list1, list2):
            if list1 is None or list2 is None:
                return False
            if len(list1) != len(list2):
                return False
            for item1, item2 in zip(list1, list2):
                if isinstance(item1, np.ndarray) or isinstance(item2, np.ndarray):
                    if isinstance(item1, np.ndarray) and isinstance(item2, np.ndarray):
                        if not np.array_equal(item1, item2):
                            return False
                    else:
                        return False
                elif item1 != item2:
                    return False
            return True

        previous_expression = []
        while not lists_are_equal(expression_list, previous_expression):
            previous_expression = expression_list[:]

            # Combine self.variables and self.temporary_storage
            combined_replacements = self.variables + self.temporary_storage

            # Replace variables in the expression list using the combined replacements
            expression_list, changes_were_made = replace_in_expression(expression_list, combined_replacements)

        logging.info("[i] Finished replacing variables in the expression list.")
        return expression_list, changes_were_made

    # CRITICAL COMPONENT
    def update_temporary_storage(self, _cleaned_expression):
        if self.temporary_storage:
            if isinstance(self.temporary_storage[-1][-1], list):
                self.main_df[self.current_variable] = self.temporary_storage[-1][-1]
            else:
                if isinstance(self.temporary_storage[-1][-1], str):
                    self.main_df[self.current_variable] = self.temporary_storage[-1][-1]
                else:
                    self.main_df[self.current_variable] = self.temporary_storage[-1][-1].tolist()

            self.temporary_storage = []
            self.variables = []  # Clear expression holding area after each exit
        else:
            target_var, value = self.general_handler.return_equal_components_from_list(_cleaned_expression)
            # If there is only one object/item/variable on the right side of the equal sign
            if len(value) == 1:
                if isinstance(value[0], (int, float)):
                    self.main_df[target_var] = np.repeat(value, len(self.main_df))
                elif isinstance(value[0], str):
                    # If set to a variable that exists in the main dataframe already
                    if value[0] in self.main_df.columns:  # Set new variable equal to current var column
                        self.main_df[target_var] = self.main_df[value[0]]
                    else:  # Otherwise, do a repeat on the string and add to self.main_df
                        self.main_df[target_var] = np.repeat(value, len(self.main_df))
            else:
                # I'm sure we will land here, and something different will need to be done. STUB Placeholder
                pass

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

        resolved_nodes = self.resolve_terminal_nodes(ctx_obj)
        cleaned_expression = self.general_handler.convert_nested_list(resolved_nodes)

        # EXIT Directive Cleanup
        if obj_identifier == 'exitDirective':
            # EVAL Handling
            if self.current_directive == 'eval':
                if self.single_existing_variable_eval:
                    self.single_existing_variable_eval = False
                    if self.temporary_storage and not self.temporary_storage[-1][-1]:
                        self.temporary_storage = self.temporary_storage[:-1]
                    return

                self.update_temporary_storage(cleaned_expression)

            # SEARCH and WHERE Directive Handling
            elif self.current_directive in ('search', 'where'):
                cleaned_expression, flag = self.replace_variables(cleaned_expression)
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                if cleaned_expression and cleaned_expression[0] in ('search', 'where'):
                    cleaned_expression = cleaned_expression[1:]
                self.main_df = self.process_search_directive(cleaned_expression)

            # MVEXPAND Directive Handling
            elif self.current_directive == 'mvexpand':
                # Step 1: Convert the string representation of lists to actual Python lists
                self.main_df['header_0_1'] = self.main_df['header_0_1'].apply(self.string_handler.convert_to_list)

                # Step 2: Explode the DataFrame on 'header_0_1'
                exploded_df = self.main_df.explode('header_0_1')

                # Step 3: Reconstruct the DataFrame by resetting the index and aligning all columns
                self.main_df = exploded_df.reset_index(drop=True)

        # EXPRESSION Handling
        elif obj_identifier == 'exitExpression':
            if self.prefilter_enabled:
                return
            flag, key, operator = True, cleaned_expression.copy(), None
            cleaned_expression, flag = self.replace_variables(cleaned_expression)
            while flag:
                cleaned_expression, flag = self.replace_variables(cleaned_expression)
            operator = self.extract_operator(cleaned_expression)
            out = self.evaluate_expression(operator, cleaned_expression)
            self.place_in_df_or_temp_storage(key, out)

        # LOGICAL Expression Handling
        elif obj_identifier == 'exitlogicalExpr':
            if self.prefilter_enabled:
                return
            if any(op in cleaned_expression for op in self.logical_operators):
                flag, key = True, cleaned_expression.copy()
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                operator = self.extract_operator(cleaned_expression)
                out = self.evaluate_logical_expr(operator, cleaned_expression)
                self.place_in_df_or_temp_storage(key, out)

        # COMPARISON Expression Handling
        elif obj_identifier == 'exitComparisonExpr':
            if self.prefilter_enabled:
                return
            if any(op in cleaned_expression for op in ('(', ')')):
                return
            if any(op in cleaned_expression for op in self.comparison_operators):
                flag, key = True, cleaned_expression.copy()
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                out = self.evaluate_comparison_expr(cleaned_expression)
                self.place_in_df_or_temp_storage(key, out)

        # MULTIPLICATIVE Expression Cleanup
        elif obj_identifier == 'exitMultiplicativeExpr':
            if self.contains_operator(cleaned_expression, ['*', '/']):
                flag = True
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                if self.contains_operator(cleaned_expression, ['*', '/']):
                    self.exit_if_contains_any_unexpected_operations(cleaned_expression, obj_identifier)
                    self.perform_math_operation_and_update_df_with_result(cleaned_expression, 'mult_div')
                else:
                    pass  # No further action needed

        # ADDITIVE Expression Cleanup
        elif obj_identifier == 'exitAdditiveExpr':
            if self.contains_operator(cleaned_expression, ['+', '-']):
                flag = True
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                if self.contains_operator(cleaned_expression, ['+', '-']):
                    self.exit_if_contains_any_unexpected_operations(cleaned_expression, obj_identifier)
                    self.perform_math_operation_and_update_df_with_result(cleaned_expression, 'add_sub')
                else:
                    pass  # No further action needed

        # UNARY Expression Handling
        elif obj_identifier == 'exitUnaryExpr':
            key = cleaned_expression.copy()
            if self.general_handler.compare_two_lists(['--', '++', '~'], cleaned_expression):
                for index, entry in enumerate(cleaned_expression):
                    if entry not in ('--', '++', '~'):
                        if entry in self.main_df.columns:
                            cleaned_expression[index] = self.get_column_entries(entry)
                        else:
                            self.generic_processing_exit('exitUnaryExpr:validate_exceptions()',
                                                         f'Variable "{entry}" not found in DataFrame.')
                self.place_in_df_or_temp_storage(key, self.math_handler.apply_unary_operation(cleaned_expression))

        # IF Statement Handling
        elif obj_identifier == 'exitIfExpression':
            key = cleaned_expression.copy()
            parsed_if_expression = self.general_handler.extract_outermost_parentheses(cleaned_expression)
            out = self.evaluate_if_then_clause(parsed_if_expression)
            self.place_in_df_or_temp_storage(key, out)

        # CASE Statement Handling
        elif obj_identifier == 'exitCaseExpression':
            key = cleaned_expression.copy()
            parsed_case_expression = self.general_handler.extract_outermost_parentheses(cleaned_expression)
            out = self.evaluate_case_statement(parsed_case_expression)
            self.place_in_df_or_temp_storage(key, out)

        # STRING FUNCTION Handling
        elif obj_identifier == 'exitStringFunctionCall':
            self.current_string_function = cleaned_expression[0]
            if not self.current_string_function or self.current_string_function not in self.all_string_funcs:
                self.generic_processing_exit('exitStringFunctionCall',
                                             f'Attempted string function "{self.current_string_function}" is unknown.')

            # Standard key and parameter extraction for all string functions
            flag, key = True, cleaned_expression.copy()
            parameters = self.extract_parameters(cleaned_expression)
            while flag:
                cleaned_expression, flag = self.replace_variables(parameters)
            self.replace_column_names_with_values(cleaned_expression)

            # CONCAT Handling
            if self.current_string_function == 'concat':
                cleaned_expression.append(self.main_df.shape[0])
                for index, potential_ndarray in enumerate(cleaned_expression):
                    if isinstance(potential_ndarray, np.ndarray):
                        cleaned_expression[index] = self.string_handler.try_ast_conversion(potential_ndarray)

                out = self.string_handler.full_concat(cleaned_expression)
                self.place_in_df_or_temp_storage(key, out)

            # REPLACE Handling
            elif self.current_string_function == 'replace':
                if len(cleaned_expression) == 3:
                    target, string_to_replace, replace_with = cleaned_expression
                    out = self.string_handler.complex_replace(target, string_to_replace, replace_with)
                    self.place_in_df_or_temp_storage(key, out)

            # UPPER Handling
            elif self.current_string_function == 'upper':
                out = self.string_handler.transform_strings(cleaned_expression, 'upper')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # LOWER Handling
            elif self.current_string_function == 'lower':
                out = self.string_handler.transform_strings(cleaned_expression, 'lower')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # CAPITALIZE Handling
            elif self.current_string_function == 'capitalize':
                out = self.string_handler.transform_strings(cleaned_expression, 'capitalize')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # LEN Handling
            elif self.current_string_function == 'len':
                out = self.string_handler.transform_strings(cleaned_expression, 'len')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # TOSTRING Handling
            elif self.current_string_function == 'tostring':
                out = self.string_handler.transform_strings(cleaned_expression, 'tostring')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # URLENCODE Handling
            elif self.current_string_function == 'urlencode':
                out = self.string_handler.transform_strings(cleaned_expression, 'urlencode')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # URLDECODE Handling
            elif self.current_string_function == 'urldecode':
                out = self.string_handler.transform_strings(cleaned_expression, 'urldecode')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # DEFANG Handling
            elif self.current_string_function == 'defang':
                out = self.string_handler.transform_strings(cleaned_expression, 'defang')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # FANG Handling
            elif self.current_string_function == 'fang':
                out = self.string_handler.transform_strings(cleaned_expression, 'fang')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # TRIM Handling
            elif self.current_string_function == 'trim':
                out = self.string_handler.trim_operation(cleaned_expression)
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # RTRIM Handling
            elif self.current_string_function == 'rtrim':
                out = self.string_handler.trim_operation(cleaned_expression, 'rtrim')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # LTRIM Handling
            elif self.current_string_function == 'ltrim':
                out = self.string_handler.trim_operation(cleaned_expression, 'ltrim')
                if isinstance(out, list) and len(out) == 1:
                    out = out[-1]
                self.place_in_df_or_temp_storage(key, out)

            # SUBSTR Handling
            elif self.current_string_function == 'substr':
                # Implement SUBSTR functionality as needed
                pass

            # FINAL UPDATE OF TEMPORARY HOLDING AREA
            self.update_temporary_storage(cleaned_expression)

        # Handle other obj_identifiers if necessary
        elif obj_identifier == 'exitSomeOtherMethod':
            # Implement handling for other methods as needed
            pass

        else:
            # Handle unexpected obj_identifier or no action required
            pass

    def build_filters(self):
        """
        Builds a combined PyArrow Expression from epoch and PyArrow filters.

        Returns:
            Optional[Expression]: The combined PyArrow Expression or None if no filters are present.
        """
        filters = []

        # Convert epoch filters to PyArrow Expressions
        if self.prefilters.epoch_filter and '_epoch' in self.prefilters.epoch_filter:
            epoch_conditions = []
            if '$gte' in self.prefilters.epoch_filter['_epoch']:
                epoch_conditions.append(ds.field('_epoch') >= self.prefilters.epoch_filter['_epoch']['$gte'])
            if '$lte' in self.prefilters.epoch_filter['_epoch']:
                epoch_conditions.append(ds.field('_epoch') <= self.prefilters.epoch_filter['_epoch']['$lte'])
            if epoch_conditions:
                epoch_expression = epoch_conditions[0]
                for cond in epoch_conditions[1:]:
                    epoch_expression &= cond
                filters.append(epoch_expression)

        # Convert PyArrow filter string to PyArrow Expression using Lark parser
        if self.prefilters.pyarrow_filter:
            try:
                parsed_filter = self.filter_parser.parse(self.prefilters.pyarrow_filter)
                filters.append(parsed_filter)
            except Exception as e:
                logging.error(f"Error parsing PyArrow filter string with Lark: {e}")
                return None

        if filters:
            combined = filters[0]
            for f in filters[1:]:
                combined &= f
            return combined
        return None

    @staticmethod
    def flatten_ctx(ctx):
        """
        Iteratively traverses the ANTLR4 parse tree context and concatenates
        all terminal node texts to produce the flattened query string.

        Args:
            ctx (ParseTree): The root context of the parse tree.

        Returns:
            str: The concatenated string of all terminal node texts.
        """
        texts = []
        stack = [ctx]

        while stack:
            current = stack.pop()
            if isinstance(current, antlr4.tree.Tree.TerminalNodeImpl):
                texts.append(str(current))
            elif hasattr(current, 'children') and current.children:
                stack.extend(current.children[::-1])

        return ' '.join(texts).replace('  ', ' ')

    def apply_combined_filter(self, dataset_paths, combined_filter):
        """
        Applies the combined filter to multiple PyArrow datasets efficiently.

        Args:
            dataset_paths (List[str]): List of paths to the PyArrow datasets (e.g., Parquet files).
            combined_filter (Optional[Expression]): The combined PyArrow filter expression.

        Returns:
            Optional[pa.Table]: The concatenated filtered PyArrow Table if successful, otherwise None.
        """
        try:
            dataset_paths = [os.path.abspath(os.path.join('../', self.target_index_uri, x)) for x in dataset_paths]
            allowed_base = os.path.abspath(self.target_index_uri)

            for dataset_path in dataset_paths:
                if not os.path.commonpath([dataset_path, allowed_base]) == allowed_base:
                    self.generic_processing_exit('enterPreFilter:apply_combined_filter',
                                                 'Target paths are not in the safe location.')

            dataset = ds.dataset(dataset_paths, format="system4.system4.parquet")
            filtered_table = dataset.to_table(filter=combined_filter) if combined_filter else dataset.to_table()
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
    def _clean_pyarrow_filter(filter_str: str) -> str:
        """
        Cleans up the PyArrow filter string by removing all parentheses and trimming whitespace.

        Args:
            filter_str (str): The raw filter string.

        Returns:
            str: The cleaned filter string without parentheses.
        """
        # Remove all parentheses
        filter_str = re.sub(r'[()]', '', filter_str)
        # Strip leading and trailing whitespace
        return filter_str.strip()


del speakQueryParser

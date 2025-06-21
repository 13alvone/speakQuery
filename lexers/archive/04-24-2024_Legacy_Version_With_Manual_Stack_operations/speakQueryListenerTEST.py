#!/usr/bin/env python3
import logging
import importlib
import numpy
import glob
import time
import os
from collections import OrderedDict
import r_datetime_parser

import antlr4.tree.Tree
import jaydebeapi
from antlr4 import *
import numpy as np
import pandas

from lexers.archive.speakQueryParser import speakQueryParser as SQP
from handlers.archive.ParquetHandler import ParquetHandler
from handlers.StringHandler import StringHandler
from handlers.archive.TimeHandler import TimeHandler
from handlers.MathematicOperations import MathHandler
from handlers.GeneralHandler import GeneralHandler
from handlers.archive.StackHandler import StackHandler
from handlers.LookupHandler import LookupHandler

from functionality.archive.call_stats import CallStats

logging.basicConfig(level=logging.INFO, format='%(message)s')

if "." in __name__:
    from lexers.archive.speakQueryParser import speakQueryParser
else:
    from lexers.archive.speakQueryParser import speakQueryParser


# This class defines a complete listener for a parse tree produced by speakQueryParser.
class speakQueryListener(ParseTreeListener):
    def __init__(self, original_query):
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
        self.original_query = original_query.strip('\n').strip(' ').strip('\n').strip(' ')
        self.script_directory = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.lookup_path = f'{self.script_directory}/../lookups'
        self.jdbc_jar_path = f'{self.script_directory}/../apache_drill/apache-drill-1.21.1/jars/jdbc-driver'
        self.target_index_uri = 'indexes/test_parquets'
        self.current_directive = None
        self.parquet_handler = ParquetHandler()
        self.string_handler = StringHandler()
        self.math_handler = MathHandler()
        self.time_handler = TimeHandler()
        self.general_handler = GeneralHandler()
        self.stack_handler = StackHandler()
        self.lookup_handler = LookupHandler()
        self.comments = OrderedDict()
        self.literal_names = []
        self.main_df = None
        self.table_cmd = None
        self.current_value = None
        self.contextStack = []
        self.expression_string = None
        self.resolved_lines = []
        self.first_directive = True
        self.type_check = importlib.import_module('speakQueryParser')
        self.directive = None
        self.current_item = None
        self.entries = []
        self.values = OrderedDict()
        self.concat_children = None
        self.ctx_obj_str = None
        self.last_entry = ''
        self.original_query = original_query.strip().lstrip('\n')
        self.known_directives = [
            'file', 'eval', 'limit', 'streamstats', 'eventstats', 'stats', 'where', 'rename', 'fields', 'search',
            'reverse', 'head', 'rex', 'regex', 'dedup', 'maketable', 'timechart', 'lookup', 'inputlookup', 'fillnull',
            'outputlookup', 'fieldsummary', 'special_func', 'spath', 'base64', 'loadjob', 'join', 'appendpipe', 'bin',
            'sort', 'to_epoch']
        self.all_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'concat',
            'replace', 'upper', 'lower', 'capitalize', 'trim', 'ltrim', 'rtrim', 'substr', 'len', 'tostring', 'match',
            'urlencode', 'defang', 'type', 'timerange']
        self.all_numeric_funcs = [
            'round', 'min', 'max', 'avg', 'sum', 'range', 'median', 'sqrt', 'random', 'tonumb', 'dcount', 'len']
        self.all_string_funcs = [
            "concat", "replace", "upper", "lower", "capitalize", "trim", "rtrim", "ltrim", "substr", "len",
            "tostring", "match", "urlencode", "urldecode", "defang", "type"]

    def enterProgram(self, ctx: speakQueryParser.ProgramContext):
        self.validate_exceptions(ctx, 'enterProgram')

    def exitProgram(self, ctx: speakQueryParser.ProgramContext):
        # if not self.is_loadjob:
        #     self.parquet_handler.save_dataframe_to_parquet(self.main_df)

        logging.info(f'[+] Successfully processed valid speakQuery syntax and collapsed expressions.\n')
        logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n'
                     f'[+] RESULT:\n{self.main_df}\n')
        logging.info(f'[+] Transposed Summary of Fields: (First 2 row entries shown from each column)'
                     f'{self.general_handler.get_main_df_overview(self.main_df)}')
        logging.info("[i] Parsing process completed.")
        logging.info("[i] Duration: {:.2f} seconds".format(time.time() - self.start_time))
        exit(0)

    def enterSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        self.validate_exceptions(ctx, 'enterSpeakQuery')

    def exitSpeakQuery(self, ctx: SQP.speakQuery):
        self.validate_exceptions(ctx, 'exitTradeQuery')

    def enterTableCall(self, ctx: SQP.TableCallContext):
        self.validate_exceptions(ctx, 'enterTableCall')

    def exitTableCall(self, ctx: SQP.TableCallContext):
        last_val = self.resolve_terminal_nodes(ctx)
        if isinstance(last_val, list):
            self.table_cmd = self.general_handler.remove_outer_newlines([str(x) for x in last_val])
            if len(self.table_cmd) > 1:
                self.drill_sql_cmd = self.table_cmd[-1].strip('"')
        if self.values:
            self.validate_exceptions(ctx, obj_identifier='exitTableCall')

        if self.table_cmd:
            self.table_cmd = [str(x) for x in self.general_handler.remove_outer_newlines(self.table_cmd)]
        elif not str(last_val).strip('').startswith('|') and ctx.children:
            self.table_cmd = self.general_handler.remove_outer_newlines([str(x) for x in ctx.children])

        if self.table_cmd[1] == 'inputlookup':
            tbl_cmd_len = len(self.table_cmd)
            if tbl_cmd_len < 3:
                self.generic_processing_exit('exitTableCall',
                                             f'[x] INPUTLOOKUP failed. Expected 4 args, got {tbl_cmd_len}')
            elif tbl_cmd_len >= 3:
                self.current_inputlookup = [str(x) for x in self.table_cmd if '.csv' in str(x).lower()][0]
                self.main_df = self.lookup_handler.load_data(f'{self.lookup_path}/{self.current_inputlookup}')

            if tbl_cmd_len > 3:
                self.main_df = self.add_epoch_time_column(self.main_df, self.table_cmd[-1])
                date_list = self.get_column_entries(str(self.table_cmd[-1]))
                self.add_or_update_column('_epoch', r_datetime_parser.parse_dates_to_epoch(date_list))
            return

        if self.table_cmd[1] == 'loadjob':
            self.is_loadjob = True
            if len(self.table_cmd) == 3:
                self.main_df = self.parquet_handler.read_parquet_file(
                    f'{self.parquet_handler.jobs_path}/{self.table_cmd[-1]}.parquet')
            else:
                self.generic_processing_exit('exitTableCall', f'[!] LOADJOB command malformed. '
                                                              f'Need 3 entries, {len(self.table_cmd)} provided.')
            return

        self.table_cmd = self.process_table_cmd(self.table_cmd)
        target_tables = [x.split('/')[-1] for x in self.target_tables]
        target_tables_len = len(target_tables)

        if not self.drill_sql_cmd:
            self.drill_sql_cmd = f'select * from TARGET_FILES'

        if target_tables_len == 1:
            self.target_tables = f'dfs.`{self.target_tables[0]}`'
            self.drill_sql_cmd = self.drill_sql_cmd.replace('TARGET_FILES', self.target_tables).strip(' ')
            self.execute_drill_query(self.drill_sql_cmd, self.jdbc_jar_path)

        elif target_tables_len > 1:
            self.target_tables = f'dfs.`{self.script_directory}/../{self.target_index_uri}/' + '{'
            for table_name in target_tables:
                self.target_tables += f'{table_name},'
            self.target_tables = f"{self.target_tables.rstrip(',')}" + '}/`'

            if self.target_tables:
                if isinstance(self.target_tables, str):
                    self.drill_sql_cmd = self.drill_sql_cmd.replace('TARGET_FILES', self.target_tables).strip(' ')
            else:
                self.generic_processing_exit('exitTableCall', 'Missing Table Directive or Table Directive Malformed.')

            self.execute_drill_query(self.drill_sql_cmd, self.jdbc_jar_path)
        else:
            self.generic_processing_exit('exitTableCall', 'No tables actually found. Exiting with failure to load...')

        self.main_df = self.add_epoch_time_column(self.main_df, 'TIMESTAMP')

    def enterSqlDrillQuery(self, ctx: speakQueryParser.SqlDrillQueryContext):
        self.validate_exceptions(ctx, 'enterSqlDrillQuery')

    def exitSqlDrillQuery(self, ctx: speakQueryParser.SqlDrillQueryContext):
        self.validate_exceptions(ctx, 'exitSqlDrillQuery')

    def enterTableName(self, ctx: speakQueryParser.TableNameContext):
        self.validate_exceptions(ctx, 'enterTableName')

    def exitTableName(self, ctx: speakQueryParser.TableNameContext):
        self.validate_exceptions(ctx, 'exitTableName')

    def enterTimerangeCall(self, ctx: SQP.TimerangeCallContext):
        self.validate_exceptions(ctx, 'enterTimerangeCall')

    def exitTimerangeCall(self, ctx: SQP.TimerangeCallContext):
        # last_key, last_val = next(reversed(self.values.items()))
        last_val = self.resolve_terminal_nodes(ctx)
        self.validate_exceptions(ctx, 'exitTimerangeCall')
        last_val = self.general_handler.remove_outer_newlines(self.general_handler.convert_nested_list(last_val))
        parameters = self.extract_parameters(last_val)
        if isinstance(parameters, list):
            if len(parameters) == 3:
                if parameters[-1] in self.main_df.columns:
                    # UPDATE: We need a way to ref this without loading it. Potentially a RUST binary here to
                    # speed this up for very very very large files.
                    parameters[-1] = self.get_column_entries(parameters[-1])
                else:
                    self.generic_processing_exit('exitTimerangeCall',
                                                 f'Time variable {parameters[-1]} not in self.main_df when filtering.')
        # Here is another opportunity to create a RUST component that filters epoch by entries.
        time_filtered_parameters = self.time_handler.filter_dates(parameters)
        self.main_df = self.general_handler.filter_df_with_dates(self.main_df, time_filtered_parameters)

    def enterValidLine(self, ctx: SQP.ValidLineContext):
        self.validate_exceptions(ctx, 'enterValidLine')

    def exitValidLine(self, ctx: speakQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'exitValidLine')

    def enterDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.current_directive = f'{ctx.children[0]}'
        self.validate_exceptions(ctx, 'enterDirective')

    def exitDirective(self, ctx: speakQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'exitDirective')
        last_val = self.resolve_terminal_nodes(ctx)

        # Handling of the last_val based on whether it's the first directive or not
        if isinstance(last_val, list):
            if self.general_handler.contains_sublist(last_val):
                if isinstance(last_val[1], list):
                    if str(last_val[1][0]) not in ('stats', 'eventstats'):
                        last_val = self.stack_handler.trim_list_to_last_sublist(last_val)[-1]
                else:
                    last_val = self.stack_handler.trim_list_to_last_sublist(last_val)[-1]
        elif isinstance(last_val, OrderedDict):
            if len(last_val) == 1:
                last_val = last_val['final']
        else:
            pass  # This section may still require an action, I'm just not sure what edge case is for it yet...

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

        # ALL EXECUTION ARMS based on the self.current_directive
        # SEARCH / WHERE Clause Handling
        if self.current_directive in ('search', 'where'):  # If SEARCH is the LAST clause in Query.
            if 'final' in self.values.keys():
                # last_key, last_val = 'final', self.values['final'][-1]
                last_val = self.resolve_terminal_nodes(ctx)
                if isinstance(last_val, list):
                    if len(last_val) == 3:
                        if isinstance(last_val[1], list):
                            if len(last_val[1]) == 2:
                                logical_expression = last_val[1][-1]
                                self.main_df = self.general_handler.filter_df_with_logical_expression(
                                    self.main_df, expression=logical_expression)
                            else:
                                self.generic_processing_exit('exitDirective', f'Search malformed. "{last_val}"')
                        else:
                            self.generic_processing_exit('exitDirective', f'Search value is malformed. "{last_val}"')
                    else:
                        self.generic_processing_exit('exitDirective', f'Search value is malformed. "{last_val}"')
                else:
                    self.generic_processing_exit('exitDirective', f'Search value is malformed. "{last_val}"')
            else:  # If SEARCH / WHERE is NOT the last clause in valid Query
                # last_key, last_val = next(reversed(self.values.items()))
                last_val = self.resolve_terminal_nodes(ctx)
                last_val = self.general_handler.trim_to_last_sublist(last_val)
                if len(last_val) >= 1:
                    if isinstance(last_val[-1], list):
                        if len(last_val[-1]) >= 2:
                            if len(last_val[-1][1]) >= 2:
                                if isinstance(last_val[-1][1][1], list):
                                    logical_expression = last_val[-1][1][1]
                                    self.main_df = self.general_handler.filter_df_with_logical_expression(
                                        self.main_df, expression=logical_expression)
                                    y = 'test'
                                else:
                                    self.generic_processing_exit('exitDirective',
                                                                 "Where/Search logical expression is not a list.")
                            else:
                                self.generic_processing_exit('exitDirective',
                                                             "Where/Search logical expression is malformed")
                        else:
                            self.generic_processing_exit('exitDirective',
                                                         "Where/Search logical expression is malformed")
                    else:
                        self.generic_processing_exit('exitDirective',
                                                     "Where/Search logical expression is not a list.")
                else:
                    self.generic_processing_exit('exitDirective',
                                                 "Where/Search logical expression is malformed")

        # EVAL Clause Handling
        elif self.current_directive == 'eval':
            pass

        # STREAMSTATS Clause Handling
        elif self.current_directive == 'streamstats':
            pass

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
            pass

        # RENAME Clause Handling
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

        # FIELDS/MAKETABLE Clause Handling
        elif self.current_directive in ('fields', 'maketable'):
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

        # FIELDSUMMARY Clause Handling
        elif self.current_directive == 'fieldsummary':
            self.main_df = self.general_handler.execute_fieldsummary(self.main_df)

        # HEAD Clause Handling
        elif self.current_directive == 'head':
            if isinstance(last_val, list):
                if len(last_val) == 2:
                    self.main_df = self.general_handler.head_call(self.main_df, int(str(last_val[-1])), 'head')
                else:
                    self.generic_processing_exit('exitDirective', 'HEAD list must be of length 2.')
            else:
                self.generic_processing_exit('exitDirective', 'HEAD must accept a list, which was not provided.')

        # LIMIT Clause Handling
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

        # REVERSE Clause Handling
        elif self.current_directive == 'reverse':
            if isinstance(last_val, list):
                if len(last_val) >= 2:
                    self.main_df = self.general_handler.reverse_df_rows(self.main_df)
                    pass

        # DEDUP Clause Handling
        elif self.current_directive == 'dedup':
            if isinstance(last_val, list):
                if len(last_val) >= 2:
                    x = self.general_handler.convert_nested_list(directive_parts)
                    self.main_df = \
                        self.general_handler.execute_dedup(self.main_df,
                                                           self.general_handler.convert_nested_list(directive_parts))

        # SORT Clause Handling
        elif self.current_directive == 'sort':
            if last_val:
                if isinstance(last_val, list):
                    if len(last_val) >= 3:
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

        # REX Clause Handling
        elif self.current_directive == 'rex':
            self.main_df = self.general_handler.execute_rex(
                self.main_df, self.general_handler.convert_nested_list(last_val))

        # REGEX Clause Handling
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

        # BASE64 Clause Handling
        elif self.current_directive == 'base64':
            self.main_df = self.general_handler.handle_base64(
                self.main_df, self.general_handler.convert_nested_list(last_val))

        # SPECIAL_FUNC Clause Handling
        elif self.current_directive == 'special_func':
            pass

        # FILLNULL Clause Handling
        elif self.current_directive == 'fillnull':
            self.general_handler.execute_fillnull(self.main_df, self.general_handler.convert_nested_list(last_val))

        # TO_EPOCH Clause Handling
        elif self.current_directive == 'to_epoch':
            if str(last_val[-1]) == ')':
                # self.main_df = self.add_epoch_time_column(self.main_df, str(last_val[-2]))  # OLD, slower parser
                date_list = self.get_column_entries(str(last_val[-2]))
                self.add_or_update_column('_epoch', r_datetime_parser.parse_dates_to_epoch(date_list))

        # OUTPUTLOOKUP Clause Handling
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
            pass

        # JOIN Clause Handling
        elif self.current_directive == 'join':
            pass

        # APPENDPIPE Clause Handling
        elif self.current_directive == 'appendpipe':
            pass

        self.current_directive = None
        self.first_eval_variable = True

    def enterSharedField(self, ctx: speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx, 'enterSharedField')

    def exitSharedField(self, ctx: speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx, 'exitSharedField')

    def enterFilename(self, ctx: speakQueryParser.FilenameContext):
        self.validate_exceptions(ctx, 'enterFilename')

    def exitFilename(self, ctx: speakQueryParser.FilenameContext):
        self.validate_exceptions(ctx, 'exitFilename')

    def enterSpecialFunctionName(self, ctx: speakQueryParser.SpecialFunctionNameContext):
        self.validate_exceptions(ctx, 'enterSpecialFunctionName')

    def exitSpecialFunctionName(self, ctx: speakQueryParser.SpecialFunctionNameContext):
        self.validate_exceptions(ctx, 'exitSpecialFunctionName')

    def enterExpression(self, ctx: speakQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'enterExpression')

    def exitExpression(self, ctx: speakQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'exitExpression')
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass

    def enterLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'enterLogicalExpr')

    def exitLogicalExpr(self, ctx: speakQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'exitLogicalExpr')

    def enterComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx, 'enterComparisonExpr')

    def exitComparisonExpr(self, ctx: speakQueryParser.ComparisonExprContext):
        self.validate_exceptions(ctx, 'exitComparisonExpr')

    def enterComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'enterComparisonOperator')

    def exitComparisonOperator(self, ctx: speakQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'exitComparisonOperator')

    def enterAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        self.validate_exceptions(ctx, 'enterAdditiveExpr')

    def exitAdditiveExpr(self, ctx: speakQueryParser.AdditiveExprContext):
        self.validate_exceptions(ctx, 'exitAdditiveExpr')

    def enterMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx, 'enterMultiplicativeExpr')

    def exitMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx, 'exitMultiplicativeExpr')

    def enterPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx, 'enterPrimaryExpr')

    def exitPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx, 'exitPrimaryExpr')

    def enterBooleanExpr(self, ctx: speakQueryParser.BooleanExprContext):
        self.validate_exceptions(ctx, 'enterBooleanExpr')

    def exitBooleanExpr(self, ctx: speakQueryParser.BooleanExprContext):
        self.validate_exceptions(ctx, 'exitBooleanExpr')

    def enterIfExpression(self, ctx: speakQueryParser.IfExpressionContext):
        self.validate_exceptions(ctx, 'enterIfExpression')

    def exitIfExpression(self, ctx: speakQueryParser.IfExpressionContext):
        self.validate_exceptions(ctx, 'exitIfExpression')

    def enterCaseExpression(self, ctx: speakQueryParser.CaseExpressionContext):
        self.validate_exceptions(ctx, 'enterCaseExpression')

    def exitCaseExpression(self, ctx: speakQueryParser.CaseExpressionContext):
        self.validate_exceptions(ctx, 'exitCaseExpression')

    def enterCaseMatch(self, ctx: speakQueryParser.CaseMatchContext):
        self.validate_exceptions(ctx, 'enterCaseMatch')

    def exitCaseMatch(self, ctx: speakQueryParser.CaseMatchContext):
        self.validate_exceptions(ctx, 'exitCaseMatch')

    def enterCaseTrue(self, ctx: speakQueryParser.CaseTrueContext):
        self.validate_exceptions(ctx, 'enterCaseTrue')

    def exitCaseTrue(self, ctx: speakQueryParser.CaseTrueContext):
        self.validate_exceptions(ctx, 'exitCaseTrue')

    def enterInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx, 'enterInExpression')

    def exitInExpression(self, ctx: speakQueryParser.InExpressionContext):
        self.validate_exceptions(ctx, 'exitInExpression')

    def enterCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx, 'enterCatchAllExpression')

    def exitCatchAllExpression(self, ctx: speakQueryParser.CatchAllExpressionContext):
        self.validate_exceptions(ctx, 'exitCatchAllExpression')

    def enterUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'enterUnaryExpr')

    def exitUnaryExpr(self, ctx: speakQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'exitUnaryExpr')

    def enterUnaryPrefix(self, ctx: speakQueryParser.UnaryPrefixContext):
        self.validate_exceptions(ctx, 'enterUnaryPrefix')

    def exitUnaryPrefix(self, ctx: speakQueryParser.UnaryPrefixContext):
        self.validate_exceptions(ctx, 'exitUnaryPrefix')

    def enterUnaryPostfix(self, ctx: speakQueryParser.UnaryPostfixContext):
        self.validate_exceptions(ctx, 'enterUnaryPostfix')

    def exitUnaryPostfix(self, ctx: speakQueryParser.UnaryPostfixContext):
        self.validate_exceptions(ctx, 'exitUnaryPostfix')

    def enterStaticNumber(self, ctx: speakQueryParser.StaticNumberContext):
        self.validate_exceptions(ctx, 'enterStaticNumber')

    def exitStaticNumber(self, ctx: speakQueryParser.StaticNumberContext):
        self.validate_exceptions(ctx, 'exitStaticNumber')

    def enterStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx, 'enterStaticString')

    def exitStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx, 'exitStaticString')

    def enterFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'enterFunctionCall')

    def exitFunctionCall(self, ctx: speakQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'exitFunctionCall')

    def enterNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'enterNumericFunctionCall')

    def exitNumericFunctionCall(self, ctx: speakQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'exitNumericFunctionCall')

    def enterStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'enterStringFunctionCall')

    def exitStringFunctionCall(self, ctx: speakQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'exitStringFunctionCall')

    def enterSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'enterSpecificFunctionCall')

    def exitSpecificFunctionCall(self, ctx: speakQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'exitSpecificFunctionCall')

    def enterStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        self.validate_exceptions(ctx, 'enterStatsFunctionCall')

    def exitStatsFunctionCall(self, ctx: speakQueryParser.StatsFunctionCallContext):
        self.validate_exceptions(ctx, 'exitStatsFunctionCall')

    def enterStringFunctionTarget(self, ctx: speakQueryParser.StringFunctionTargetContext):
        self.validate_exceptions(ctx, 'enterStringFunctionTarget')

    def exitStringFunctionTarget(self, ctx: speakQueryParser.StringFunctionTargetContext):
        self.validate_exceptions(ctx, 'exitStringFunctionTarget')

    def enterHttpStringField(self, ctx: speakQueryParser.HttpStringFieldContext):
        self.validate_exceptions(ctx, 'enterHttpStringField')

    def exitHttpStringField(self, ctx: speakQueryParser.HttpStringFieldContext):
        self.validate_exceptions(ctx, 'exitHttpStringField')

    def enterMultivalueField(self, ctx: speakQueryParser.MultivalueFieldContext):
        self.validate_exceptions(ctx, 'enterMultivalueField')

    def exitMultivalueField(self, ctx: speakQueryParser.MultivalueFieldContext):
        self.validate_exceptions(ctx, 'exitMultivalueField')

    def enterMultivalueStringField(self, ctx: speakQueryParser.MultivalueStringFieldContext):
        self.validate_exceptions(ctx, 'enterMultivalueStringField')

    def exitMultivalueStringField(self, ctx: speakQueryParser.MultivalueStringFieldContext):
        self.validate_exceptions(ctx, 'exitMultivalueStringField')

    def enterMultivalueNumericField(self, ctx: speakQueryParser.MultivalueNumericFieldContext):
        self.validate_exceptions(ctx, 'enterMultivalueNumericField')

    def exitMultivalueNumericField(self, ctx: speakQueryParser.MultivalueNumericFieldContext):
        self.validate_exceptions(ctx, 'exitMultivalueNumericField')

    def enterStaticMultivalueStringField(self, ctx: speakQueryParser.StaticMultivalueStringFieldContext):
        self.validate_exceptions(ctx, 'enterStaticMultivalueStringField')

    def exitStaticMultivalueStringField(self, ctx: speakQueryParser.StaticMultivalueStringFieldContext):
        self.validate_exceptions(ctx, 'exitStaticMultivalueStringField')

    def enterStaticMultivalueNumericField(self, ctx: speakQueryParser.StaticMultivalueNumericFieldContext):
        self.validate_exceptions(ctx, 'enterStaticMultivalueNumericField')

    def exitStaticMultivalueNumericField(self, ctx: speakQueryParser.StaticMultivalueNumericFieldContext):
        self.validate_exceptions(ctx, 'exitStaticMultivalueNumericField')

    def enterRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        self.validate_exceptions(ctx, 'enterRegexTarget')

    def exitRegexTarget(self, ctx: speakQueryParser.RegexTargetContext):
        self.validate_exceptions(ctx, 'exitRegexTarget')

    def enterTrimTarget(self, ctx: speakQueryParser.TrimTargetContext):
        self.validate_exceptions(ctx, 'enterTrimTarget')

    def exitTrimTarget(self, ctx: speakQueryParser.TrimTargetContext):
        self.validate_exceptions(ctx, 'exitTrimTarget')

    def enterSubstrTarget(self, ctx: speakQueryParser.SubstrTargetContext):
        self.validate_exceptions(ctx, 'enterSubstrTarget')

    def exitSubstrTarget(self, ctx: speakQueryParser.SubstrTargetContext):
        self.validate_exceptions(ctx, 'exitSubstrTarget')

    def enterSubstrStart(self, ctx: speakQueryParser.SubstrStartContext):
        self.validate_exceptions(ctx, 'enterSubstrStart')

    def exitSubstrStart(self, ctx: speakQueryParser.SubstrStartContext):
        self.validate_exceptions(ctx, 'exitSubstrStart')

    def enterSubstrLength(self, ctx: speakQueryParser.SubstrLengthContext):
        self.validate_exceptions(ctx, 'enterSubstrLength')

    def exitSubstrLength(self, ctx: speakQueryParser.SubstrLengthContext):
        self.validate_exceptions(ctx, 'exitSubstrLength')

    def enterTrimRemovalTarget(self, ctx: speakQueryParser.TrimRemovalTargetContext):
        self.validate_exceptions(ctx, 'enterTrimRemovalTarget')

    def exitTrimRemovalTarget(self, ctx: speakQueryParser.TrimRemovalTargetContext):
        self.validate_exceptions(ctx, 'exitTrimRemovalTarget')

    def enterMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        self.validate_exceptions(ctx, 'enterMvfindObject')

    def exitMvfindObject(self, ctx: speakQueryParser.MvfindObjectContext):
        self.validate_exceptions(ctx, 'exitMvfindObject')

    def enterMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        self.validate_exceptions(ctx, 'enterMvindexIndex')

    def exitMvindexIndex(self, ctx: speakQueryParser.MvindexIndexContext):
        self.validate_exceptions(ctx, 'exitMvindexIndex')

    def enterMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        self.validate_exceptions(ctx, 'enterMvDelim')

    def exitMvDelim(self, ctx: speakQueryParser.MvDelimContext):
        self.validate_exceptions(ctx, 'exitMvDelim')

    def enterInputCron(self, ctx: speakQueryParser.InputCronContext):
        self.validate_exceptions(ctx, 'enterInputCron')

    def exitInputCron(self, ctx: speakQueryParser.InputCronContext):
        self.validate_exceptions(ctx, 'exitInputCron')

    def enterCronformat(self, ctx: speakQueryParser.CronformatContext):
        self.validate_exceptions(ctx, 'enterCronformat')

    def exitCronformat(self, ctx: speakQueryParser.CronformatContext):
        self.validate_exceptions(ctx, 'exitCronformat')

    def enterExecutionMaro(self, ctx: speakQueryParser.ExecutionMaroContext):
        self.validate_exceptions(ctx, 'enterExecutionMaro')

    def exitExecutionMaro(self, ctx: speakQueryParser.ExecutionMaroContext):
        self.validate_exceptions(ctx, 'exitExecutionMaro')

    def enterTimeStringValue(self, ctx: speakQueryParser.TimeStringValueContext):
        self.validate_exceptions(ctx, 'enterTimeStringValue')

    def exitTimeStringValue(self, ctx: speakQueryParser.TimeStringValueContext):
        self.validate_exceptions(ctx, 'exitTimeStringValue')

    def enterTimespan(self, ctx: speakQueryParser.TimespanContext):
        self.validate_exceptions(ctx, 'enterTimespan')

    def exitTimespan(self, ctx: speakQueryParser.TimespanContext):
        self.validate_exceptions(ctx, 'exitTimespan')

    def enterVariableName(self, ctx: speakQueryParser.VariableNameContext):
        if self.first_eval_variable:
            self.current_variable = self.general_handler.validate_ast(str(ctx.children[0]))
            self.first_eval_variable = False
        self.validate_exceptions(ctx, 'enterVariableName')

    def exitVariableName(self, ctx: speakQueryParser.VariableNameContext):
        self.validate_exceptions(ctx, 'enterVariableName')

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************
    def process_table_cmd(self, input_list):
        """
        Process the list by removing any leading entries equal to "" or "=",
        and then return all entries up to the end or an entry equal to "\n".

        :param input_list: List of strings to be processed.
        :return: A list of strings after processing.
        """
        input_list = [str(x) for x in input_list if str(x) not in ('\n', ',')]  # Step 1: Remove all newlines

        processed_list = []  # Step 2: Parse remaining entries
        is_file = False
        for index, entry in enumerate(input_list):
            test_entry = str(entry)
            processed_list.append(test_entry)

            if test_entry in ('file', 'FILE'):
                is_file = True
                continue

            if is_file and test_entry == '=':
                continue

            if self.drill_sql_cmd:
                if is_file and test_entry != '=' and index < len(input_list) - 1:
                    filename = test_entry.strip('"')
                    if filename:
                        start_path = f'{self.script_directory}/../{self.target_index_uri}'
                        if "*" in filename:
                            self.target_tables.extend(glob.glob(f'{start_path}/{filename}'))
                        else:
                            self.target_tables.append(f'{start_path}/{filename}')
            elif not self.drill_sql_cmd and is_file:
                if is_file and test_entry != '=' and index < len(input_list):
                    filename = test_entry.strip('"')
                    if filename:
                        start_path = f'{self.script_directory}/../{self.target_index_uri}'
                        if "*" in filename:
                            self.target_tables.extend(glob.glob(os.path.join(start_path, filename)))
                        else:
                            self.target_tables.append(f'{start_path}/{filename}.parquet')

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
        logging.error(f'[x] Failure at "{obj_failure}". {err_msg}')
        exit(1)

    def execute_drill_query(self, sql_query, jdbc_driver_dir):
        # Find all JAR files in the driver directory and its subdirectories (if any)
        jar_files = glob.glob(os.path.join(jdbc_driver_dir, '*.jar'))
        classpath = os.pathsep.join(jar_files)  # Join all JAR files into a single classpath string

        # JDBC connection details
        jdbc_driver = 'org.apache.drill.jdbc.Driver'
        jdbc_url = 'jdbc:drill:drillbit=localhost:31010'

        try:
            # Connect to Drill using the JDBC driver and the complete classpath
            with jaydebeapi.connect(jdbc_driver, jdbc_url, ['', ''], jars=classpath) as conn:
                with conn.cursor() as cursor:  # Create a cursor
                    cursor.execute(sql_query)  # Execute the query
                    columns = [desc[0] for desc in cursor.description]  # Fetch results & convert to pandas DataFrame
                    data = cursor.fetchall()
                    self.main_df = pandas.DataFrame(data, columns=columns)

        except jaydebeapi.DatabaseError as e:
            print(f"Database error: {e}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

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
            if pandas.isna(date_str) or date_str == "":
                return 0
            return self.time_handler.parse_input_date(date_str)

        # Apply the conversion function to each entry in target column
        df['_epoch'] = df[target_time_field].apply(parse_date_to_epoch)

        return df

    def add_or_update_column(self, field_name, values):
        """
        Adds or updates a column in self.main_df with the given list of values.

        Parameters:
        field_name (str): The name of the column to add or update.
        values (list): A list of values to be added to the column. If the list length does not match
                       the DataFrame length, the operation is skipped with a warning.
        """
        try:
            # Check if the list of values matches the DataFrame's length
            if isinstance(values, list) and len(values) != len(self.main_df):
                logging.warning(f"[!] List length for '{field_name}' doesn't match DataFrame length. Skipping.")
                return

            # If values is a single value, repeat it across all rows; else, assign the list directly
            if not isinstance(values, list):
                cleaned_vals = [str(x).strip('"') for x in numpy.repeat(values, len(self.main_df))]
                self.main_df[str(field_name)] = cleaned_vals

            else:
                self.main_df[str(field_name)] = values

            # logging.info(f"[i] Column '{field_name}' added/updated successfully.")
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
            updated_df = pandas.merge(self.main_df, lookup_df, on=shared_field, how='left')  # Perform join operation
            logging.info("[i] Lookup join completed successfully.")
            return updated_df
        except Exception as e:
            logging.error("[x] Error during DataFrame join: {}".format(str(e)))
            return self.main_df  # Return the unchanged main DataFrame in case of join error

    @staticmethod
    def resolve_terminal_nodes(ctx_obj):
        resolved_list = []

        def recurse(ctx):
            if isinstance(ctx, list):
                for sub_ctx in ctx:
                    recurse(sub_ctx)
            elif isinstance(ctx, antlr4.tree.Tree.TerminalNodeImpl):
                resolved_list.append(ctx)
            elif hasattr(ctx, 'children'):
                for child in ctx.children:
                    recurse(child)
            else:
                resolved_list.append(ctx)

        recurse(ctx_obj)
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
            if isinstance(entry, (numpy.ndarray, list, set)):
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
                if any(op in item for op in operators):
                    return True
            elif isinstance(item, list):
                if self.contains_operator(item, operators):
                    return True
        return False

    def remove_list_params(self, data):
        """
        Recursively filters out elements that are equal to '(' or ')'
        in a list which may contain sublists, numpy arrays, or other mixed entries.

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
        if self.current_variable in self.main_df.columns:  # Hold RESULTING VALUES, not expressions along with key.

            if self.general_handler.contains_sublist(key):
                self.temporary_storage.append((self.current_variable, out))
                logging.info(f"[i] Stored values in temp storage under key 'final'.")
            else:
                self.temporary_storage.append((key, out))
                logging.info(f"[i] Stored values in temp storage under key '{key}'.")

        else:
            self.main_df[self.current_variable] = out.tolist()  # Update main_df with RESULTING VALUES
            logging.info(f"[i] Updated DataFrame with new/updated column '{key}'.")

            # Add variable name tied to original expression for future EXPRESSION replacements
            target = (key, self.current_variable)
            if target not in self.variables:
                self.variables.append(target)

    # CRITICAL COMPONENT
    def replace_variables(self, expression_list):
        """Replace parts of the expression list with corresponding variables
        from self.variables and self.temporary_storage."""
        changes_were_made = False
        if not self.variables and not self.temporary_storage:
            return expression_list, False

        def replace_in_expression(expr_list, replacements):
            i = 0
            a_replacement_occurred = False
            while i < len(expr_list):
                replaced = False
                for expr, var in replacements:
                    expr_len = len(expr)
                    if lists_are_equal(expr_list[i:i + expr_len], expr):
                        expr_list = expr_list[:i] + [var] + expr_list[i + expr_len:]
                        replaced = True
                        a_replacement_occurred = True
                        break
                if replaced:
                    i = 0  # Restart from the beginning after a replacement
                else:
                    i += 1
            return self.remove_list_params(expr_list), a_replacement_occurred

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
    def validate_exceptions(self, ctx_obj, obj_identifier, start=True, end=True):
        resolved_nodes = self.resolve_terminal_nodes(ctx_obj)
        cleaned_expression = self.general_handler.convert_nested_list(resolved_nodes)

        #  EXIT Directive Cleanup
        if obj_identifier == 'exitDirective':
            if self.current_directive == 'eval':
                if self.temporary_storage:  # For mathematics operations
                    self.main_df[self.current_variable] = self.temporary_storage[-1][-1].tolist()
                    self.temporary_storage = []
                    self.variables = []  # Clear expression holding area after each exit
                else:
                    target_var, value = self.general_handler.return_equal_components_from_list(cleaned_expression)
                    # If there is only one object/item/variable on the right side of the equal sign
                    if len(value) == 1:
                        if isinstance(value[0], (int, float)):
                            self.main_df[target_var] = numpy.repeat(value, len(self.main_df))
                        elif isinstance(value[0], str):
                            # If set to a variable that exists in the main dataframe already
                            if value[0] in self.main_df.columns:  # Set new variable equal to current var column
                                self.main_df[target_var] = self.main_df[value[0]]
                            else:  # Otherwise, do a repeat on the string and add to self.main_df
                                self.main_df[target_var] = numpy.repeat(value, len(self.main_df))
                    else:
                        # I'm sure we will land here, and something different will need to be done. STUB Placeholder
                        pass

        elif obj_identifier == 'exitExpression':
            pass

        #  EXIT Mult/Div Cleanup
        elif obj_identifier == 'exitMultiplicativeExpr':
            if self.contains_operator(cleaned_expression, ['*', '/']):
                flag = True
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                if self.contains_operator(cleaned_expression, ['*', '/']):
                    self.exit_if_contains_any_unexpected_operations(cleaned_expression, obj_identifier)
                    self.perform_math_operation_and_update_df_with_result(cleaned_expression, 'mult_div')
                else:
                    pass

        # EXIT Add/Sub Cleanup
        elif obj_identifier == 'exitAdditiveExpr':
            if self.contains_operator(cleaned_expression, ['+', '-']):
                flag = True
                while flag:
                    cleaned_expression, flag = self.replace_variables(cleaned_expression)
                if self.contains_operator(cleaned_expression, ['+', '-']):
                    self.exit_if_contains_any_unexpected_operations(cleaned_expression, obj_identifier)
                    self.perform_math_operation_and_update_df_with_result(cleaned_expression, 'add_sub')
                else:
                    pass


del speakQueryParser

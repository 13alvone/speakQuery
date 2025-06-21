#!/usr/bin/env python3
import logging
import importlib
import numpy
import glob
import time
import os
from collections import OrderedDict, Counter
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
        self.assignment_val = None
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
        self.variables = OrderedDict()
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
        if not self.is_loadjob:
            self.parquet_handler.save_dataframe_to_parquet(self.main_df)

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
        # if not self.is_loadjob:
        #     self.parquet_handler.save_dataframe_to_parquet(self.main_df)
        # if self.values:
        #     self.validate_exceptions(ctx, 'exitTradeQuery')
        # logging.info(f'[+] Successfully processed valid speakQuery syntax and collapsed expressions.\n')
        # logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n'
        #              f'[+] RESULT:\n{self.main_df}\n')
        # logging.info(f'[+] Transposed Summary of Fields: (First 2 row entries shown from each column)'
        #              f'{self.general_handler.get_main_df_overview(self.main_df)}')
        # logging.info("[i] Parsing process completed.")
        # logging.info("[i] Duration: {:.2f} seconds".format(time.time() - self.start_time))

    def enterTableCall(self, ctx: SQP.TableCallContext):
        self.validate_exceptions(ctx, 'enterTableCall')

    def exitTableCall(self, ctx: SQP.TableCallContext):
        last_key, last_val = next(reversed(self.values.items()))
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
        last_key, last_val = next(reversed(self.values.items()))
        if isinstance(last_val, list):
            self.table_cmd = self.general_handler.remove_outer_newlines([str(x) for x in last_val])
            if len(self.table_cmd) > 1:
                self.drill_sql_cmd = self.table_cmd[-1].strip('"')

    def enterTableName(self, ctx: speakQueryParser.TableNameContext):
        self.validate_exceptions(ctx, 'enterTableName')

    def exitTableName(self, ctx: speakQueryParser.TableNameContext):
        self.validate_exceptions(ctx, 'exitTableName')

    def enterTimerangeCall(self, ctx: SQP.TimerangeCallContext):
        self.validate_exceptions(ctx, 'enterTimerangeCall')

    def exitTimerangeCall(self, ctx: SQP.TimerangeCallContext):
        last_key, last_val = next(reversed(self.values.items()))
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

        if len(self.values) > 1:
            last_key, last_val = next(reversed(self.values.items()))
        else:
            last_key, last_val = next(reversed(self.values.items()))
            last_key, last_val = [], OrderedDict([('final', last_val[-1])])

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

        # # Extract directive and directive subcomponents
        # if isinstance(last_val, list):
        #     if str(last_val[0]) == '|':
        #         if self.general_handler.contains_sublist(last_val):
        #             self.current_directive = str(last_val[1][0]).lower()
        #         else:
        #             self.current_directive = str(last_val[1])
        #     elif str(last_val[0]) in self.known_directives:
        #         self.current_directive = str(last_val[0])
        #
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
                last_key, last_val = 'final', self.values['final'][-1]
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
                last_key, last_val = next(reversed(self.values.items()))
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
            # variable_name, assignment_val = self.general_handler.return_equal_components_from_list(directive_parts)
            # if variable_name and assignment_val:  # Ensure variable name and assignment are not malformed
            #     if len(assignment_val) == 1:  # Ensure that there are not multiple assignment values detected.
            #         if not isinstance(assignment_val[0], list):
            #             try:
            #                 assignment_val = self.general_handler.resolve_terminal_nodes(assignment_val)
            #             except Exception as e:
            #                 self.generic_processing_exit('exitDirective', e)
            #         if not isinstance(assignment_val, list):
            #             if assignment_val[0] in self.main_df.columns:
            #                 self.add_or_update_column(variable_name, self.get_column_entries(assignment_val[0]))
            #             else:
            #                 self.add_or_update_column(variable_name, assignment_val[0])
            #         else:  # When new variable DOES NOT ALREADY exist in self.main_df
            #             for block in assignment_val:
            #                 if isinstance(block, (str, int, float)):  # When set to a single value
            #                     self.add_or_update_column(str(variable_name), block)
            #                     y = 'test'
            #                 if isinstance(block, (list, set, tuple)):
            #                     pass
            #
            #     else:
            #         self.generic_processing_exit('exitDirective', 'More than one assignment value found for eval.')
            # else:
            #     self.generic_processing_exit('exitDirective', 'Eval missing variable name or assignment val or both.')

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

    def enterSharedField(self, ctx:speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx, 'enterSharedField')

    def exitSharedField(self, ctx:speakQueryParser.SharedFieldContext):
        self.validate_exceptions(ctx, 'exitSharedField')

    def enterFilename(self, ctx:speakQueryParser.FilenameContext):
        self.validate_exceptions(ctx, 'enterFilename')

    def exitFilename(self, ctx:speakQueryParser.FilenameContext):
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
        # if self.current_directive == 'eval':
        #     last_sublist = self.extract_last_sublist()
        #     if last_sublist:
        #         if '/' in self.general_handler.convert_nested_list(last_sublist) or '*' in \
        #                 self.general_handler.convert_nested_list(last_sublist):
        #             y = 'test'
        #     y = 'test'
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass

    def enterMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx, 'enterMultiplicativeExpr')

    def exitMultiplicativeExpr(self, ctx: speakQueryParser.MultiplicativeExprContext):
        self.validate_exceptions(ctx, 'enterMultiplicativeExpr')
        # if self.current_directive == 'eval':
        #     last_sublist = self.extract_last_sublist()
        #     if last_sublist:
        #         if '/' in self.general_handler.convert_nested_list(last_sublist) or '*' in \
        #                 self.general_handler.convert_nested_list(last_sublist):
        #             y = 'test'
        #     y = 'test'
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass

        # self.validate_exceptions(ctx, 'exitAdditiveExpr', end=False)
        # last_key, last_val = next(reversed(self.values.items()))
        # self.validate_exceptions(ctx, 'exitAdditiveExpr', start=False)
        # y = 'test'
        # if len(self.values) > 1:
        #     last_key, last_val = self.general_handler.trim_to_last_sublist(
        #         next(reversed(self.values.items())))
        #     if self.general_handler.are_all_terminal_instances(last_val):
        #         if self.general_handler.contains_sublist(last_val):
        #             x = self.general_handler.convert_nested_list(last_val)
        #             y = self.general_handler.trim_to_last_sublist(last_val)[-1]
        # else:
        #     self.generic_processing_exit('exitMultiplicativeExpr', 'The last value is malformed.')
        #
        # y = 'test'

    def enterPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx, 'enterPrimaryExpr')

    def exitPrimaryExpr(self, ctx: speakQueryParser.PrimaryExprContext):
        self.validate_exceptions(ctx, 'exitPrimaryExpr')
        if self.general_handler.are_all_terminal_instances(ctx.children):
            pass

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

        # SITUATION: When there is an eval statement that is set equal to a single, static number.
        if self.current_directive == 'eval':
            assignment = self.general_handler.return_equal_components_from_list(self.extract_last_sublist())[-1]
            if assignment and isinstance(assignment, list):
                if len(assignment) == 1:
                    self.add_or_update_column(self.current_variable, self.general_handler.validate_ast(assignment[0]))

    def enterStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx, 'enterStaticString')

    def exitStaticString(self, ctx: speakQueryParser.StaticStringContext):
        self.validate_exceptions(ctx, 'exitStaticString')
        y = 'test'

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
        self.current_variable = self.general_handler.validate_ast(ctx.children[0])
        self.validate_exceptions(ctx, 'enterVariableName')

    def exitVariableName(self, ctx: speakQueryParser.VariableNameContext):
        self.validate_exceptions(ctx, 'enterVariableName')

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************
    @staticmethod
    def concatenate_children(_children_list):
        return f"{' '.join([str(x) for x in _children_list])}"

    def validate_exceptions(self, ctx_obj, obj_identifier, start=True, end=True):
        # if obj_identifier == 'exitStaticNumber':
        #     pass
        if start:
            if self.early_exit:
                return
            if ctx_obj.exception:
                logging.warning(f'[!] CUSTOM {obj_identifier} Error: {ctx_obj.exception}; '
                                f'{self.general_handler.convert_nested_list(ctx_obj.children)}')
            if not ctx_obj.children:
                logging.warning(f'[!] CUSTOM {obj_identifier}: NO CHILDREN!')
            if len(ctx_obj.children) <= 0:
                logging.warning(f'[!] CUSTOM {obj_identifier}: CHILDREN ENTRIES BLANK!')

            self.bubble_up_resolution(ctx_obj)  # This resolves terminalNodes on the self.values stack, in-place.

            if obj_identifier.startswith('enter'):  # Tracing stack operations and resolutions as they happen
                self.contextStack.append((obj_identifier.lstrip('enter'), ctx_obj))

            elif obj_identifier.startswith('exit'):
                if obj_identifier == 'exitProgram' or len(self.values) <= 1:
                    # self.exitProgram(ctx_obj)  # This option should only appear when it is the LAST directive.
                    self.early_exit = True
                    return

            # if self.values:  # Set the current eval variable name if found.
            #     if len(ctx_obj.children) == 1:
            #         if isinstance(ctx_obj.children[0], antlr4.tree.Tree.TerminalNodeImpl) \
            #                 and isinstance(ctx_obj, SQP.VariableNameContext):
            #             self.variables[ctx_obj.children[0]] = str(ctx_obj.children[0])
            #             if isinstance(ctx_obj.parentCtx.children[0], antlr4.tree.Tree.TerminalNodeImpl) \
            #                     and str(ctx_obj.parentCtx.children[0]) == 'eval':
            #                 self.current_variable = self.variables[ctx_obj.children[0]]

        # Collapse the stack found at self.values if terminal nodes only are found in last_val:
        if end:
            if self.values:
                if len(self.values) > 1:
                    last_key, last_val = next(reversed(self.values.items()))
                    if isinstance(last_val, antlr4.tree.Tree.TerminalNodeImpl):
                        while isinstance(last_val, antlr4.tree.Tree.TerminalNodeImpl):
                            self.collapse(self.values)
                            if not len(self.values) == 1:
                                self.values.popitem(last=True)
                                self.stack_handler.trim_stack_to_last_list(self.values)
                                last_key, last_val = next(reversed(self.values.items()))
                            else:
                                self.generic_processing_exit('validate_exceptions()', 'Stuck in a loop, so exiting.')
                    elif isinstance(last_val, list):
                        if self.general_handler.are_all_terminal_instances(last_val):
                            self.collapse(self.values)
                            if not len(self.values) == 1:
                                self.values.popitem(last=True)
                                self.stack_handler.trim_stack_to_last_list(self.values)
        x = 'test'

    def init_table_query(self, table_line):
        tables = self.general_handler.extract_db_names(table_line)
        if tables:
            tables = [f'{self.script_directory}/../{self.target_index_uri}/{x}.parquet' for x in tables]
            for table in tables:
                self.main_df = self.parquet_handler.read_parquet_file(table)
            print(self.main_df)

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
            resolved_values = self.general_handler.resolve_terminal_nodes(antlr_objs.children)
            last_stack_object = self.contextStack.pop()[-1]
            self.values[last_stack_object] = self.general_handler.flatten_outer_lists(last_stack_object.children)

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
                            self.target_tables.extend(glob.glob(f'{start_path}/{filename}.parquet'))
                        else:
                            self.target_tables.append(f'{start_path}/{filename}.parquet')
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

    def make_headers_unique(self):
        """
        Renames duplicate headers by appending an incremental number to make them unique.
        """
        header_counter = Counter(self.main_df.columns)  # Create a Counter to keep track of duplicate headers
        new_headers = []  # Create a new list for storing unique headers
        header_occurrences = {}  # Create a dictionary to store the count of each header encountered

        for header in self.main_df.columns:  # Iterate through the existing headers
            if header_counter[header] > 1:  # If this header is duplicated
                # Increment the count in header_occurrences
                header_occurrences[header] = header_occurrences.get(header, 0) + 1
                # Append the count to the header name to make it unique
                unique_header = f"{header}_{header_occurrences[header]}"
                new_headers.append(unique_header)
            else:
                new_headers.append(header)  # If not duplicated, keep the original

        self.main_df.columns = new_headers  # Assign the new unique headers to the DataFrame
        logging.info("[i] Duplicate headers have been renamed to be unique.")

    def replace_nan_with_empty_values(self):
        """
        Replaces NaN values in the DataFrame with the empty value of the column's given type.
        """
        for column in self.main_df:
            col_type = self.main_df[column].dtype
            if col_type == object:
                # Assuming object columns can contain mixed types including strings and lists
                if all(isinstance(x, list) for x in self.main_df[column].dropna()):
                    self.main_df[column] = self.main_df[column].fillna("")
            elif np.issubdtype(col_type, np.number):
                self.main_df[column] = self.main_df[column].fillna(0)
            else:
                logging.warning(f"[!] Column {column} has an unsupported type: {col_type}")

    def collapse(self, ordered_dict):
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
                                    logging.info(f'[x] Collapse Error {e}')

                    _last_key, _last_val = _od.popitem(last=False)
                    if len(_od) > 0:
                        collapse_recursive(_last_key, _last_val, _od, _collapsed_output)
                if not _collapsed_output and _last_val:
                    pass
                #     if isinstance(_last_val, list):
                #         self.values = OrderedDict([('final', [entry for entry in _last_val if isinstance(entry, list)])])
                #     # x = OrderedDict([([], [entry for entry in _last_val if isinstance(entry, list)])])
                #     # self.general_handler.print_final_command(_last_val)
                #     y = 'test'
                break

        reverse_od = OrderedDict(reversed(list(ordered_dict.items())))
        last_key, last_val = reverse_od.popitem(last=False)
        collapsed_output = []
        collapse_recursive(last_key, last_val, reverse_od, collapsed_output)

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

    # def app_or_update_column(self, _variable_name, assignment_val):
    #     """
    #     Adds or updates a column in self.main_df with the given list of values.
    #
    #     Parameters:
    #     field_name (str): The name of the column to add or update.
    #     values (list): A list of values to be added to the column. If the list length does not match
    #                    the DataFrame length, the operation is skipped with a warning.
    #     """
    #     try:
    #         # Check if the list of values matches the DataFrame's length
    #         _variable_name = str(_variable_name)
    #         if isinstance(assignment_val, list) and len(assignment_val) != len(self.main_df):
    #             logging.warning(f"[!] List length for '{_variable_name}' doesn't match DataFrame length. Skipping.")
    #             return
    #
    #         # If values is a single value, repeat it across all rows; else, assign the list directly
    #         if not isinstance(assignment_val, list):
    #             cleaned_vals = [str(x).strip('"') for x in np.repeat(assignment_val, len(self.main_df))]
    #             self.main_df[_variable_name] = cleaned_vals
    #         else:
    #             self.main_df[_variable_name] = assignment_val
    #
    #         logging.info(f"[i] Column '{_variable_name}' added/updated successfully.")
    #     except Exception as e:
    #         logging.error(f"[x] Failed to add/update '{_variable_name}' as a column: {e}")

    def get_column_entries(self, column_name):
        try:
            if column_name in self.main_df.columns:
                return self.main_df[column_name].tolist()
            else:
                logging.error(f"[x] The specified column '{column_name}' does not exist in the DataFrame.")
                return []
        except Exception as e:
            logging.error(f"[x] An error occurred while extracting entries from column '{column_name}': {e}")
            return []

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
                self.main_df[field_name] = cleaned_vals
            else:
                self.main_df[field_name] = values

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
        x = f'{self.script_directory}/../lookups/{filename}'
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

    def extract_last_sublist(self):
        _, last_val = next(reversed(self.values.items()))
        if isinstance(last_val, list):
            if self.general_handler.contains_sublist(last_val):
                last_val = self.general_handler.trim_to_last_sublist(last_val)[-1]
                if isinstance(last_val, list):
                    while self.general_handler.contains_sublist(last_val):
                        last_val = self.general_handler.trim_to_last_sublist(last_val)[-1]
                    return self.general_handler.return_equal_components_from_list(last_val)[-1]
            else:
                return last_val
        return []


del speakQueryParser

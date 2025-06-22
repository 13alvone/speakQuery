#!/usr/bin/env python3
"""
Module: speakQueryListener.py
Purpose: Implements the parse tree listener for speakQueryParser with secure dynamic
         loading of compiled shared objects (.so) for performance-critical operations.
         The shared objects are dynamically resolved and imported using the functionality
         provided by functionality/so_loader.py.
"""

import logging
import inspect
import shlex
import sys
import os
import re

from antlr4.tree.Tree import TerminalNodeImpl
from antlr4 import ParseTreeListener
# import pyarrow.dataset as ds
# import pyarrow.compute as pc
# import pyarrow as pa
# from pyarrow.dataset import Expression

from handlers.GeneralHandler import GeneralHandler
from handlers.LookupHandler import LookupHandler
from handlers.SearchCmdHandler import SearchDirective
from handlers.StatsHandler import StatsHandler
from handlers.MacroHandler import MacroHandler
from handlers.MultiSearchHandler import MultiSearchHandler

# Import speakQueryParser (support for relative or absolute import)
if "." in __name__:
    from .antlr4_active.speakQueryParser import speakQueryParser
else:
    from antlr4_active.speakQueryParser import speakQueryParser

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(message)s')


# ------------------------------------------------------------------------------------------------
# Function to dynamically find project root by climbing up from current file
# ------------------------------------------------------------------------------------------------
def find_project_root(start_path: str, marker_files=('app.py', '.git')):
    logging.debug("[DEBUG] Attempting to find project root...")
    current = os.path.abspath(start_path)
    while current != os.path.dirname(current):  # while not root
        if any(os.path.exists(os.path.join(current, marker)) for marker in marker_files):
            logging.debug(f"[DEBUG] Project root identified: {current}")
            return current
        current = os.path.dirname(current)
    raise RuntimeError("Project root not found")

# Compute project root and set working directory
project_root = find_project_root(__file__)
os.chdir(project_root)

# Define module build directories for the compiled shared libraries
cpp_index_path = os.path.join(project_root, "functionality", "cpp_index_call", "build")
cpp_datetime_path = os.path.join(project_root, "functionality", "cpp_datetime_parser", "build")

logging.info(f"[i] Project root: {project_root}")
logging.info(f"[i] CPP Index Call Build Directory: {cpp_index_path}")
logging.info(f"[i] CPP Datetime Parser Build Directory: {cpp_datetime_path}")

# Import the secure dynamic loader module for .so files
try:
    from functionality.so_loader import resolve_and_import_so
except ImportError as e:
    logging.error(f"[x] Could not import so_loader: {e}")
    sys.exit(1)

# Dynamically load the cpp_index_call shared object
try:
    cpp_index_module = resolve_and_import_so(cpp_index_path, "cpp_index_call")
    process_index_calls = cpp_index_module.process_index_calls
    logging.info("[i] Successfully loaded 'cpp_index_call' module.")
except ImportError as e:
    logging.error(f"[x] Could not import cpp_index_call: {e}")
    sys.exit(1)

# Dynamically load the cpp_datetime_parser shared object
try:
    cpp_datetime_module = resolve_and_import_so(cpp_datetime_path, "cpp_datetime_parser")
    parse_dates_to_epoch = cpp_datetime_module.parse_dates_to_epoch
    logging.info("[i] Successfully loaded 'cpp_datetime_parser' module.")
except ImportError as e:
    logging.error(f"[x] Could not import cpp_datetime_parser: {e}")
    sys.exit(1)


# ------------------------------------------------------------------------------------------------
# speakQueryListener Class Definition
# ------------------------------------------------------------------------------------------------
class speakQueryListener(ParseTreeListener):
    def __init__(self, cleaned_query):
        self.current_search_cmd_tokens = None
        self.project_root = f'{os.path.abspath(os.path.curdir)}'
        self.lookup_root = f'{self.project_root}/lookups'
        self.loadjob_root = f'{self.project_root}/{os.path.join("frontend", "static", "temp")}'
        self.current_inputlookup_path = None
        self.current_inputlookup_call = None
        self.current_inputlookup_filename = None
        self.current_loadjob_path = None
        self.current_loadjob_call = None
        self.current_loadjob_filename = None
        self.main_df = None
        self.root_ctx = None
        self.earliest_clause = None
        self.earliest_time = None
        self.latest_clause = None
        self.latest_time = None
        self.target_index = None
        self.initial_sequence_enabled = False
        self.original_query = cleaned_query.strip()
        # Remove spaces from the first pipe-separated part of the query
        self.original_index_call = ''.join(self.original_query.split('|')[0].strip()).replace(' ', '')
        self.general_handler = GeneralHandler()
        self.lookup_handler = LookupHandler()
        self.search_cmd_handler = SearchDirective()
        self.stats_handler = StatsHandler()
        self.macro_handler = MacroHandler()
        self.multisearch_handler = MultiSearchHandler()

    # Enter a parse tree produced by speakQueryParser#speakQuery.
    def enterSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        pass

    # Exit a parse tree produced by speakQueryParser#speakQuery.
    def exitSpeakQuery(self, ctx: speakQueryParser.SpeakQueryContext):
        """
        Exit hook for the top-level query context. Handles index calls,
        inputlookup, loadjob, and a full transformation pipeline (stats/eventstats/streamstats and eval)
        by re-parsing the raw query string to ensure all fields (e.g. multiple BY fields) are captured.
        Returns the resulting pandas DataFrame or None.
        """

        # Save the root context on first invocation
        if self.root_ctx is None:
            self.root_ctx = ctx

        # Validate any SEARCH directives early
        self.validate_exceptions(ctx)

        # Flatten to get tokens for index vs. transformations
        flattened = self.ctx_flatten(self.root_ctx)
        tokens = [t for t in flattened if t != "<EOF>"]

        # Skip a leading pipe if present
        if tokens and tokens[0] == '|':
            tokens = tokens[1:]

        # --- inputlookup handling ---
        if tokens and tokens[0].lower() == 'inputlookup':
            filename = tokens[1].strip('"').strip()
            path = os.path.join(self.lookup_root, filename)
            logging.info(f"[i] Running inputlookup on file: {filename}")
            self.main_df = self.lookup_handler.load_data(path)

            # Force any further transforms through eval
            follow = tokens[2:]
            if follow and follow[0].lower() != 'eval':
                follow.insert(0, 'eval')
            from handlers.EvalHandler import EvalHandler
            eval_handler = EvalHandler()
            try:
                self.main_df = eval_handler.run_eval(follow, self.main_df)
            except Exception as e:
                logging.error(f"[x] EvalHandler failure on inputlookup: {e}")
                raise
            return self.main_df

        # --- loadjob handling ---
        if tokens and tokens[0].lower() == 'loadjob':
            job_id = tokens[1].strip("'").strip()
            path = os.path.join(self.loadjob_root, job_id)
            logging.info(f"[i] Running loadjob on ID: {job_id}")
            self.main_df = self.general_handler.loadjob_pickle_file(path)

            follow = tokens[2:]
            if follow and follow[0].lower() != 'eval':
                follow.insert(0, 'eval')
            from handlers.EvalHandler import EvalHandler
            eval_handler = EvalHandler()
            try:
                self.main_df = eval_handler.run_eval(follow, self.main_df)
            except Exception as e:
                logging.error(f"[x] EvalHandler failure on loadjob: {e}")
                raise
            return self.main_df

        # --- index call only? ---
        try:
            first_pipe = tokens.index('|')
        except ValueError:
            # no pipe = pure index call
            combined = ''.join(tokens).replace(' ', '')
            if combined == self.original_index_call:
                logging.info("[i] Executing index call only.")
                self.main_df = process_index_calls(tokens)
            else:
                logging.warning("[!] Tokens did not match original index call.")
            return self.main_df

        # --- execute index call portion ---
        index_tokens = tokens[:first_pipe]
        combined_index = ''.join(index_tokens).replace(' ', '')
        if combined_index == self.original_index_call:
            logging.info("[i] Executing index call portion.")
            self.main_df = process_index_calls(index_tokens)
        else:
            logging.warning("[!] Index call tokens mismatch expected index call.")

        # --- transformations: re-tokenize raw query so we capture all BY fields ---
        # Raw pipeline string = everything after first '|'
        raw_pipeline = self.original_query.split('|', 1)[1]
        # Split on any additional '|' to get each segment
        segment_strs = [seg.strip() for seg in raw_pipeline.split('|') if seg.strip()]

        for seg_str in segment_strs:
            # Tokenize segment, preserving quoted literals
            try:
                seg_tokens = shlex.split(seg_str)
            except ValueError as e:
                logging.error(f"[x] Failed to tokenize segment '{seg_str}': {e}")
                raise

            cmd = seg_tokens[0].split('(')[0].lower()
            logging.info(f"[i] Processing pipeline segment: {cmd}")

            if cmd in ('stats', 'eventstats', 'streamstats'):
                try:
                    self.main_df = self.stats_handler.run_stats(seg_tokens, self.main_df)
                except Exception as e:
                    logging.error(f"[x] StatsHandler failure on '{seg_str}': {e}")
                    raise

            elif cmd == 'timechart':
                from handlers.ChartHandler import ChartHandler
                chart_handler = ChartHandler()
                try:
                    self.main_df = chart_handler.run_timechart(seg_tokens, self.main_df)
                except Exception as e:
                    logging.error(f"[x] ChartHandler failure on '{seg_str}': {e}")
                    raise

            elif cmd == 'eval':
                from handlers.EvalHandler import EvalHandler
                eval_handler = EvalHandler()
                try:
                    self.main_df = eval_handler.run_eval(seg_tokens, self.main_df)
                except Exception as e:
                    logging.error(f"[x] EvalHandler failure on '{seg_str}': {e}")
                    raise

            elif cmd in ('head', 'limit'):
                try:
                    count = int(seg_tokens[1]) if len(seg_tokens) > 1 else 5
                    self.main_df = self.general_handler.head_call(self.main_df, count, 'head')
                except Exception as e:
                    logging.error(f"[x] HEAD/LIMIT failure: {e}")

            elif cmd == 'sort':
                try:
                    cols = [c.lstrip('+-').strip(',') for c in seg_tokens[1:]]
                    direction = '+' if seg_tokens[1].startswith('+') else '-'
                    self.main_df = self.general_handler.sort_df_by_columns(self.main_df, cols, direction)
                except Exception as e:
                    logging.error(f"[x] SORT failure: {e}")

            elif cmd == 'reverse':
                self.main_df = self.general_handler.reverse_df_rows(self.main_df)

            elif cmd == 'regex':
                try:
                    arg = seg_tokens[1]
                    field, regex = arg.split('=', 1)
                    self.main_df = self.general_handler.filter_df_by_regex(self.main_df, field, regex)
                except Exception as e:
                    logging.error(f"[x] REGEX failure: {e}")

            elif cmd == 'fields':
                mode = '+'
                cols = []
                for tok in seg_tokens[1:]:
                    if tok.startswith('-'):
                        mode = '-'
                        cols.append(tok[1:].strip(','))
                    else:
                        cols.append(tok.strip(','))
                try:
                    self.main_df = self.general_handler.filter_df_columns(self.main_df, cols, mode)
                except Exception as e:
                    logging.error(f"[x] FIELDS failure: {e}")

            elif cmd == 'rename':
                try:
                    pairs = [p.strip() for p in ' '.join(seg_tokens[1:]).split(',')]
                    for pair in pairs:
                        if 'as' in pair:
                            old, new = [s.strip() for s in pair.split('as')]
                            self.main_df = self.general_handler.rename_column(self.main_df, old, new)
                except Exception as e:
                    logging.error(f"[x] RENAME failure: {e}")

            elif cmd == 'fieldsummary':
                try:
                    self.main_df = self.general_handler.execute_fieldsummary(self.main_df)
                except Exception as e:
                    logging.error(f"[x] FIELDSUMMARY failure: {e}")

            elif cmd == 'fillnull':
                try:
                    self.main_df = self.general_handler.execute_fillnull(self.main_df, seg_tokens[1:])
                except Exception as e:
                    logging.error(f"[x] FILLNULL failure: {e}")

            elif cmd == 'table':
                cols = [c.strip(',') for c in seg_tokens[1:]]
                try:
                    self.main_df = self.general_handler.filter_df_columns(self.main_df, cols, '+')
                except Exception as e:
                    logging.error(f"[x] TABLE failure: {e}")

            elif cmd == 'base64':
                try:
                    self.main_df = self.general_handler.handle_base64(self.main_df, seg_tokens)
                except Exception as e:
                    logging.error(f"[x] BASE64 failure: {e}")

            elif cmd == 'bin':
                try:
                    field = seg_tokens[1]
                    span = seg_tokens[seg_tokens.index('span') + 2] if 'span' in seg_tokens else '1h'
                    self.main_df = self.general_handler.execute_bin(self.main_df, field, span)
                except Exception as e:
                    logging.error(f"[x] BIN failure: {e}")

            elif cmd == 'join':
                try:
                    join_type = 'inner'
                    fields = []
                    idx = 1
                    while idx < len(seg_tokens) and seg_tokens[idx] != '[':
                        tok = seg_tokens[idx]
                        if tok.startswith('type='):
                            join_type = tok.split('=',1)[1]
                        else:
                            fields.extend([x.strip(',') for x in tok.split(',') if x])
                        idx += 1
                    sub_df = None
                    if '[' in seg_tokens:
                        start = seg_tokens.index('[') + 1
                        end = seg_tokens.index(']')
                        sub_df = process_index_calls(seg_tokens[start:end])
                    if sub_df is not None:
                        self.main_df = self.general_handler.execute_join(self.main_df, sub_df, fields, join_type)
                except Exception as e:
                    logging.error(f"[x] JOIN failure: {e}")

            elif cmd == 'append':
                try:
                    if '[' in seg_tokens:
                        start = seg_tokens.index('[') + 1
                        end = seg_tokens.index(']')
                        add_df = process_index_calls(seg_tokens[start:end])
                        self.main_df = self.general_handler.execute_append(self.main_df, add_df)
                except Exception as e:
                    logging.error(f"[x] APPEND failure: {e}")

            elif cmd == 'appendpipe':
                try:
                    if '[' in seg_tokens:
                        start = seg_tokens.index('[') + 1
                        end = seg_tokens.index(']')
                        sub_tokens = seg_tokens[start:end]
                        sub_df = self.run_subsearch(sub_tokens, self.main_df)
                        self.main_df = self.general_handler.execute_append(self.main_df, sub_df)
                except Exception as e:
                    logging.error(f"[x] APPENDPIPE failure: {e}")

            elif cmd == 'multisearch':
                try:
                    subs = []
                    idx = 1
                    while idx < len(seg_tokens):
                        if seg_tokens[idx] == '[':
                            end = seg_tokens.index(']', idx)
                            subs.append(seg_tokens[idx+1:end])
                            idx = end + 1
                        else:
                            idx += 1
                    self.main_df = self.multisearch_handler.run_multisearch(subs, process_index_calls)
                except Exception as e:
                    logging.error(f"[x] MULTISEARCH failure: {e}")

            elif cmd == 'lookup':
                try:
                    filename = seg_tokens[1]
                    key = seg_tokens[2]
                    output_fields = [t.strip(',') for t in seg_tokens[4:]] if 'OUTPUT' in seg_tokens else []
                    lookup_df = self.lookup_handler.load_data(os.path.join(self.lookup_root, filename))
                    if lookup_df is not None:
                        self.main_df = self.general_handler.execute_join(self.main_df, lookup_df, [key], 'left')
                        if output_fields:
                            self.main_df = self.general_handler.filter_df_columns(self.main_df, self.main_df.columns.tolist(), '+')
                except Exception as e:
                    logging.error(f"[x] LOOKUP failure: {e}")

            elif cmd == 'outputlookup':
                try:
                    args = self.general_handler.parse_outputlookup_args(seg_tokens[1:])
                    if isinstance(args, str):
                        kwargs = {"filename": os.path.join(self.lookup_root, args)}
                    else:
                        args['filename'] = os.path.join(self.lookup_root, args.get('filename', 'output.csv'))
                        kwargs = args
                    self.general_handler.execute_outputlookup(self.main_df, **kwargs)
                except Exception as e:
                    logging.error(f"[x] OUTPUTLOOKUP failure: {e}")

            elif cmd == 'coalesce':
                try:
                    if '(' in seg_str and ')' in seg_str:
                        inside = seg_str[seg_str.find('(')+1:seg_str.rfind(')')]
                        fields = [f.strip().strip(',') for f in inside.split(',')]
                    else:
                        fields = [t.strip(',') for t in seg_tokens[1:]]
                    self.main_df = self.general_handler.execute_coalesce(self.main_df, fields)
                except Exception as e:
                    logging.error(f"[x] COALESCE failure: {e}")

            elif cmd == 'mvexpand':
                field = seg_tokens[1]
                self.main_df = self.general_handler.execute_mvexpand(self.main_df, field)

            elif cmd == 'mvreverse':
                field = seg_tokens[1]
                self.main_df = self.general_handler.execute_mvreverse(self.main_df, field)

            elif cmd == 'mvcombine':
                field = None
                delim = ' '
                for t in seg_tokens[1:]:
                    if t.startswith('delim='):
                        delim = t.split('=',1)[1].strip('"')
                    else:
                        field = t.strip(',')
                if field:
                    self.main_df = self.general_handler.execute_mvcombine(self.main_df, field, delim)

            elif cmd == 'mvdedup':
                field = seg_tokens[1]
                self.main_df = self.general_handler.execute_mvdedup(self.main_df, field)

            elif cmd == 'mvappend':
                fields = [t.strip(',') for t in seg_tokens[1:]]
                self.main_df = self.general_handler.execute_mvappend(self.main_df, fields, fields[0])

            elif cmd == 'mvfilter':
                field = seg_tokens[1]
                value = seg_tokens[2].split('=')[1] if '=' in seg_tokens[2] else seg_tokens[2]
                self.main_df = self.general_handler.execute_mvfilter(self.main_df, field, value)

            elif cmd == 'mvcount':
                field = seg_tokens[1]
                self.main_df = self.general_handler.execute_mvcount(self.main_df, field, f"{field}_count")

            elif cmd == 'mvdc':
                field = seg_tokens[1]
                self.main_df = self.general_handler.execute_mvdc(self.main_df, field, f"{field}_dc")

            elif cmd == 'mvfind':
                field = seg_tokens[1]
                pattern = seg_tokens[2] if len(seg_tokens) > 2 else ''
                self.main_df = self.general_handler.execute_mvfind(self.main_df, field, pattern)

            elif cmd == 'mvzip':
                field1 = seg_tokens[1].rstrip(',')
                field2 = seg_tokens[2].rstrip(',')
                delim = seg_tokens[3].strip('"') if len(seg_tokens) > 3 else '_'
                self.main_df = self.general_handler.execute_mvzip(self.main_df, field1, field2, delim, 'mvzip')

            elif cmd == 'mvjoin':
                field = seg_tokens[1]
                delim = seg_tokens[2].split('=')[1] if len(seg_tokens)>2 else ' '
                self.main_df = self.general_handler.execute_mvjoin(self.main_df, field, delim)

            elif cmd == 'mvindex':
                field = seg_tokens[1]
                idxs = [int(i.strip(',')) for i in seg_tokens[2:]]
                self.main_df = self.general_handler.execute_mvindex(self.main_df, field, idxs, 'mvindex')

            elif cmd in ('if_', 'case', 'tonumber'):
                from handlers.EvalHandler import EvalHandler
                eval_handler = EvalHandler()
                try:
                    self.main_df = eval_handler.run_eval(seg_tokens, self.main_df)
                except Exception as e:
                    logging.error(f"[x] EvalHandler failure on '{seg_str}': {e}")
                    raise

            elif seg_str.startswith('`') and seg_str.endswith('`'):
                macro_body = seg_str[1:-1]
                if '(' in macro_body and macro_body.endswith(')'):
                    name, arg_str = macro_body.split('(', 1)
                    arg_str = arg_str[:-1]
                else:
                    name, arg_str = macro_body, ''
                args = self.macro_handler.parse_arguments(arg_str)
                try:
                    self.main_df = self.macro_handler.execute_macro(name, args, self.main_df)
                except Exception as e:
                    logging.error(f"[x] Macro '{name}' failure: {e}")

            else:
                logging.warning(f"[!] Unhandled transformation '{cmd}', defaulting to eval")
                from handlers.EvalHandler import EvalHandler
                eval_handler = EvalHandler()
                try:
                    self.main_df = eval_handler.run_eval(seg_tokens, self.main_df)
                except Exception as e:
                    logging.error(f"[x] EvalHandler failure on '{seg_str}': {e}")
                    raise

        return self.main_df

    # Enter a parse tree produced by speakQueryParser#initialSequence.
    def enterInitialSequence(self, ctx: speakQueryParser.InitialSequenceContext):
        self.initial_sequence_enabled = True
        self.validate_exceptions(ctx)

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
            # Use the dynamically loaded process_index_calls function
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
        self.current_inputlookup_call = self.ctx_flatten(ctx)
        self.current_inputlookup_filename = self.current_inputlookup_call[-1].strip().strip('"')
        self.current_inputlookup_path = f'{self.lookup_root}/{self.current_inputlookup_filename}'
        self.main_df = self.lookup_handler.load_data(f'{self.current_inputlookup_path}')
        self.validate_exceptions(ctx)

    # Enter a parse tree produced by speakQueryParser#loadjobInit.
    def enterLoadjobInit(self, ctx: speakQueryParser.LoadjobInitContext):
        pass

    # Exit a parse tree produced by speakQueryParser#loadjobInit.
    def exitLoadjobInit(self, ctx: speakQueryParser.LoadjobInitContext):
        self.current_loadjob_call = self.ctx_flatten(ctx)
        self.current_loadjob_filename = self.current_loadjob_call[-1].strip("'").strip()
        self.current_loadjob_path = f'{self.loadjob_root}/{self.current_loadjob_filename}'
        self.main_df = self.general_handler.loadjob_pickle_file(self.current_loadjob_path)
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
        self.validate_exceptions(ctx)

    # **************************************************************************************************************
    # Custom Functions
    # **************************************************************************************************************
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
        """
        Flattens the parse tree context and returns a normalized list of tokens.
        It uses extract_screenshot_of_ctx() and flatten_with_parens() to produce a token list,
        then normalizes the tokens by:
          - Stripping extra whitespace from each token.
          - Collapsing spaces between function names and the opening parenthesis.

        This ensures that tokens like "lower ( userRole )" become "lower(userRole)"
        for consistent downstream processing.
        """
        # Flatten the context while preserving parentheses
        flattened = self.flatten_with_parens(self.extract_screenshot_of_ctx(ctx))

        # Normalize tokens: strip whitespace and collapse identifier-parenthesis spaces.
        import re
        normalized = []
        for token in flattened:
            token = token.strip()
            token = re.sub(r'([a-zA-Z_][a-zA-Z_0-9]*)\s*\(', r'\1(', token)
            normalized.append(token)

        return normalized

    def run_subsearch(self, tokens, df):
        """Execute a list of tokens as a pipeline against df."""
        segments = []
        current = []
        for tok in tokens:
            if tok == '|':
                if current:
                    segments.append(current)
                current = []
            else:
                current.append(tok)
        if current:
            segments.append(current)

        result_df = df.copy()
        for seg in segments:
            if not seg:
                continue
            cmd = seg[0].split('(')[0].lower()
            if cmd in ('stats', 'eventstats', 'streamstats'):
                result_df = self.stats_handler.run_stats(seg, result_df)
            elif cmd == 'eval':
                from handlers.EvalHandler import EvalHandler
                eval_handler = EvalHandler()
                result_df = eval_handler.run_eval(seg, result_df)
            elif cmd in ('head', 'limit'):
                count = int(seg[1]) if len(seg) > 1 else 5
                result_df = self.general_handler.head_call(result_df, count, 'head')
            elif cmd == 'fields':
                mode = '+'
                cols = []
                for t in seg[1:]:
                    if t.startswith('-'):
                        mode = '-'
                        cols.append(t[1:].strip(','))
                    else:
                        cols.append(t.strip(','))
                result_df = self.general_handler.filter_df_columns(result_df, cols, mode)
        return result_df

    # CRITICAL COMPONENT
    def extract_screenshot_of_ctx(self, ctx):
        """
        Recursively processes the context tree and generates a list representing
        all terminal nodes, without handling parentheses nesting.
        """
        # tokens_to_skip = {'\n', '\r', '\t', ' ', '', ','}  # Removing original for now, but if errors, I will return.
        tokens_to_skip = {'\n', '\r', '\t', ' ', ''}

        if ctx is None:
            return None

        # Base case: If the context is a terminal node, return its text.
        if isinstance(ctx, TerminalNodeImpl):
            text = ctx.getText()
            if text.strip() in tokens_to_skip:
                return None  # Skip empty or unwanted tokens.
            else:
                return text

        children_results = []  # List to hold the final results.
        if hasattr(ctx, 'children') and ctx.children:
            idx = 0
            while idx < len(ctx.children):
                child = ctx.children[idx]
                child_result = self.extract_screenshot_of_ctx(child)  # Process each child recursively.
                if child_result is not None:
                    children_results.append(child_result)
                idx += 1

            # Remove None values and flatten the results.
            children_results = [child for child in children_results if child is not None]
            return self.flatten_list(children_results)
        else:
            return None

    # CRITICAL COMPONENT
    @staticmethod
    def flatten_list(result):
        """
        Flattens lists that have only one element to avoid unnecessary nesting.
        """
        if isinstance(result, list):
            flat_result = []
            for item in result:
                if isinstance(item, list):
                    flat_result.extend(item)
                else:
                    flat_result.append(item)
            if len(flat_result) == 1:
                return flat_result[0]
            return flat_result
        else:
            return result

    # CRITICAL COMPONENT
    @staticmethod
    def flatten_with_parens(input_list):
        """
        Flattens a nested list while preserving parentheses as individual items.
        """
        def flatten_recursive(element):
            if isinstance(element, list):
                if not element:
                    return []
                result = []
                for item in element:
                    if item == '(':
                        result.append('(')
                    elif item == ')':
                        result.append(')')
                    else:
                        result.extend(flatten_recursive(item))
                return result
            elif isinstance(element, str):
                return [element]
            else:
                return []
        return flatten_recursive(input_list)

    # CRITICAL COMPONENT
    def validate_exceptions(self, ctx_obj):
        """
        Validates exceptions by checking the current context and handling errors gracefully.
        """
        obj_identifier = inspect.currentframe().f_back.f_code.co_name
        if not obj_identifier or not isinstance(obj_identifier, str):
            self.generic_processing_exit('validate_exceptions', 'General Syntax Failure.')

        if obj_identifier == 'exitDirective':
            if str(ctx_obj.children[0]) == 'search':
                self.current_search_cmd_tokens = self.ctx_flatten(ctx_obj)[1:]
                self.main_df = self.search_cmd_handler.run_search(self.current_search_cmd_tokens, self.main_df)

    @staticmethod
    def normalize_tokens(token_list):
        """
        Normalizes a list of token strings by stripping extra whitespace and collapsing
        spaces between function names and the opening parenthesis.
        E.g. converts "lower ( userRole )" to "lower(userRole)".
        """
        normalized = []
        for token in token_list:
            # Strip token first
            token = token.strip()
            # Remove spaces between identifiers and '('
            token = re.sub(r'([a-zA-Z_][a-zA-Z_0-9]*)\s*\(', r'\1(', token)
            normalized.append(token)
        return normalized


# Clean up parser import to prevent circular imports
del speakQueryParser

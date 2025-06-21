#!/usr/bin/env python3

import logging
import numpy
import os
import re
from collections import OrderedDict

from tradeQueryParser import tradeQueryParser
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
        self.executable_query = self.general_handler.parse_comments(self.comments, self.original_query)
        self.expression_replacements = OrderedDict()
        self.current_function_tree = OrderedDict()
        self.all_variables = OrderedDict()
        self.function_depth_counter = 0
        self.index_counter = 0
        self.files = []
        self.main_df = None
        self.current_function_rep = ''
        self.eval_variable_name = ''
        self.table_line = ''
        self.other_handler = ''
        self.current_factor_numeric_result = ''
        self.current_unary_expression = ''
        self.current_boolean_value = ''
        self.current_term_numeric_result = ''
        self.current_variable_value = ''
        self.current_numeric_value = ''
        self.current_numeric_function_call = ''
        self.current_specific_function_call = ''
        self.current_comparison_operator = ''
        self.current_numeric_result = ''
        self.current_term_expression = ''
        self.current_string_function_call = ''
        self.current_compare_expression = ''
        self.current_arithmetic_expression = ''
        self.current_logical_operator = ''
        self.current_function_call = ''
        self.current_logical_expression = ''
        self.current_array_numeric_value = ''
        self.current_value = ''
        self.current_factor_string_expression = ''
        self.current_factor_numeric_expression = ''
        self.current_arithmetic_numeric_result = ''
        self.current_string_value = ''
        self.current_array_value = ''
        self.current_any_expression = ''
        self.current_arithmetic_string_expression = ''
        self.current_compare_numeric_expression = ''
        self.current_logical_string_expression = ''
        self.current_string_expression = ''
        self.current_expression = ''
        self.current_valid_line = ''
        self.file_custom_directive = ''
        self.concat_children = ''
        self.ctx_obj_str = ''
        self.current_function = ''
        self.collapsed_query = ''
        self.file_directive = ''
        self.current_variable = ''
        self.last_expression = ''
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
        self.attributes_to_replace = [
            'last_expression', 'current_valid_line', 'current_function', 'current_expression', 'current_function_call',
            'current_directive', 'current_arithmetic_expression', 'current_term_expression', 'current_value'
            'current_numeric_function_call', 'current_compare_numeric_expression', 'eval_variable_name']

    # Enter a parse tree produced by tradeQueryParser#program.
    def enterProgram(self, ctx: tradeQueryParser.ProgramContext):
        self.validate_exceptions(ctx, 'enterProgram')

    # Exit a parse tree produced by tradeQueryParser#program.
    def exitProgram(self, ctx: tradeQueryParser.ProgramContext):
        # self.validate_exceptions(ctx, 'exitProgram', replacements=False)
        pass

    # Enter a parse tree produced by tradeQueryParser#tradeQuery.
    def enterTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.validate_exceptions(ctx, 'enterTradeQuery')

    # Exit a parse tree produced by tradeQueryParser#tradeQuery.
    def exitTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.validate_exceptions(ctx, 'exitTradeQuery')
        self.general_handler.prep_variables(self.all_variables)
        self.general_handler.add_variables_as_columns(self.all_variables, self.main_df)

        logging.info(f'[+] Successfully processed valid TrueQRY syntax and collapsed expressions.')
        logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n'
                     f'[-] COLLAPSED QUERY:\n{self.executable_query}\n\n'
                     f'[+] RESULT:\n{self.main_df}\n\n')
        logging.info(f'[+] Summary of Fields: {self.general_handler.get_main_df_overview(self.main_df)}')

    # Enter a parse tree produced by tradeQueryParser#validLine.
    def enterValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'enterValidLine')
        self.table_line = \
            self.last_expression.strip('\n').strip().strip('\n').split('\n')[0].strip('\n').strip().strip('\n').strip()
        self.collapsed_query = self.table_line + '\n'
        self.init_table_query(self.collapsed_query)
        self.current_valid_line = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#validLine.
    def exitValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'exitValidLine')
        self.current_valid_line = ''

    # Enter a parse tree produced by tradeQueryParser#directive.
    def enterDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'enterDirective')
        self.current_directive = self.concat_children
        self.eval_variable_name = re.search(r'eval\s(\[.*])\s=', self.concat_children)
        if self.eval_variable_name:
            self.eval_variable_name = self.eval_variable_name.group(1)

    # Exit a parse tree produced by tradeQueryParser#directive.
    def exitDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'exitDirective')
        if str(self.current_directive) not in self.collapsed_query:
            self.collapsed_query += f'| {self.current_directive}\n'
        self.current_directive = ''

    # Enter a parse tree produced by tradeQueryParser#expression.
    def enterExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'enterExpression')
        self.current_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#expression.
    def exitExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'exitExpression')
        variable_name, variable_val = (x.strip() for x in self.current_directive.lstrip('eval ').split('='))
        self.make_extra_replacements(variable_name, variable_val)
        self.current_expression = ''

    # Enter a parse tree produced by tradeQueryParser#expressionString.
    def enterExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'enterExpressionString')
        self.current_string_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#expressionString.
    def exitExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'exitExpressionString')
        self.current_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#logicalStringExpr.
    def enterLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'enterLogicalStringExpr')
        self.current_logical_string_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def exitLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'exitLogicalStringExpr')
        self.current_logical_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def enterArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticStringExpr')
        self.current_arithmetic_string_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def exitArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticStringExpr')
        self.current_arithmetic_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#factorStringExpr.
    def enterFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'enterFactorStringExpr')
        self.current_factor_string_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#factorStringExpr.
    def exitFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'exitFactorStringExpr')
        self.current_factor_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#expressionNumeric.
    def enterExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'enterExpressionNumeric')
        self.current_factor_numeric_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#expressionNumeric.
    def exitExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'exitExpressionNumeric')
        self.current_factor_numeric_expression = ''

    # Enter a parse tree produced by tradeQueryParser#compareNumericExpr.
    def enterCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'enterCompareNumericExpr')
        self.current_compare_numeric_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def exitCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'exitCompareNumericExpr')
        self.current_compare_numeric_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def enterArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticNumericResultOnlyExpr')
        self.current_arithmetic_numeric_result = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def exitArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticNumericResultOnlyExpr')
        if self.current_directive.startswith('eval '):
            line = self.current_directive.lstrip('eval').strip()
            expression_parts = line.split('=')
            variable_name = expression_parts[0].split('eval')[-1].strip(' ').strip('\n').strip(' ').strip('\n')
            expression = '='.join(expression_parts[1:]).strip(' ').strip('\n').strip(' ').strip('\n')
            if self.general_handler.is_valid_expression(expression):
                eval_result = str(eval(expression))
                if eval_result:
                    if self.current_unary_expression:
                        self.make_extra_replacements(self.current_unary_expression, eval_result)
                    elif expression:
                        self.make_extra_replacements(expression, eval_result)

                    self.all_variables[variable_name] = eval_result
                    self.expression_replacements[expression] = eval_result
        self.current_arithmetic_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def enterTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterTermNumericResultOnlyExpr')
        self.current_term_numeric_result = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def exitTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitTermNumericResultOnlyExpr')
        self.current_term_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def enterFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterFactorNumericResultOnlyExpr')
        self.current_factor_numeric_result = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def exitFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitFactorNumericResultOnlyExpr')
        self.current_factor_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#expressionAny.
    def enterExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'enterExpressionAny')
        self.current_any_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#expressionAny.
    def exitExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'exitExpressionAny')
        self.current_any_expression = ''

    # Enter a parse tree produced by tradeQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'enterLogicalExpr')
        self.current_logical_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'exitLogicalExpr')
        self.current_logical_expression = ''

    # Enter a parse tree produced by tradeQueryParser#compareExpr.
    def enterCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'enterCompareExpr')
        self.current_compare_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#compareExpr.
    def exitCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'exitCompareExpr')
        self.current_compare_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticExpr.
    def enterArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticExpr')
        self.current_arithmetic_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def exitArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticExpr')
        self.current_arithmetic_expression = ''

    # Enter a parse tree produced by tradeQueryParser#termExpr.
    def enterTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'enterTermExpr')
        self.current_term_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#termExpr.
    def exitTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'exitTermExpr')
        self.current_term_expression = ''

    # Enter a parse tree produced by tradeQueryParser#factorExpr.
    def enterFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'enterFactorExpr')
        self.current_term_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#factorExpr.
    def exitFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'exitFactorExpr')
        self.current_term_expression = ''

    # Enter a parse tree produced by tradeQueryParser#value.
    def enterValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'enterValue')
        self.current_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#value.
    def exitValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'exitValue')
        self.current_value = ''

    # Enter a parse tree produced by tradeQueryParser#valueStringOnly.
    def enterValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'enterValueStringOnly')
        self.current_string_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#valueStringOnly.
    def exitValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'exitValueStringOnly')
        self.current_string_value = ''

    # Enter a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def enterValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'enterValueNumericResultOnly')
        self.current_numeric_result = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def exitValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'exitValueNumericResultOnly')
        self.current_numeric_result = 0

    # Enter a parse tree produced by tradeQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'enterUnaryExpr')
        self.current_unary_expression = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'exitUnaryExpr')
        if self.current_unary_expression:
            self.current_unary_expression = \
                self.current_unary_expression.replace(self.ctx_obj_str, self.concat_children, 1)
        result = str(self.string_handler.perform_unary_operation(self.current_unary_expression.replace(' ', '')))
        if self.current_unary_expression:
            self.make_extra_replacements(self.current_unary_expression, result)
        self.current_unary_expression = ''

    # Enter a parse tree produced by tradeQueryParser#numericValue.
    def enterNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'enterNumericValue')
        if self.current_unary_expression:
            self.current_unary_expression = \
                self.current_unary_expression.replace(self.ctx_obj_str, self.concat_children, 1)
        self.current_numeric_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#numericValue.
    def exitNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'exitNumericValue')
        self.current_numeric_value = ''

    # Enter a parse tree produced by tradeQueryParser#booleanValue.
    def enterBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'enterBooleanValue')
        self.current_boolean_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#booleanValue.
    def exitBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'exitBooleanValue')
        self.current_boolean_value = ''

    # Enter a parse tree produced by tradeQueryParser#stringValue.
    def enterStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'enterStringValue')
        self.current_string_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#stringValue.
    def exitStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'exitStringValue')
        self.current_string_value = ''

    # Enter a parse tree produced by tradeQueryParser#arrayValue.
    def enterArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'enterArrayValue')
        self.current_array_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#arrayValue.
    def exitArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'exitArrayValue')
        self.current_array_value = ''

    # Enter a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def enterArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'enterArrayValueNumeric')
        self.current_array_numeric_value = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def exitArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'exitArrayValueNumeric')
        self.current_array_numeric_value = ''

    # Enter a parse tree produced by tradeQueryParser#variableValue.
    def enterVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'enterVariableValue')
        if self.concat_children in self.main_df.columns:
            self.make_extra_replacements(
                self.concat_children, str(self.general_handler.get_column_entries(self.main_df, self.concat_children)))

        self.current_variable_value = self.concat_children
        x = 'test'

    # Exit a parse tree produced by tradeQueryParser#variableValue.
    def exitVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'exitVariableValue')
        if self.concat_children not in self.main_df.columns:
            if self.concat_children not in self.all_variables.keys():
                # Address when variables are mentioned that don't exist or haven't been pre-defined earlier in query.
                if self.eval_variable_name != self.concat_children \
                        and self.current_directive.startswith('eval') \
                        and self.concat_children not in self.main_df.columns \
                        and self.concat_children not in self.all_variables.keys():
                    logging.error(f'[!] Invalid TradeQuery: Variable {self.concat_children} mentioned, but not defined')
                    exit(1)
                else:
                    self.all_variables[self.concat_children] = self.concat_children

        self.current_variable_value = ''

    # Enter a parse tree produced by tradeQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'enterComparisonOperator')
        self.current_comparison_operator = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'exitComparisonOperator')
        self.current_comparison_operator = ''

    # Enter a parse tree produced by tradeQueryParser#logicalOperator.
    def enterLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'enterLogicalOperator')
        self.current_logical_operator = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#logicalOperator.
    def exitLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'exitLogicalOperator')
        self.current_logical_operator = ''

    # Enter a parse tree produced by tradeQueryParser#functionCall.
    def enterFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'enterFunctionCall')
        self.current_function_call = self.concat_children
        self.current_function = self.concat_children

    # Exit a parse tree produced by tradeQueryParser#functionCall.
    def exitFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'exitFunctionCall')
        self.current_function_call = ''
        self.current_function = ''

    # Enter a parse tree produced by tradeQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'enterNumericFunctionCall')
        self.current_numeric_function_call = self.concat_children
        self.current_function_rep = str(ctx)
        self.current_function_tree[str(ctx)] = self.concat_children
        self.function_depth_counter += 1

        if self.current_function:
            _, self.current_function = self.current_function_tree.popitem(last=True)
            self.current_function_tree[self.current_function] = self.concat_children  # VERY IMPORTANT LINE

    # Exit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'exitNumericFunctionCall')

        if self.current_function_tree:
            _, self.current_function = self.current_function_tree.popitem(last=True)
        else:
            logging.error(f'[!] Error in processing at exitStringFunctionCall: FUNCTION TREE UNEXPECTEDLY EMPTY.')

        unchanged_params = self.general_handler.extract_outermost_parentheses(self.current_function)
        args = self.general_handler.clean_args(self.all_variables, unchanged_params)
        args_numeric = self.math_handler.ast_numeric_args(args[0])

        target_function_name = self.general_handler.extract_function_name(self.concat_children)
        results = unchanged_params
        if target_function_name == 'min' and len(args) == 1:
            results = self.math_handler.column_operation('min', args_numeric)
        elif target_function_name == 'max' and len(args) == 1:
            results = self.math_handler.column_operation('max', args_numeric)
        elif target_function_name == 'avg' and len(args) == 1:
            results = self.math_handler.column_operation('avg', args_numeric)
        elif target_function_name == 'sum' and len(args) == 1:
            results = self.math_handler.complex_sum(args_numeric)
        elif target_function_name == 'range' and len(args) == 1:
            results = self.math_handler.column_operation('range', args_numeric)
        elif target_function_name == 'median' and len(args) == 1:
            results = self.math_handler.column_operation('median', args_numeric)
        elif target_function_name == 'mode' and len(args) == 1:
            results = self.math_handler.column_operation('mode', args_numeric)
        elif target_function_name == 'dcount' and len(args) == 1:
            results = self.math_handler.column_operation('dcount', args_numeric)
        elif target_function_name == 'sqrt' and len(args) == 1:
            results = self.math_handler.complex_square_root(args_numeric)
        elif target_function_name == 'random':
            results = self.math_handler.complex_randomize(args_numeric)
        elif target_function_name == 'round':
            target, round_level = args_numeric, args[-1]
            results = self.math_handler.complex_round(target, round_level)

        if self.current_function_call:
            self.make_extra_replacements(self.current_function_call, str(results))
        elif self.current_function:
            self.make_extra_replacements(self.current_function, str(results))

        if self.eval_variable_name in self.all_variables and \
                self.all_variables[self.eval_variable_name] == self.eval_variable_name:
            self.all_variables[self.eval_variable_name] = \
                self.all_variables[self.eval_variable_name].replace(self.eval_variable_name, str(results), 1)
        elif self.eval_variable_name in self.all_variables:
            self.all_variables[self.eval_variable_name] = str(results)

        self.function_depth_counter -= 1
        self.current_numeric_function_call = ''

    # Enter a parse tree produced by tradeQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'enterStringFunctionCall')
        self.current_string_function_call = self.concat_children
        self.current_function_rep = str(ctx)
        self.current_function_tree[str(ctx)] = self.concat_children
        self.function_depth_counter += 1

        if self.current_function:
            _, self.current_function = self.current_function_tree.popitem(last=True)
            self.current_function_tree[self.current_function] = self.concat_children  # VERY IMPORTANT LINE

    # Exit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'exitStringFunctionCall')
        if self.current_function_tree:
            _, self.current_function = self.current_function_tree.popitem(last=True)
        else:
            logging.error(f'[!] Error in processing at exitStringFunctionCall: FUNCTION TREE UNEXPECTEDLY EMPTY.')

        unchanged_params = self.general_handler.extract_outermost_parentheses(self.current_function)
        args = self.general_handler.clean_args(self.all_variables, unchanged_params)

        target_function_name = self.general_handler.extract_function_name(self.concat_children)
        results = ''
        # args = self.string_handler.\
        #     ast_string_args(self.general_handler.clean_args(self.all_variables, unchanged_params))
        if target_function_name in ['upper', 'lower', 'capitalize', 'len', 'defang', 'urlencode', 'urldecode']:
            results = self.string_handler.transform_strings(args, target_function_name)[0]
        elif target_function_name == 'concat':
            args = self.general_handler.process_concat_args(args)
            results = self.string_handler.full_concat(args)
        elif target_function_name == 'replace':
            start_string, string_to_replace, replace_with = args
            results = self.string_handler.replace_occurrences(start_string, string_to_replace, replace_with)
        elif target_function_name == 'substr':
            start_string, start_position, end_position = args
            results = self.string_handler.substring(start_string, start_position, end_position)
        elif target_function_name in ['trim', 'ltrim', 'rtrim']:
            if len(args) == 2:
                results = self.string_handler.trim_strings(args[0], characters=args[-1])
            else:
                results = self.string_handler.trim_strings(args)

        # Update all variables holding locations
        if self.eval_variable_name in self.all_variables and \
                self.all_variables[self.eval_variable_name] == self.eval_variable_name:
            self.all_variables[self.eval_variable_name] = \
                self.all_variables[self.eval_variable_name].replace(self.eval_variable_name, str(results), 1)
        elif self.eval_variable_name in self.all_variables:
            self.all_variables[self.eval_variable_name] = str(results)

        # Update all function call holding locations
        if self.current_function_call:
            if isinstance(results, list):
                for variable_name, var_value in self.all_variables.items():
                    if str(self.current_function_call) in var_value:
                        self.all_variables[variable_name] = results
            self.make_extra_replacements(self.current_function_call, str(results))
        elif self.current_function:
            if isinstance(results, list):
                for variable_name, var_value in self.all_variables.items():
                    if str(self.current_function) in var_value:
                        self.all_variables[variable_name] = results
            self.make_extra_replacements(self.current_function, str(results))

        self.function_depth_counter -= 1
        self.current_string_function_call = ''

    # Enter a parse tree produced by tradeQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'enterSpecificFunctionCall')
        self.current_function_tree[str(ctx)] = self.concat_children
        self.function_depth_counter += 1
        self.current_specific_function_call = self.concat_children

        if self.current_function:
            _, self.current_function = self.current_function_tree.popitem(last=True)
            self.current_function_tree[self.current_function] = self.concat_children  # VERY IMPORTANT LINE

    # Exit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'exitSpecificFunctionCall')

        if self.current_function_tree:
            _, self.current_function = self.current_function_tree.popitem(last=True)
        else:
            logging.error(f'[!] Error in processing at exitSpecificFunctionCall: FUNCTION TREE UNEXPECTEDLY EMPTY.')

        unchanged_params = self.general_handler.extract_outermost_parentheses(self.current_function)
        _, has_sub_function, _ = self.general_handler.contains_known_function(unchanged_params, self.all_string_funcs)

        if not has_sub_function:
            target_function_name = self.general_handler.extract_function_name(self.concat_children)
            results = ''
            args = self.general_handler.clean_args(self.all_variables, unchanged_params)
            if target_function_name == 'time':
                results = self.time_handler.parse_input_date(args[0])
            elif target_function_name == 'timerange':
                results = self.general_handler.filter_dates(self.time_handler, args[0], args[1], args[2])
                self.main_df = self.general_handler.filter_df_with_dates(self.main_df, results)  # Filter by time range.
            elif target_function_name == 'fieldsummary':
                print(f'[+] Summary of Fields: {self.general_handler.get_main_df_overview(self.main_df)}')
            elif target_function_name == 'earliest':        # Requires stats or eventstats directive
                pass
            elif target_function_name == 'latest':          # Requires stats or eventstats directive
                pass
            elif target_function_name == 'null':
                return ''
            elif target_function_name == 'isnull':
                if args:
                    if isinstance(args[0], list):
                        return ['True' if not x or x == '' else 'False' for x in args[0]]
                    else:
                        return 'True' if not args or args[0] == '' else 'False'
            elif target_function_name == 'isnotnull':
                if args:
                    if isinstance(args[0], list):
                        return ['False' if not x or x == '' else 'True' for x in args[0]]
                    else:
                        return 'False' if not args or args[0] == '' else 'True'
            elif target_function_name == 'coalesce':
                results = self.general_handler.coalesce_lists(args)
            elif target_function_name == 'mvjoin':
                pass
            elif target_function_name == 'mvindex':
                pass
            elif target_function_name == 'first':           # Requires stats or eventstats directive
                pass
            elif target_function_name == 'last':            # Requires stats or eventstats directive
                pass
            elif target_function_name == 'macro':
                pass
            elif target_function_name == 'values':
                pass

            if self.eval_variable_name in self.all_variables and \
                    self.all_variables[self.eval_variable_name] == self.eval_variable_name:
                self.all_variables[self.eval_variable_name] = \
                    self.all_variables[self.eval_variable_name].replace(self.eval_variable_name, str(results), 1)
            elif self.eval_variable_name in self.all_variables:
                self.all_variables[self.eval_variable_name] = str(results)

            if self.current_function_call:
                if isinstance(results, list):
                    for variable_name, var_value in self.all_variables.items():
                        if str(self.current_function_call) in var_value:
                            self.all_variables[variable_name] = results
                self.make_extra_replacements(self.current_function_call, str(results))
            elif self.current_function:
                if isinstance(results, list):
                    for variable_name, var_value in self.all_variables.items():
                        if str(self.current_function) in var_value:
                            self.all_variables[variable_name] = results
                self.make_extra_replacements(self.current_function, str(results))

        self.function_depth_counter -= 1
        self.current_string_function_call = ''

    # *************************************************************************************
    # Custom Functions
    # *************************************************************************************

    @staticmethod
    def concatenate_children(_children_list):
        return f"{' '.join([str(x) for x in _children_list])}"

    def validate_exceptions(self, ctx_obj, obj_identifier, replacements=True):
        if ctx_obj.exception:
            logging.error(f'[!] CUSTOM {obj_identifier} Error: {ctx_obj.exception}')
        if not ctx_obj.children:
            logging.error(f'[!] CUSTOM {obj_identifier}: NO CHILDREN!')
        if len(ctx_obj.children) <= 0:
            logging.error(f'[!] CUSTOM {obj_identifier}: CHILDREN ENTRIES BLANK!')

        self.ctx_obj_str = str(ctx_obj)
        self.concat_children = self.concatenate_children(ctx_obj.children)

        if not replacements:
            return

        if not self.last_expression:
            self.last_expression = self.concat_children
        else:
            if self.function_depth_counter > 1 and str(ctx_obj) in self.last_expression:
                self.last_expression = \
                    self.last_expression.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_valid_line = \
                    self.current_valid_line.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_function = \
                    self.current_function.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_expression = \
                    self.current_expression.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_function_call = \
                    self.current_function_call.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_directive = \
                    self.current_directive.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_arithmetic_expression = \
                    self.current_arithmetic_expression.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_term_expression = \
                    self.current_term_expression.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_numeric_function_call = \
                    self.current_numeric_function_call.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_compare_numeric_expression = \
                    self.current_compare_numeric_expression.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                if self.eval_variable_name:
                    self.eval_variable_name = \
                        self.eval_variable_name.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                self.current_value = self.current_value.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
                for _, _val in self.current_function_tree.items():
                    self.current_function_tree[_] = _val.replace(str(ctx_obj.parentCtx), self.concat_children, 1)
            else:
                if self.ctx_obj_str:
                    self.last_expression = self.last_expression.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_valid_line = \
                        self.current_valid_line.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_function = \
                        self.current_function.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_expression = \
                        self.current_expression.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_function_call = \
                        self.current_function_call.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_directive = \
                        self.current_directive.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_arithmetic_expression = \
                        self.current_arithmetic_expression.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_term_expression = \
                        self.current_term_expression.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_numeric_function_call = \
                        self.current_numeric_function_call.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_compare_numeric_expression = \
                        self.current_compare_numeric_expression.replace(self.ctx_obj_str, self.concat_children, 1)
                    if self.eval_variable_name:
                        self.eval_variable_name = \
                            self.eval_variable_name.replace(self.ctx_obj_str, self.concat_children, 1)
                    self.current_value = self.current_value.replace(self.ctx_obj_str, self.concat_children, 1)
                    for _, _val in self.current_function_tree.items():
                        self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concat_children, 1)

    def make_extra_replacements(self, replace_this, with_this):
        self.last_expression = \
            self.last_expression.replace(replace_this, with_this, 1)
        self.current_valid_line = \
            self.current_valid_line.replace(replace_this, with_this, 1)
        self.current_function = \
            self.current_function.replace(replace_this, with_this, 1)
        self.current_expression = \
            self.current_expression.replace(replace_this, with_this, 1)
        self.current_function_call = \
            self.current_function_call.replace(replace_this, with_this, 1)
        self.current_directive = \
            self.current_directive.replace(replace_this, with_this, 1)
        self.current_arithmetic_expression = \
            self.current_arithmetic_expression.replace(replace_this, with_this, 1)
        self.current_term_expression = \
            self.current_term_expression.replace(replace_this, with_this, 1)
        self.current_numeric_function_call = \
            self.current_numeric_function_call.replace(replace_this, with_this, 1)
        self.current_compare_numeric_expression = \
            self.current_compare_numeric_expression.replace(replace_this, with_this, 1)
        self.current_value = self.current_value.replace(replace_this, with_this, 1)

        for _, _val in self.current_function_tree.items():
            self.current_function_tree[_] = _val.replace(str(replace_this), str(with_this), 1)
        if self.eval_variable_name:
            self.all_variables[self.eval_variable_name] = \
                self.all_variables[self.eval_variable_name].replace(str(replace_this), str(with_this), 1)

    def init_table_query(self, table_line):
        tables = self.general_handler.extract_db_names(table_line)
        if tables:
            tables = [f'{self.script_directory}/../{self.target_index_uri}/{x}.parquet' for x in tables]
            for table in tables:
                self.main_df = self.parquet_handler.read_parquet_file(table)
            print(self.main_df)


del tradeQueryParser

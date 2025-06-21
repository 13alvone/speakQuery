import logging
import os
import re

import antlr4.tree.Tree
from collections import OrderedDict
from itertools import takewhile
from tradeQueryParser import tradeQueryParser
from tradeQueryListener import tradeQueryListener
from StringHandler import *

logging.basicConfig(level=logging.INFO, format='%(message)s')


class CustomTradeQueryListener(tradeQueryListener):
    def __init__(self, original_query):
        self.current_factor_numeric_result = None
        self.current_unary_expression = None
        self.current_boolean_value = None
        self.current_term_numeric_result = None
        self.current_variable_value = None
        self.current_numeric_value = None
        self.current_numeric_function_call = None
        self.current_specific_function_call = None
        self.current_comparison_operator = None
        self.current_numeric_result = None
        self.current_term_expression = None
        self.current_string_function_call = None
        self.current_compare_expression = None
        self.current_arithmetic_expression = None
        self.current_logical_operator = None
        self.current_function_call = None
        self.current_logical_expression = None
        self.current_array_numeric_value = None
        self.current_value = None
        self.current_factor_string_expression = None
        self.current_factor_numeric_expression = None
        self.current_arithmetic_numeric_result = None
        self.current_string_value = None
        self.current_array_value = None
        self.current_any_expression = None
        self.current_arithmetic_string_expression = None
        self.current_compare_numeric_expression = None
        self.current_logical_string_expression = None
        self.current_string_expression = None
        self.current_expression = None
        self.current_valid_line = ''
        self.file_custom_directive = None
        self.function_depth_counter = 0
        self.concatenated_children = ''
        self.ctx_obj_str = ''
        self.custom_functions = CustomFunctions()
        self.current_function_tree = OrderedDict()
        self.current_function = ''
        self.original_query = original_query.strip('\n').strip(' ').strip('\n').strip(' ')
        self.all_variables = OrderedDict()
        self.collapsed_query = ''
        self.target_index_uri = 'indexes/test_dbs'
        self.file_directive = None
        self.current_variable = {'name': '', 'value': None}
        self.last_expression = ''
        self.script_directory = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.main_df = None
        self.index_counter = 0
        self.files = []
        self.current_directive = 'table'
        self.known_directives = [
            'file', 'eval', 'stats', 'where', 'rename', 'fields', 'search', 'reverse', 'head', 'rex', 'regex', 'dedup']
        self.all_functions = [
            'round (', 'min (', 'max (', 'avg (', 'sum (', 'range (', 'median (', 'sqrt (', 'random (', 'tonumb (',
            'dcount (', 'concat (', 'replace (', 'upper (', 'lowerm(', 'capitalize (', 'trim (', 'ltrim (', 'rtrim (',
            'substr (', 'len (', 'tostring (', 'match (', 'urlencode (', 'defang (', 'type (']
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
        logging.info(f'[+] Successfully processed valid TrueQRY syntax and collapsed expressions.')
        # self.last_expression = self.last_expression.strip().replace('\n ', '\n')
        output = '\n'.join([line.strip() for line in self.last_expression.split('\n')]).strip()
        logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n'
                     f'[-]COLLAPSED QUERY:\n{output}\n')

    # Enter a parse tree produced by tradeQueryParser#validLine.
    def enterValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'enterValidLine')
        self.current_valid_line = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#validLine.
    def exitValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.validate_exceptions(ctx, 'exitValidLine')
        self.current_valid_line = ''

    # Enter a parse tree produced by tradeQueryParser#directive.
    def enterDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'enterDirective')
        self.current_directive = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#directive.
    def exitDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.validate_exceptions(ctx, 'exitDirective')
        self.current_directive = ''

    # Enter a parse tree produced by tradeQueryParser#expression.
    def enterExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'enterExpression')
        self.current_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#expression.
    def exitExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.validate_exceptions(ctx, 'exitExpression')
        self.current_expression = ''

    # Enter a parse tree produced by tradeQueryParser#expressionString.
    def enterExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'enterExpressionString')
        self.current_string_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#expressionString.
    def exitExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.validate_exceptions(ctx, 'exitExpressionString')
        self.current_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#logicalStringExpr.
    def enterLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'enterLogicalStringExpr')
        self.current_logical_string_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def exitLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.validate_exceptions(ctx, 'exitLogicalStringExpr')
        self.current_logical_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def enterArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticStringExpr')
        self.current_arithmetic_string_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def exitArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticStringExpr')
        self.current_arithmetic_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#factorStringExpr.
    def enterFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'enterFactorStringExpr')
        self.current_factor_string_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#factorStringExpr.
    def exitFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.validate_exceptions(ctx, 'exitFactorStringExpr')
        self.current_factor_string_expression = ''

    # Enter a parse tree produced by tradeQueryParser#expressionNumeric.
    def enterExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'enterExpressionNumeric')
        self.current_factor_numeric_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#expressionNumeric.
    def exitExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.validate_exceptions(ctx, 'exitExpressionNumeric')
        self.current_factor_numeric_expression = ''

    # Enter a parse tree produced by tradeQueryParser#compareNumericExpr.
    def enterCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'enterCompareNumericExpr')
        self.current_compare_numeric_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def exitCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.validate_exceptions(ctx, 'exitCompareNumericExpr')
        self.current_compare_numeric_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def enterArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticNumericResultOnlyExpr')
        self.current_arithmetic_numeric_result = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def exitArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticNumericResultOnlyExpr')
        if self.current_directive == 'eval':
            parts = self.last_expression.strip('\n').split('\n')
            expression = parts[0].split('=')[-1].strip(' ').strip('\n').strip(' ').strip('\n')
            if self.is_valid_expression(expression, 'any') and self.current_variable['name'] in parts[0]:
                eval_result = eval(expression)
                self.current_variable['value'] = eval_result
                if eval_result:
                    self.all_variables[self.current_variable['name']] = self.current_variable['value']
                self.collapsed_query += f'{parts[0].strip(" ").replace(expression, str(eval_result))}\n'
        self.current_arithmetic_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def enterTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterTermNumericResultOnlyExpr')
        self.current_term_numeric_result = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def exitTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitTermNumericResultOnlyExpr')
        self.current_term_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def enterFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'enterFactorNumericResultOnlyExpr')
        self.current_factor_numeric_result = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def exitFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.validate_exceptions(ctx, 'exitFactorNumericResultOnlyExpr')
        self.current_factor_numeric_result = ''

    # Enter a parse tree produced by tradeQueryParser#expressionAny.
    def enterExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'enterExpressionAny')
        self.current_any_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#expressionAny.
    def exitExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.validate_exceptions(ctx, 'exitExpressionAny')
        self.current_any_expression = ''

    # Enter a parse tree produced by tradeQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'enterLogicalExpr')
        self.current_logical_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.validate_exceptions(ctx, 'exitLogicalExpr')
        self.current_logical_expression = ''

    # Enter a parse tree produced by tradeQueryParser#compareExpr.
    def enterCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'enterCompareExpr')
        self.current_compare_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#compareExpr.
    def exitCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.validate_exceptions(ctx, 'exitCompareExpr')
        self.current_compare_expression = ''

    # Enter a parse tree produced by tradeQueryParser#arithmeticExpr.
    def enterArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'enterArithmeticExpr')
        self.current_arithmetic_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def exitArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.validate_exceptions(ctx, 'exitArithmeticExpr')
        self.current_arithmetic_expression = ''

    # Enter a parse tree produced by tradeQueryParser#termExpr.
    def enterTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'enterTermExpr')
        self.current_term_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#termExpr.
    def exitTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.validate_exceptions(ctx, 'exitTermExpr')
        self.current_term_expression = ''

    # Enter a parse tree produced by tradeQueryParser#factorExpr.
    def enterFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'enterFactorExpr')
        self.current_term_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#factorExpr.
    def exitFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.validate_exceptions(ctx, 'exitFactorExpr')
        self.current_term_expression = ''

    # Enter a parse tree produced by tradeQueryParser#value.
    def enterValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'enterValue')
        self.current_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#value.
    def exitValue(self, ctx: tradeQueryParser.ValueContext):
        self.validate_exceptions(ctx, 'exitValue')
        self.current_value = ''

    # Enter a parse tree produced by tradeQueryParser#valueStringOnly.
    def enterValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'enterValueStringOnly')
        self.current_string_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#valueStringOnly.
    def exitValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.validate_exceptions(ctx, 'exitValueStringOnly')
        self.current_string_value = ''

    # Enter a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def enterValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'enterValueNumericResultOnly')
        self.current_numeric_result = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def exitValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.validate_exceptions(ctx, 'exitValueNumericResultOnly')
        self.current_numeric_result = 0

    # Enter a parse tree produced by tradeQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'enterUnaryExpr')
        self.current_unary_expression = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.validate_exceptions(ctx, 'exitUnaryExpr')
        self.current_unary_expression = ''

    # Enter a parse tree produced by tradeQueryParser#numericValue.
    def enterNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'enterNumericValue')
        self.current_numeric_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#numericValue.
    def exitNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.validate_exceptions(ctx, 'exitNumericValue')
        self.current_numeric_value = ''

    # Enter a parse tree produced by tradeQueryParser#booleanValue.
    def enterBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'enterBooleanValue')
        self.current_boolean_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#booleanValue.
    def exitBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.validate_exceptions(ctx, 'exitBooleanValue')
        self.current_boolean_value = ''

    # Enter a parse tree produced by tradeQueryParser#stringValue.
    def enterStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'enterStringValue')
        self.current_string_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#stringValue.
    def exitStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.validate_exceptions(ctx, 'exitStringValue')
        self.current_string_value = ''
        # if self.function_depth_counter > 0 and str(ctx.parentCtx) in self.last_expression:
        #     self.last_expression = self.last_expression.replace(str(ctx.parentCtx), self.ctx_obj_str)

    # Enter a parse tree produced by tradeQueryParser#arrayValue.
    def enterArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'enterArrayValue')
        self.current_array_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#arrayValue.
    def exitArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.validate_exceptions(ctx, 'exitArrayValue')
        self.current_array_value = ''

    # Enter a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def enterArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'enterArrayValueNumeric')
        self.current_array_numeric_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def exitArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.validate_exceptions(ctx, 'exitArrayValueNumeric')
        self.current_array_numeric_value = ''

    # Enter a parse tree produced by tradeQueryParser#variableValue.
    def enterVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'enterVariableValue')
        self.current_variable_value = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#variableValue.
    def exitVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.validate_exceptions(ctx, 'exitVariableValue')
        self.current_variable_value = ''

    # Enter a parse tree produced by tradeQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'enterComparisonOperator')
        self.current_comparison_operator = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.validate_exceptions(ctx, 'exitComparisonOperator')
        self.current_comparison_operator = ''

    # Enter a parse tree produced by tradeQueryParser#logicalOperator.
    def enterLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'enterLogicalOperator')
        self.current_logical_operator = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#logicalOperator.
    def exitLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.validate_exceptions(ctx, 'exitLogicalOperator')
        self.current_logical_operator = ''

    # Enter a parse tree produced by tradeQueryParser#functionCall.
    def enterFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'enterFunctionCall')
        self.current_function_call = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#functionCall.
    def exitFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.validate_exceptions(ctx, 'exitFunctionCall')
        self.current_function_call = ''

    # Enter a parse tree produced by tradeQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'enterNumericFunctionCall')
        self.function_depth_counter += 1
        self.current_numeric_function_call = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.validate_exceptions(ctx, 'exitNumericFunctionCall')
        self.function_depth_counter -= 1
        self.current_numeric_function_call = ''

    # Enter a parse tree produced by tradeQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'enterStringFunctionCall')
        self.current_string_function_call = self.concatenated_children
        self.current_function_tree[str(ctx)] = self.concatenated_children
        self.function_depth_counter += 1

    # Exit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.validate_exceptions(ctx, 'exitStringFunctionCall')
        if self.current_function_tree:
            _, self.current_function = self.current_function_tree.popitem(last=True)
        else:
            logging.error(f'[!] Error in processing at exitStringFunctionCall: FUNCTION TREE UNEXPECTEDLY EMPTY.')

        unchanged_params = self.extract_outermost_parentheses(self.current_function)
        _, has_sub_function, _ = self.contains_known_function(unchanged_params, self.all_string_funcs)

        if not has_sub_function:
            target_function_name = self.extract_function_name(self.concatenated_children)
            results = ''
            if target_function_name in ['upper', 'lower', 'capitalize', 'length', 'type', 'str']:
                results = self.custom_functions.transform_strings(unchanged_params, target_function_name)
            elif target_function_name == 'concat':
                results = self.custom_functions.ct_concat(unchanged_params)
            elif target_function_name == 'replace':
                start_string, string_to_replace, replace_with = \
                    [x.strip().strip('"') for x in unchanged_params.split(',')]
                results = self.custom_functions.replace_occurrences(start_string, string_to_replace, replace_with)

            for _key, resolved_function_string in self.current_function_tree.items():
                self.current_function_tree[_key] = resolved_function_string.replace(self.current_function, results)
            self.last_expression = self.last_expression.replace(self.current_function, results)

            # elif target_function_name in ['trim', 'ltrim', 'rtrim']:
            #     params = unchanged_params.split(',')
            #     params_len = len(params)
            #     initial_string = ''
            #     trim_string = ''
            #     if params_len == 2:
            #         initial_string, trim_string = sub_expression.split(',')
            #     elif params_len == 1:
            #         initial_string = ''
            #         trim_string = sub_expression
            #     else:
            #         logging.error(f'[!] Unknown String Operation Called. See exitStringFunctionCall.')
            #     results = self.custom_functions.trim_strings(initial_string, trim_string)

        self.function_depth_counter -= 1
        self.current_string_function_call = ''

    # Enter a parse tree produced by tradeQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'enterSpecificFunctionCall')
        self.function_depth_counter += 1
        self.current_specific_function_call = self.concatenated_children

    # Exit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.validate_exceptions(ctx, 'exitSpecificFunctionCall')
        self.function_depth_counter -= 1
        self.current_specific_function_call = ''

    # *************************************************************************************
    # Custom Functions
    # *************************************************************************************

    @staticmethod
    def concat_children(_children_list):
        return f"{' '.join([str(x) for x in _children_list])}"

    def validate_exceptions(self, ctx_obj, obj_identifier):
        if ctx_obj.exception:
            logging.error(f'[!] CUSTOM {obj_identifier} Error: {ctx_obj.exception}')
        if not ctx_obj.children:
            logging.error(f'[!] CUSTOM {obj_identifier}: NO CHILDREN!')
        if len(ctx_obj.children) <= 0:
            logging.error(f'[!] CUSTOM {obj_identifier}: CHILDREN ENTRIES BLANK!')

        if not self.last_expression:
            self.last_expression = self.concatenated_children
        else:
            if self.function_depth_counter > 1 and str(ctx_obj) in self.last_expression:
                self.last_expression = \
                    self.last_expression.replace(str(ctx_obj.parentCtx), self.concatenated_children, 1)
            else:
                self.last_expression = self.last_expression.replace(self.ctx_obj_str, self.concatenated_children, 1)
                for _, _val in self.current_function_tree.items():
                    self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concatenated_children, 1)

        self.ctx_obj_str = str(ctx_obj)
        self.concatenated_children = self.concat_children(ctx_obj.children)

        # self.ctx_obj_str = str(ctx_obj)
        # self.concatenated_children = self.concat_children(ctx_obj.children)
        # self.last_expression = self.last_expression.replace(self.ctx_obj_str, self.concatenated_children, 1)
        # for _, _val in self.current_function_tree.items():
        #     self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concatenated_children, 1)

    @staticmethod
    def is_valid_expression(expression, mode):
        """
        Check if the provided mathematical expression is valid based on the specified mode.
        Args:
            expression (str): The mathematical expression to evaluate.
            mode (str): The mode of operation, can be 'add_sub', 'mul_div', or 'any'.
        Returns:
            bool: True if the expression is valid, False otherwise.
        """
        # Define regex patterns for each mode
        patterns = {
            'add_sub': r'^[\d\s\+\-\(\)]+$',
            'mul_div': r'^[\d\s\*\/\(\)]+$',
            'any': r'^[\d\s\+\-\*\/\(\)]+$'
        }
        pattern = patterns.get(mode)

        if not pattern:
            logging.error("[x] Invalid mode provided. Choose from 'add_sub', 'mul_div', or 'any'.")
            return False

        if re.fullmatch(pattern, expression):
            logging.info("[i] The expression is valid for mode: {}".format(mode))
            return True
        else:
            logging.warning("[!] The expression is invalid for mode: {}".format(mode))
            return False

    def contains_known_sub_function(self, _string):
        for known_function_name in self.all_functions:
            if known_function_name in _string:
                return True
        return False

    def extract_function_name_and_params(self, _input_string):
        """
        Extracts the name of the function and its parameters from the provided text.
        Args:
            text (str): The string to search within.
            all_functions (list): A list of function names to look for.
        Returns:
            tuple: A tuple containing the name of the first matched function and its parameters as a string,
                   or (None, None) if no match is found.
        """
        # Prepare the function names for regex search (escape special characters and remove spaces)
        func_names = [re.escape(func.strip()) for func in self.all_funcs]

        # Join the function names with '|' to create a regex pattern that matches any of them
        pattern = r'((' + '|'.join(func_names) + r')\s*\((.*?)\))'

        # Use non-greedy matching to capture parameters
        match = re.search(pattern, _input_string, re.DOTALL)

        if match:
            return match.group(2), match.group(1)
        else:
            return None, None

    @staticmethod
    def extract_outermost_parentheses(_text):
        """
        Extracts text from the outermost parentheses, handling nested parentheses.
        Args:
            _text (str): The string to search.
        Returns:
            str: The text within the outermost parentheses, or None if no such text is found.
        """
        start = _text.find('(')
        if start == -1:
            return None  # No opening parenthesis found

        # Track levels of nested parentheses
        depth = 0
        for i in range(start, len(_text)):
            if _text[i] == '(':
                depth += 1
            elif _text[i] == ')':
                depth -= 1
                if depth == 0:
                    # Found the matching closing parenthesis for the outermost opening parenthesis
                    return _text[start + 1:i]  # Exclude the parentheses themselves

        return None  # No matching closing parenthesis found

    @staticmethod
    def process_ctx_children(ctx_children):
        # Concatenate with '###', then strip in the same order as the original
        concatenated = '###'.join([str(obj) for obj in ctx_children])
        stripped = concatenated.strip('\n').strip(' ').strip('\n').strip("###")
        # Split based on '###', which now reflects the original manipulation more closely
        split = stripped.split("###")
        # Convert to string again as takewhile checks against the string "\n", not the newline character itself
        return [obj for obj in takewhile(lambda x: str(x) != "\n", split)]

    def contains_known_function(self, input_string, known_functions, position=0, is_root=False):
        if not input_string:
            return None, None, 0
        expression = []
        has_nested_function = False
        while position < len(input_string):
            char = input_string[position]

            if char.isalpha() and not is_root:  # Potential function call
                start = position
                while position < len(input_string) and input_string[position].isalpha():
                    position += 1
                func_name = input_string[start:position]

                # Skip optional spaces between function name and '('
                while position < len(input_string) and input_string[position] == ' ':
                    position += 1

                if func_name in known_functions and input_string[position] == '(':
                    expression.append(func_name)
                    position += 1  # Skip '('
                    nested_expression, nested_has_nested_function, position = \
                        self.contains_known_function(input_string, known_functions, position)
                    expression.append(nested_expression)
                    has_nested_function = True  # Any found function is considered nested
                    continue

            if char == '(':
                position += 1  # Skip '('
                nested_expression, nested_has_nested_function, position = \
                    self.contains_known_function(input_string, known_functions, position)
                expression.append('(' + nested_expression + ')')
                if nested_has_nested_function:
                    has_nested_function = True
            elif char == ')':
                position += 1  # Skip ')'
                break  # End of current expression
            else:
                expression_start = position
                while position < len(input_string) and input_string[position] not in ',()':
                    position += 1
                expression.append(input_string[expression_start:position])

            # Skip delimiters
            if position < len(input_string) and input_string[position] in ', ':
                position += 1

        return ' '.join(expression), has_nested_function, position

    @staticmethod
    def extract_function_name(function_call: str) -> str:
        try:
            match = re.match(r'^(\w+)\s*\(', function_call)
            if match:
                return match.group(1)  # Return the first captured group, which is the function name
            else:
                logging.warning("[!] No function name found in the input.")
                return ""
        except Exception as e:
            logging.error(f"[x] Error extracting function name: {e}")
            return ""


del tradeQueryParser

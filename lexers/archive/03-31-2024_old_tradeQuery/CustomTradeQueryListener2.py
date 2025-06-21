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
        self.current_valid_line = ''
        self.file_custom_directive = None
        self.function_depth_counter = 0
        self.concatenated_children = ''
        self.ctx_obj_str = ''
        self.custom_functions = CustomFunctions()
        self.current_function_tree = OrderedDict()
        self.current_expression = {'ctx_obj_str': '', 'concatenated_children': '', 'type': ''}
        self.current_function = {'ctx_obj_str': '', 'concatenated_children': '', 'type': ''}
        self.from_function = {'ctx_obj_str': '', 'concatenated_children': '', 'type': ''}
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
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterProgram')

    # Exit a parse tree produced by tradeQueryParser#program.
    def exitProgram(self, ctx: tradeQueryParser.ProgramContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitProgram')

    # Enter a parse tree produced by tradeQueryParser#tradeQuery.
    def enterTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterTradeQuery')

        self.file_custom_directive = self.process_ctx_children(ctx.children)
        # Ensure that the initial TABLE directive is NOT MALFORMED
        if len(self.file_custom_directive) < 5:
            logging.error(f'[!] File Load Error: TABLE ENTRY MALFORMED')
        if self.file_custom_directive[1] != 'table' or self.file_custom_directive[2] != 'file':
            logging.error(f'[!] File Load Error: TABLE ENTRY MALFORMED')

    # Exit a parse tree produced by tradeQueryParser#tradeQuery.
    def exitTradeQuery(self, ctx: tradeQueryParser.TradeQueryContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitTradeQuery')
        logging.info(f'[+] Successfully processed valid TrueQRY syntax and collapsed expressions.')
        logging.info(f'[-] ORIGINAL QUERY:\n{self.original_query}\n\n[-]COLLAPSED QUERY:\n{self.collapsed_query}\n')
        pass

    # Enter a parse tree produced by tradeQueryParser#validLine.
    def enterValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterValidLine')
        self.current_valid_line = self.concatenated_children
        if self.current_directive == 'table':
            target = str(self.last_expression.strip(' ').strip('\n').strip(' ').strip('\n').split('\n')[0]) + '\n'
            self.collapsed_query += target

    # Exit a parse tree produced by tradeQueryParser#validLine.
    def exitValidLine(self, ctx: tradeQueryParser.ValidLineContext):
        # if self.current_directive == 'table':
        #     table_parts = self.last_expression.lstrip('\n').rstrip('\n').lstrip(' ').rstrip(' ').split('\n')
        #     self.file_directive = table_parts[0]
        #     self.collapsed_query = table_parts[0].strip("\n").strip(" ").strip("\n").strip(" ") + '\n'
        #     self.last_expression = self.concatenated_children
        # else:
        #     table_parts = self.last_expression.lstrip('\n').rstrip('\n').split('\n')
        #     self.last_expression = '\n'.join(table_parts[1:])
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitValidLine')
        self.collapsed_query += self.current_valid_line

    # Enter a parse tree produced by tradeQueryParser#directive.
    def enterDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterDirective')
        self.current_directive = str(ctx.children[0])

    # Exit a parse tree produced by tradeQueryParser#directive.
    def exitDirective(self, ctx: tradeQueryParser.DirectiveContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitDirective')
        x = 'test'
        # if '|' not in self.last_expression:
        #     logging.info(f'[+] Completed Query Successfully.')
        # self.last_expression = '\n'.join(self.last_expression.strip('\n').strip(' ').split('\n')[1:])
        # pass

    # Enter a parse tree produced by tradeQueryParser#expression.
    def enterExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterExpression')

    # Exit a parse tree produced by tradeQueryParser#expression.
    def exitExpression(self, ctx: tradeQueryParser.ExpressionContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitExpression')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#expressionString.
    def enterExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterExpressionString')

    # Exit a parse tree produced by tradeQueryParser#expressionString.
    def exitExpressionString(self, ctx: tradeQueryParser.ExpressionStringContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitExpressionString')

    # Enter a parse tree produced by tradeQueryParser#logicalStringExpr.
    def enterLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterLogicalStringExpr')

    # Exit a parse tree produced by tradeQueryParser#logicalStringExpr.
    def exitLogicalStringExpr(self, ctx: tradeQueryParser.LogicalStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitLogicalStringExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def enterArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterArithmeticStringExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticStringExpr.
    def exitArithmeticStringExpr(self, ctx: tradeQueryParser.ArithmeticStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitArithmeticStringExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#factorStringExpr.
    def enterFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterFactorStringExpr')

    # Exit a parse tree produced by tradeQueryParser#factorStringExpr.
    def exitFactorStringExpr(self, ctx: tradeQueryParser.FactorStringExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitFactorStringExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#expressionNumeric.
    def enterExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterExpressionNumeric')

    # Exit a parse tree produced by tradeQueryParser#expressionNumeric.
    def exitExpressionNumeric(self, ctx: tradeQueryParser.ExpressionNumericContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitExpressionNumeric')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#compareNumericExpr.
    def enterCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterCompareNumericExpr')

    # Exit a parse tree produced by tradeQueryParser#compareNumericExpr.
    def exitCompareNumericExpr(self, ctx: tradeQueryParser.CompareNumericExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitCompareNumericExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def enterArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = \
            self.validate_exceptions(ctx, 'enterArithmeticNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticNumericResultOnlyExpr.
    def exitArithmeticNumericResultOnlyExpr(self, ctx: tradeQueryParser.ArithmeticNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = \
            self.validate_exceptions(ctx, 'exitArithmeticNumericResultOnlyExpr')
        x = 'test'
        # if self.current_directive == 'eval':
        #     parts = self.last_expression.strip('\n').split('\n')
        #     expression = parts[0].split('=')[-1].strip(' ').strip('\n').strip(' ').strip('\n')
        #     if self.is_valid_expression(expression, 'any') and self.current_variable['name'] in parts[0]:
        #         eval_result = eval(expression)
        #         self.current_variable['value'] = eval_result
        #         if eval_result:
        #             self.all_variables[self.current_variable['name']] = self.current_variable['value']
        #         self.collapsed_query += f'{parts[0].strip(" ").replace(expression, str(eval_result))}\n'

    # Enter a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def enterTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterTermNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#termNumericResultOnlyExpr.
    def exitTermNumericResultOnlyExpr(self, ctx: tradeQueryParser.TermNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitTermNumericResultOnlyExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def enterFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterFactorNumericResultOnlyExpr')

    # Exit a parse tree produced by tradeQueryParser#factorNumericResultOnlyExpr.
    def exitFactorNumericResultOnlyExpr(self, ctx: tradeQueryParser.FactorNumericResultOnlyExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitFactorNumericResultOnlyExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#expressionAny.
    def enterExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterExpressionAny')

    # Exit a parse tree produced by tradeQueryParser#expressionAny.
    def exitExpressionAny(self, ctx: tradeQueryParser.ExpressionAnyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitExpressionAny')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#logicalExpr.
    def enterLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterLogicalExpr')

    # Exit a parse tree produced by tradeQueryParser#logicalExpr.
    def exitLogicalExpr(self, ctx: tradeQueryParser.LogicalExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitLogicalExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#compareExpr.
    def enterCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterCompareExpr')

    # Exit a parse tree produced by tradeQueryParser#compareExpr.
    def exitCompareExpr(self, ctx: tradeQueryParser.CompareExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitCompareExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#arithmeticExpr.
    def enterArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterArithmeticExpr')

    # Exit a parse tree produced by tradeQueryParser#arithmeticExpr.
    def exitArithmeticExpr(self, ctx: tradeQueryParser.ArithmeticExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitArithmeticExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#termExpr.
    def enterTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterTermExpr')

    # Exit a parse tree produced by tradeQueryParser#termExpr.
    def exitTermExpr(self, ctx: tradeQueryParser.TermExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitTermExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#factorExpr.
    def enterFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterFactorExpr')

    # Exit a parse tree produced by tradeQueryParser#factorExpr.
    def exitFactorExpr(self, ctx: tradeQueryParser.FactorExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitFactorExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#value.
    def enterValue(self, ctx: tradeQueryParser.ValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterValue')

    # Exit a parse tree produced by tradeQueryParser#value.
    def exitValue(self, ctx: tradeQueryParser.ValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#valueStringOnly.
    def enterValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterValueStringOnly')

    # Exit a parse tree produced by tradeQueryParser#valueStringOnly.
    def exitValueStringOnly(self, ctx: tradeQueryParser.ValueStringOnlyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitValueStringOnly')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def enterValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterValueNumericResultOnly')

    # Exit a parse tree produced by tradeQueryParser#valueNumericResultOnly.
    def exitValueNumericResultOnly(self, ctx: tradeQueryParser.ValueNumericResultOnlyContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitValueNumericResultOnly')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#unaryExpr.
    def enterUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterUnaryExpr')

    # Exit a parse tree produced by tradeQueryParser#unaryExpr.
    def exitUnaryExpr(self, ctx: tradeQueryParser.UnaryExprContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitUnaryExpr')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#numericValue.
    def enterNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterNumericValue')

    # Exit a parse tree produced by tradeQueryParser#numericValue.
    def exitNumericValue(self, ctx: tradeQueryParser.NumericValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitNumericValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#booleanValue.
    def enterBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterBooleanValue')

    # Exit a parse tree produced by tradeQueryParser#booleanValue.
    def exitBooleanValue(self, ctx: tradeQueryParser.BooleanValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitBooleanValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#stringValue.
    def enterStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterStringValue')
        if self.current_directive == 'table':
            self.files.append(f'{self.script_directory}/{self.target_index_uri}/{self.concatenated_children}')

    # Exit a parse tree produced by tradeQueryParser#stringValue.
    def exitStringValue(self, ctx: tradeQueryParser.StringValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitStringValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#arrayValue.
    def enterArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterArrayValue')

    # Exit a parse tree produced by tradeQueryParser#arrayValue.
    def exitArrayValue(self, ctx: tradeQueryParser.ArrayValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitArrayValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def enterArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterArrayValueNumeric')

    # Exit a parse tree produced by tradeQueryParser#arrayValueNumeric.
    def exitArrayValueNumeric(self, ctx: tradeQueryParser.ArrayValueNumericContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitArrayValueNumeric')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#variableValue.
    def enterVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterVariableValue')
        # if self.current_directive == 'eval':
        #     self.current_variable = {'name': str(ctx.children[0]), 'value': None}

    # Exit a parse tree produced by tradeQueryParser#variableValue.
    def exitVariableValue(self, ctx: tradeQueryParser.VariableValueContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitVariableValue')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#comparisonOperator.
    def enterComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterComparisonOperator')

    # Exit a parse tree produced by tradeQueryParser#comparisonOperator.
    def exitComparisonOperator(self, ctx: tradeQueryParser.ComparisonOperatorContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitComparisonOperator')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#logicalOperator.
    def enterLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterLogicalOperator')

    # Exit a parse tree produced by tradeQueryParser#logicalOperator.
    def exitLogicalOperator(self, ctx: tradeQueryParser.LogicalOperatorContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitLogicalOperator')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#functionCall.
    def enterFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#functionCall.
    def exitFunctionCall(self, ctx: tradeQueryParser.FunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitFunctionCall')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#numericFunctionCall.
    def enterNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterNumericFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#numericFunctionCall.
    def exitNumericFunctionCall(self, ctx: tradeQueryParser.NumericFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitNumericFunctionCall')
        x = 'test'

    # Enter a parse tree produced by tradeQueryParser#stringFunctionCall.
    def enterStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterStringFunctionCall')
        self.function_depth_counter += 1
        self.current_function_tree[self.concatenated_children] = self.concatenated_children
        self.current_function = {'ctx_obj_str': self.ctx_obj_str,
                                 'concatenated_children': self.concatenated_children,
                                 'type': 'enterStringFunctionCall'}

        if self.from_function['ctx_obj_str']:
            pass
        else:
            self.from_function = {'ctx_obj_str': self.ctx_obj_str,
                                  'concatenated_children': self.concatenated_children,
                                  'type': 'enterStringFunctionCall'}

    # Exit a parse tree produced by tradeQueryParser#stringFunctionCall.
    def exitStringFunctionCall(self, ctx: tradeQueryParser.StringFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitStringFunctionCall')
        # self.extract_function_name(self.concatenated_children)
        if self.concatenated_children in self.current_function_tree:
            self.current_function['concatenated_children'] = self.current_function_tree[self.concatenated_children]
            self.concatenated_children = self.current_function_tree[self.concatenated_children]
        self.function_depth_counter -= 1
        unchanged_params = self.extract_outermost_parentheses(
            self.current_function['concatenated_children'])
        sub_expression, has_sub_function, _ = \
            self.contains_known_function(
                self.extract_outermost_parentheses(
                    self.current_function['concatenated_children']), self.all_string_funcs)

        if not has_sub_function and sub_expression:
            target_function_name = self.extract_function_name(self.concatenated_children)
            results = ''
            if target_function_name in ['upper', 'lower', 'capitalize', 'length', 'type', 'str']:
                results = self.custom_functions.transform_strings(unchanged_params, target_function_name)
            elif target_function_name == 'concat':
                results = self.custom_functions.ct_concat(unchanged_params)
            elif target_function_name == 'replace':
                start_string, string_to_replace, replace_with = unchanged_params.split(',')
                results = self.custom_functions.replace_occurrences(start_string, string_to_replace, replace_with)
            elif target_function_name in ['trim', 'ltrim', 'rtrim']:
                params = unchanged_params.split(',')
                params_len = len(params)
                initial_string = ''
                trim_string = ''
                if params_len == 2:
                    initial_string, trim_string = sub_expression.split(',')
                elif params_len == 1:
                    initial_string = ''
                    trim_string = sub_expression
                else:
                    logging.error(f'[!] Unknown String Operation Called. See exitStringFunctionCall.')

                results = self.custom_functions.trim_strings(initial_string, trim_string)

            # self.current_valid_line = self.current_valid_line\
            #     .replace(self.current_function['concatenated_children'], results, 1)
            # for _, _val in self.current_function_tree.items():
            #     self.current_function_tree[_] = _val.replace(self.current_function['concatenated_children'], results, 1)
            # self.last_expression = \
            #     self.last_expression.replace(self.current_function['concatenated_children'], results, 1)
            # self.from_function['concatenated_children'] = \
            #     self.from_function['concatenated_children']\
            #         .replace(self.current_function['concatenated_children'], results, 1)
            # self.current_function['concatenated_children'] = self.current_function['concatenated_children'].\
            #     replace(self.current_function['concatenated_children'], results, 1)

        # self.current_function = {'ctx_obj_str': '', 'concatenated_children': '', 'type': 'exitStringFunctionCall'}

    # Enter a parse tree produced by tradeQueryParser#specificFunctionCall.
    def enterSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'enterSpecificFunctionCall')

    # Exit a parse tree produced by tradeQueryParser#specificFunctionCall.
    def exitSpecificFunctionCall(self, ctx: tradeQueryParser.SpecificFunctionCallContext):
        self.ctx_obj_str, self.concatenated_children = self.validate_exceptions(ctx, 'exitSpecificFunctionCall')
        x = 'test'

    # *************************************************************************************
    # Custom Functions
    # *************************************************************************************

    # def load_tables_to_pf(self):
    #     # TBD - Need to define the complex ways in which we can call
    #     self.main_df = ''
    #     if self.current_enterExpression[0] != 'file' and self.current_enterDirective[1] != '=':
    #         logging.warning(f'[!] Error in load_tables_to_pf(): LOAD TABLE EXPRESSION MALFORMED.')
    #
    #     for _file_name in self.current_enterExpression[2:]:
    #         if _file_name != ',':
    #             _file_name = _file_name.replace('"', '')
    #             logging.info(f'[+] Opening FILE: {_file_name}')
    #             self.files.append(f'{self.script_directory}/indexes/test_parquets/{_file_name}')
    #         # TBD --> Build a function that robustly opens an individual FILE with or without WHERE clause

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

        self.ctx_obj_str = str(ctx_obj).strip()
        self.concatenated_children = self.concat_children(ctx_obj.children)

        # if self.function_depth_counter > 0:
        #     self.current_function['concatenated_children'] = \
        #         self.current_function['concatenated_children'].replace(self.ctx_obj_str, self.concatenated_children, 1)
        #     self.from_function['concatenated_children'] = \
        #         self.from_function['concatenated_children'].replace(self.ctx_obj_str, self.concatenated_children, 1)
        #     for _, _val in self.current_function_tree.items():
        #         self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concatenated_children, 1)
        # if self.concatenated_children:
        #     self.current_valid_line = self.current_valid_line.replace(self.ctx_obj_str, self.concatenated_children, 1)
        #     if self.ctx_obj_str in self.last_expression:
        #         self.last_expression = self.last_expression.replace(self.ctx_obj_str, self.concatenated_children, 1)
        #         for _, _val in self.current_function_tree.items():
        #             self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concatenated_children, 1)
        #     elif self.last_expression == '':
        #         self.last_expression = self.concatenated_children

        if self.function_depth_counter > 1 and str(ctx_obj) in self.last_expression:
            self.last_expression = \
                self.last_expression.replace(str(ctx_obj.parentCtx), self.concatenated_children, 1)
        else:
            self.last_expression = self.last_expression.replace(self.ctx_obj_str, self.concatenated_children, 1)
            for _, _val in self.current_function_tree.items():
                self.current_function_tree[_] = _val.replace(self.ctx_obj_str, self.concatenated_children, 1)

        self.ctx_obj_str = str(ctx_obj)
        self.concatenated_children = self.concat_children(ctx_obj.children)

        return self.ctx_obj_str, self.concatenated_children

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

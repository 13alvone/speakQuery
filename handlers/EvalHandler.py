#!/usr/bin/env python3
"""
EvalHandler.py
Purpose: Implements evaluation of SPL eval commands. This module supports functions
         such as if_/case/tonumber/concat/round and uses a custom evaluation environment
         to support nested and complex expressions.
"""

import logging
import numpy as np
import pandas as pd
import base64
import ast
import operator

from handlers.MathematicOperations import MathHandler
from handlers.StringHandler import StringHandler
from handlers.GeneralHandler import GeneralHandler

logger = logging.getLogger(__name__)


class EvalHandler:
    def __init__(self):
        self.math_handler = MathHandler()
        self.string_handler = StringHandler()
        self.general_handler = GeneralHandler()

    def run_eval(self, eval_tokens, df):
        """
        Expects eval_tokens in the form:
          ['eval', <assignment>, <assignment>, ... ]
        Each assignment must follow the format: field = expression.
        The expressions are evaluated via safe_eval and the results assigned to new
        columns in df.
        """
        # If eval_tokens is a list, join tokens starting at index 1 (skip the "eval" directive)
        if isinstance(eval_tokens, list):
            assignments_str = " ".join(token.strip() for token in eval_tokens[1:])
        else:
            assignments_str = eval_tokens

        # Split assignments at the top level (by commas that are not nested)
        assignment_list = self.split_arguments(assignments_str)
        for assignment in assignment_list:
            parts = assignment.split("=", 1)
            if len(parts) != 2:
                raise ValueError("Invalid assignment format: " + assignment)
            field = parts[0].strip()
            expr = parts[1].strip()
            result = self.safe_eval(expr, df)
            df[field] = result
        return df

    def safe_eval(self, expr, df):
        """
        Evaluates an expression string in the context of DataFrame df.
        It performs minimal normalization by stripping the entire expression and
        ensuring that function names are immediately followed by '('.
        Dedicated branches handle functions such as concat and round; if any
        special keyword is detected the expression is delegated to custom_eval.
        Otherwise, it falls back to pandas.eval.
        """
        import re
        expr = expr.strip()
        # Collapse spaces between an identifier and an opening parenthesis.
        expr = re.sub(r'([a-zA-Z_][a-zA-Z_0-9]*)\s*\(', r'\1(', expr)
        try:
            if expr.startswith("concat("):
                inner = expr[expr.find("(") + 1:expr.rfind(")")]
                args = [arg.strip() for arg in self.split_arguments(inner)]
                length = len(df)
                evaluated_args = []
                for arg in args:
                    # If the token is a quoted string, preserve the internal spaces.
                    if arg.startswith("\"") and arg.endswith("\""):
                        value = arg.strip("\"")
                    elif arg.startswith("tostring("):
                        inner_arg = arg[arg.find("(") + 1:arg.rfind(")")].strip()
                        # Remove all whitespace if not quoted.
                        if not (inner_arg.startswith("\"") and inner_arg.endswith("\"")):
                            inner_arg = inner_arg.replace(" ", "")
                        value = df[inner_arg].astype(str)
                    elif arg.startswith("lower("):
                        inner_arg = arg[arg.find("(") + 1:arg.rfind(")")].strip()
                        if not (inner_arg.startswith("\"") and inner_arg.endswith("\"")):
                            inner_arg = inner_arg.replace(" ", "")
                        value = df[inner_arg].str.lower()
                    elif arg.startswith("upper("):
                        inner_arg = arg[arg.find("(") + 1:arg.rfind(")")].strip()
                        if not (inner_arg.startswith("\"") and inner_arg.endswith("\"")):
                            inner_arg = inner_arg.replace(" ", "")
                        value = df[inner_arg].str.upper()
                    else:
                        try:
                            value = pd.to_numeric(arg)
                        except Exception:
                            key = arg.replace(" ", "")  # remove all spaces for identifier lookup
                            value = df[key] if key in df.columns else arg
                    evaluated_args.append(self.ensure_series(value, length))
                result = self.vectorized_concat(*evaluated_args)
                return result

            elif expr.startswith("round("):
                inner = expr[expr.find("(") + 1:expr.rfind(")")]
                args = [arg.strip() for arg in self.split_arguments(inner)]
                if len(args) == 1:
                    value = self.safe_eval(args[0], df)
                    precision = 0
                elif len(args) == 2:
                    value = self.safe_eval(args[0], df)
                    precision = int(args[1])
                else:
                    raise ValueError("round expects 1 or 2 arguments")
                return self.math_handler.complex_round(value, precision)

            special_keywords = [
                "if_", "case", "tonumber",
                "lower(", "upper(", "trim(", "ltrim(", "rtrim(", "randomize(", "avg(", "min(", "max(", "sum(", "median(", "mode(",
                "sqrt(", "abs(", "random("
            ]
            if any(keyword in expr for keyword in special_keywords):
                return self.custom_eval(expr, df)
            else:
                return df.eval(expr)
        except Exception as e:
            logging.error(f"[x] Error in safe_eval for expression '{expr}': {e}")
            raise

    def custom_eval(self, expr, df):
        """
        Evaluates the expression using a custom local environment that defines our
        special functions, including nested support.
        """
        expr = expr.strip()
        local_env = {col: df[col] for col in df.columns}
        # Also add trimmed keys for flexibility.
        local_env.update({col.replace(" ", ""): df[col] for col in df.columns})
        local_env.update({
            "if_": lambda cond, true_val, false_val: pd.Series(
                        np.where(
                            cond,
                            true_val if isinstance(true_val, pd.Series) else np.repeat(true_val, len(df)),
                            false_val if isinstance(false_val, pd.Series) else np.repeat(false_val, len(df))
                        ), index=df.index),
            "tonumber": lambda x: pd.to_numeric(x),
            "lower": lambda x: x.str.lower() if isinstance(x, pd.Series) else x.lower(),
            "upper": lambda x: x.str.upper() if isinstance(x, pd.Series) else x.upper(),
            "round": round,
            "concat": lambda *args: self.vectorized_concat(*[self.ensure_series(arg, len(df)) for arg in args]),
            "case": lambda *args: self.case_func(*args, df=df),
            "tostring": lambda x: x.astype(str) if isinstance(x, pd.Series) else str(x),
            "trim": lambda x: StringHandler().trim_strings(x, " ", "trim"),
            "ltrim": lambda x: StringHandler().trim_strings(x, " ", "ltrim"),
            "rtrim": lambda x: StringHandler().trim_strings(x, " ", "rtrim"),
            "randomize": lambda x: self.math_handler.complex_randomize(x) if isinstance(x, (pd.Series, list))
                                     else self.math_handler.complex_randomize(float(x)),
            "avg": lambda a, b: (a + b) / 2,  # example avg definition
            "coalesce": lambda *args: pd.Series(
                GeneralHandler.coalesce_lists([self.ensure_series(a, len(df)).tolist() for a in args])
            ),
            "isnull": lambda x: x.isnull() if isinstance(x, pd.Series) else pd.isna(x),
            "isnotnull": lambda x: x.notnull() if isinstance(x, pd.Series) else not pd.isna(x),
            "base64_encode": lambda x: x.apply(lambda v: base64.b64encode(str(v).encode()).decode())
            if isinstance(x, pd.Series) else base64.b64encode(str(x).encode()).decode(),
            "base64_decode": lambda x: x.apply(lambda v: base64.b64decode(str(v)).decode())
            if isinstance(x, pd.Series) else base64.b64decode(str(x)).decode()
        })
        try:
            # Parse the expression into an AST
            tree = ast.parse(expr, mode="eval")

            allowed_funcs = set(local_env.keys())

            class SafeEvaluator(ast.NodeVisitor):
                allowed_operators = {
                    ast.Add: operator.add,
                    ast.Sub: operator.sub,
                    ast.Mult: operator.mul,
                    ast.Div: operator.truediv,
                    ast.Mod: operator.mod,
                    ast.Pow: operator.pow,
                    ast.FloorDiv: operator.floordiv,
                }

                allowed_unary = {ast.UAdd: operator.pos, ast.USub: operator.neg, ast.Not: operator.not_}

                allowed_bool = {ast.And: lambda a, b: a & b, ast.Or: lambda a, b: a | b}

                allowed_compare = {
                    ast.Eq: operator.eq,
                    ast.NotEq: operator.ne,
                    ast.Lt: operator.lt,
                    ast.LtE: operator.le,
                    ast.Gt: operator.gt,
                    ast.GtE: operator.ge,
                }

                def __init__(self, env):
                    self.env = env

                def visit(self, node):
                    return super().visit(node)

                def generic_visit(self, node):
                    raise ValueError(f"Unsupported expression: {ast.dump(node)}")

                def visit_Expression(self, node):
                    return self.visit(node.body)

                def visit_Name(self, node):
                    if node.id in self.env:
                        return self.env[node.id]
                    raise ValueError(f"Use of name '{node.id}' is not allowed")

                def visit_Constant(self, node):
                    return node.value

                # Python <3.8 compatibility
                def visit_Num(self, node):
                    return node.n

                def visit_BinOp(self, node):
                    if type(node.op) not in self.allowed_operators:
                        raise ValueError("Operator not allowed")
                    left = self.visit(node.left)
                    right = self.visit(node.right)
                    return self.allowed_operators[type(node.op)](left, right)

                def visit_UnaryOp(self, node):
                    if type(node.op) not in self.allowed_unary:
                        raise ValueError("Unary operator not allowed")
                    operand = self.visit(node.operand)
                    return self.allowed_unary[type(node.op)](operand)

                def visit_BoolOp(self, node):
                    if type(node.op) not in self.allowed_bool:
                        raise ValueError("Boolean operator not allowed")
                    values = [self.visit(v) for v in node.values]
                    result = values[0]
                    for v in values[1:]:
                        result = self.allowed_bool[type(node.op)](result, v)
                    return result

                def visit_Compare(self, node):
                    left = self.visit(node.left)
                    results = []
                    for op, comp in zip(node.ops, node.comparators):
                        if type(op) not in self.allowed_compare:
                            raise ValueError("Comparison operator not allowed")
                        right = self.visit(comp)
                        results.append(self.allowed_compare[type(op)](left, right))
                        left = right
                    result = results[0]
                    for r in results[1:]:
                        result = result & r
                    return result

                def visit_Call(self, node):
                    if not isinstance(node.func, ast.Name):
                        raise ValueError("Only direct function calls allowed")
                    func_name = node.func.id
                    if func_name not in allowed_funcs or not callable(self.env.get(func_name)):
                        raise ValueError(f"Function '{func_name}' is not allowed")
                    args = [self.visit(a) for a in node.args]
                    kwargs = {kw.arg: self.visit(kw.value) for kw in node.keywords}
                    return self.env[func_name](*args, **kwargs)

            evaluator = SafeEvaluator(local_env)
            result = evaluator.visit(tree)
            return result
        except Exception as e:
            logging.error(f"[x] Error in custom_eval for expression '{expr}': {e}")
            raise

    def case_func(self, *args, df):
        """
        Evaluates a case statement in a vectorized manner.
        Arguments must be provided as condition, result pairs, with an optional default.
        """
        try:
            length = len(df)
            series_args = [self.ensure_series(arg, length) for arg in args]
            if len(series_args) % 2 == 1:
                default = series_args[-1]
                pairs = series_args[:-1]
            else:
                default = pd.Series([None] * length, index=df.index)
                pairs = series_args
            result = default
            # Process pairs in reverse order
            for i in range(len(pairs) - 2, -1, -2):
                condition = pairs[i]
                value = pairs[i+1]
                result = pd.Series(np.where(condition, value, result), index=df.index)
            return result
        except Exception as e:
            logging.error(f"[x] Error in case_func: {e}")
            raise

    def ensure_series(self, arg, length):
        """Ensures that arg is a pandas Series of the given length."""
        if isinstance(arg, pd.Series):
            if len(arg) != length:
                return pd.Series(np.resize(arg.values, length), index=range(length))
            return arg
        elif isinstance(arg, (int, float, str)):
            return pd.Series([arg] * length)
        else:
            try:
                s = pd.Series(arg)
                if len(s) != length:
                    s = pd.Series(np.resize(s.values, length))
                return s
            except Exception as e:
                logging.error(f"[x] Error converting argument to series: {e}")
                raise

    def vectorized_concat(self, *args):
        """
        Concatenates a list of Series (or scalars converted to Series) elementwise.
        """
        try:
            target_length = None
            for arg in args:
                if isinstance(arg, pd.Series):
                    target_length = len(arg)
                    break
            if target_length is None:
                target_length = len(pd.Series(args[0]))
            series_list = [self.ensure_series(arg, target_length) for arg in args]
            result = series_list[0].astype(str)
            for s in series_list[1:]:
                result = result + s.astype(str)
            return result
        except Exception as e:
            logging.error(f"[x] Error in vectorized_concat: {e}")
            raise

    def split_arguments(self, arg_str):
        """
        Splits a comma-separated string into arguments, taking nesting and quoted strings into account.
        Commas inside quotes or parentheses are not treated as delimiters.
        """
        args = []
        current = ""
        depth = 0
        in_quote = False
        quote_char = None

        for char in arg_str:
            # If the character is a quote and we're not already inside a quote:
            if char in ('"', "'"):
                if not in_quote:
                    in_quote = True
                    quote_char = char
                elif char == quote_char:
                    in_quote = False
                    quote_char = None
                current += char
            # If we're inside a quote, add the character verbatim.
            elif in_quote:
                current += char
            # When not in a quote, manage parentheses depth.
            else:
                if char == "(":
                    depth += 1
                    current += char
                elif char == ")":
                    depth -= 1
                    current += char
                # Only split on commas if at top-level (depth 0) and not in a quote.
                elif char == "," and depth == 0:
                    args.append(current.strip())
                    current = ""
                else:
                    current += char
        if current.strip():
            args.append(current.strip())
        return args

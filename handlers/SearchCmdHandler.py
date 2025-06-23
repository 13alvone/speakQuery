#!/usr/bin/env python3

"""
search_directive.py

A class-based Python module that mirrors the logical and comparison
filtering from the C++ index call code, but applies it to an already-loaded
Pandas DataFrame. The filtering is guided by a list of tokens
(e.g., ['status', '=', '"success"', 'x', '>', '5', 'earliest', '=', '"2024-01-07"']).

Usage in your app.py or similar:
    from search_directive import SearchDirective
    import pandas as pd

    sd = SearchDirective()
    df = pd.read_csv("some_data.csv")  # or however you load your data
    tokens = ['level', '=', '"ERROR"', ...]  # e.g., from a query
    filtered_df = sd.run_search(tokens, df)

    # filtered_df is now your reduced DataFrame
"""

import logging
import re
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)


class SearchDirective:
    """
    Encapsulates logic for tokenizing, parsing, and applying a "search" directive
    against an existing Pandas DataFrame.
    """

    class TokenType:
        IDENTIFIER = "IDENTIFIER"
        STRING_LITERAL = "STRING_LITERAL"
        NUMBER_LITERAL = "NUMBER_LITERAL"
        OPERATOR = "OPERATOR"
        PARENTHESIS = "PARENTHESIS"
        COMMA = "COMMA"
        LITERAL = "LITERAL"

    class ASTNodeType:
        COMPARISON = "COMPARISON"
        LOGICAL_OP = "LOGICAL_OP"
        IN_CLAUSE = "IN_CLAUSE"
        IDENTIFIER = "IDENTIFIER"
        LITERAL = "LITERAL"

    class Token:
        def __init__(self, token_type: str, value: str):
            self.type = token_type
            self.value = value

    class ASTNode:
        def __init__(self, node_type: str):
            self.node_type = node_type
            self.operator_: Optional[str] = None
            self.identifier: Optional[str] = None
            self.values: List[str] = []
            self.left: Optional['SearchDirective.ASTNode'] = None
            self.right: Optional['SearchDirective.ASTNode'] = None
            self.literal_or_ident: Optional[str] = None

    # ------------------------------------------------------------------
    # Public method to call for applying the "search" directive
    # ------------------------------------------------------------------
    def run_search(self, search_tokens: List[str], df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a list of token strings (e.g. ['status', '=', '"success"', 'x', '>', '5']),
        build an AST, convert it to a Pandas query, and apply it to 'df'.

        Returns a filtered DataFrame or empty DataFrame if errors occur or if nothing matches.
        """

        if not search_tokens:
            logging.info("[i] No search tokens provided. Returning the original DataFrame.")
            return df

        logging.info(f"[i] Received search tokens: {search_tokens}")

        try:
            # 1) Convert raw string tokens -> Token objects
            token_list = self.tokenize_query_tokens(search_tokens)
            if not token_list:
                logging.info("[i] Could not produce any tokens. Returning original DataFrame.")
                return df

            # 2) Parse tokens -> AST
            parser = self.Parser(token_list)
            ast_root = parser.parse_expression()

            # 3) Convert AST -> Pandas query string
            pandas_query_str = self.ast_to_query(ast_root)
            if not pandas_query_str:
                pandas_query_str = "True"

            logging.info(f"[i] Generated Pandas query: {pandas_query_str}")

            # 4) Apply the filter
            if pandas_query_str == "True":
                logging.info("[i] Pandas query is 'True'; no filtering needed.")
                return df

            try:
                filtered_df = df.query(pandas_query_str)
                logging.info(f"[i] DataFrame filtered. Rows before: {len(df.index)}; after: {len(filtered_df.index)}.")
                return filtered_df
            except Exception as ex:
                logging.error(f"[x] Error while applying Pandas query: {ex}")
                return pd.DataFrame()

        except Exception as e:
            logging.error(f"[x] Exception in run_search: {str(e)}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Tokenizer
    # ------------------------------------------------------------------
    def tokenize_query_tokens(self, raw_tokens: List[str]) -> List['SearchDirective.Token']:
        """
        Convert raw token strings into Token objects. This parallels the
        C++ 'tokenize_query_tokens' function.
        """
        tokens: List[SearchDirective.Token] = []
        for rt in raw_tokens:
            if rt in ["(", ")"]:
                tokens.append(self.Token(self.TokenType.PARENTHESIS, rt))
            elif rt.upper() in ["=", "!=", "<", ">", "<=", ">=", "AND", "OR", "IN"]:
                # Convert operators to uppercase
                tokens.append(self.Token(self.TokenType.OPERATOR, rt.upper()))
            elif rt == ",":
                tokens.append(self.Token(self.TokenType.COMMA, rt))
            elif rt in ["True", "False"]:
                tokens.append(self.Token(self.TokenType.LITERAL, rt))
            else:
                # Distinguish between string-literal, number-literal, and identifier
                if len(rt) >= 2 and rt.startswith('"') and rt.endswith('"'):
                    # It's a quoted string literal
                    trimmed = rt[1:-1]
                    tokens.append(self.Token(self.TokenType.STRING_LITERAL, trimmed))
                elif rt.isdigit():
                    # numeric literal
                    tokens.append(self.Token(self.TokenType.NUMBER_LITERAL, rt))
                else:
                    # otherwise treat as identifier
                    tokens.append(self.Token(self.TokenType.IDENTIFIER, rt))

        return tokens

    # ------------------------------------------------------------------
    # Parser (similar to the C++ Parser)
    # ------------------------------------------------------------------
    class Parser:
        def __init__(self, tokens: List['SearchDirective.Token']):
            self.tokens = tokens
            self.index = 0

        def parse_expression(self) -> 'SearchDirective.ASTNode':
            return self.parse_or()

        def parse_or(self) -> 'SearchDirective.ASTNode':
            left_node = self.parse_and()
            while self.peek_operator("OR"):
                self.get_next_token()  # consume "OR"
                right_node = self.parse_and()
                new_node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.LOGICAL_OP)
                new_node.operator_ = "|"
                new_node.left = left_node
                new_node.right = right_node
                left_node = new_node
            return left_node

        def parse_and(self) -> 'SearchDirective.ASTNode':
            left_node = self.parse_comparison()
            while True:
                if self.peek_operator("AND"):
                    self.get_next_token()  # consume "AND"
                    right_node = self.parse_comparison()
                    new_node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.LOGICAL_OP)
                    new_node.operator_ = "&"
                    new_node.left = left_node
                    new_node.right = right_node
                    left_node = new_node
                elif self.peek_implicit_and():
                    right_node = self.parse_comparison()
                    new_node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.LOGICAL_OP)
                    new_node.operator_ = "&"
                    new_node.left = left_node
                    new_node.right = right_node
                    left_node = new_node
                else:
                    break
            return left_node

        def parse_comparison(self) -> 'SearchDirective.ASTNode':
            # Parenthesized sub-expression
            if self.peek_parenthesis("("):
                self.get_next_token()  # consume '('
                node = self.parse_expression()
                if not self.peek_parenthesis(")"):
                    raise ValueError("[x] Expected ')' after sub-expression.")
                self.get_next_token()  # consume ')'
                return node

            left_operand = self.parse_operand()
            if self.has_next() and self.current_token().type == SearchDirective.TokenType.OPERATOR:
                op = self.current_token().value
                self.get_next_token()  # consume operator
                if op == "IN":
                    if left_operand.node_type != SearchDirective.ASTNodeType.IDENTIFIER:
                        raise ValueError("[x] Expected identifier before IN operator.")
                    if not self.peek_parenthesis("("):
                        raise ValueError("[x] Expected '(' after IN operator.")
                    self.get_next_token()  # consume '('

                    values: List[str] = []
                    while True:
                        if not self.has_next():
                            raise ValueError("[x] Unexpected end of IN clause.")
                        if self.peek_parenthesis(")"):
                            self.get_next_token()  # consume ')'
                            break
                        elif self.current_token().type == SearchDirective.TokenType.COMMA:
                            self.get_next_token()  # skip comma
                            continue
                        elif self.current_token().type in [
                            SearchDirective.TokenType.STRING_LITERAL,
                            SearchDirective.TokenType.NUMBER_LITERAL,
                            SearchDirective.TokenType.IDENTIFIER,
                            SearchDirective.TokenType.LITERAL
                        ]:
                            val_token = self.current_token()
                            values.append(self.normalize_literal(val_token))
                            self.get_next_token()
                        else:
                            raise ValueError("[x] Unexpected token in IN clause.")

                    in_node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.IN_CLAUSE)
                    in_node.identifier = left_operand.literal_or_ident
                    in_node.values = values
                    return in_node
                else:
                    # standard comparison operator (=, !=, <, >, <=, >=)
                    right_operand = self.parse_operand()
                    cmp_node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.COMPARISON)
                    cmp_node.operator_ = op
                    cmp_node.left = left_operand
                    cmp_node.right = right_operand
                    return cmp_node
            return left_operand

        def parse_operand(self) -> 'SearchDirective.ASTNode':
            if not self.has_next():
                raise ValueError("[x] Unexpected end of expression while parsing operand.")

            tok = self.current_token()
            self.get_next_token()  # consume

            if tok.type == SearchDirective.TokenType.IDENTIFIER:
                # Validate a suitable identifier
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', tok.value):
                    raise ValueError(f"[x] Invalid identifier: '{tok.value}'")
                node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.IDENTIFIER)
                node.literal_or_ident = tok.value
                return node

            elif tok.type in [
                SearchDirective.TokenType.STRING_LITERAL,
                SearchDirective.TokenType.NUMBER_LITERAL,
                SearchDirective.TokenType.LITERAL
            ]:
                node = SearchDirective.ASTNode(SearchDirective.ASTNodeType.LITERAL)
                node.literal_or_ident = self.normalize_literal(tok)
                return node

            else:
                raise ValueError(f"[x] Unexpected token type: {tok.type} = {tok.value}")

        @staticmethod
        def normalize_literal(tok: 'SearchDirective.Token') -> str:
            """
            Convert a token into a string suitable for Pandas query. For example:
            - Strings get single-quoted
            - True/False remain unquoted booleans
            - Numbers remain as-is
            """
            if tok.type == SearchDirective.TokenType.STRING_LITERAL:
                escaped = tok.value.replace("'", "\\'")
                return f"'{escaped}'"
            elif tok.type == SearchDirective.TokenType.NUMBER_LITERAL:
                return tok.value
            elif tok.type == SearchDirective.TokenType.LITERAL:
                if tok.value in ["True", "False"]:
                    return tok.value
                else:
                    return f"'{tok.value}'"
            elif tok.type == SearchDirective.TokenType.IDENTIFIER:
                return tok.value
            else:
                raise ValueError("[x] Could not normalize token.")

        def current_token(self) -> 'SearchDirective.Token':
            return self.tokens[self.index]

        def has_next(self) -> bool:
            return self.index < len(self.tokens)

        def get_next_token(self) -> 'SearchDirective.Token':
            tok = self.tokens[self.index]
            self.index += 1
            return tok

        def peek_operator(self, op: str) -> bool:
            if self.has_next():
                nt = self.tokens[self.index]
                return nt.type == SearchDirective.TokenType.OPERATOR and nt.value == op
            return False

        def peek_parenthesis(self, paren: str) -> bool:
            if self.has_next():
                nt = self.tokens[self.index]
                return nt.type == SearchDirective.TokenType.PARENTHESIS and nt.value == paren
            return False

        def peek_implicit_and(self) -> bool:
            if not self.has_next():
                return False
            nt = self.tokens[self.index]
            if nt.type in [
                SearchDirective.TokenType.IDENTIFIER,
                SearchDirective.TokenType.STRING_LITERAL,
                SearchDirective.TokenType.NUMBER_LITERAL,
                SearchDirective.TokenType.LITERAL
            ]:
                return True
            if nt.type == SearchDirective.TokenType.PARENTHESIS and nt.value == "(":
                return True
            return False

    # ------------------------------------------------------------------
    # AST -> Pandas Query
    # ------------------------------------------------------------------
    def ast_to_query(self, node: 'ASTNode') -> str:
        if node.node_type == self.ASTNodeType.COMPARISON:
            op_map = {
                "=": "==",
                "!=": "!=",
                "<": "<",
                ">": ">",
                "<=": "<=",
                ">=": ">="
            }
            left_str = self.ast_to_query(node.left)
            right_str = self.ast_to_query(node.right)
            op = op_map.get(node.operator_)
            if not op:
                raise ValueError(f"[x] Unknown operator: {node.operator_}")
            return f"({left_str} {op} {right_str})"

        elif node.node_type == self.ASTNodeType.LOGICAL_OP:
            left_str = self.ast_to_query(node.left)
            right_str = self.ast_to_query(node.right)
            return f"({left_str} {node.operator_} {right_str})"

        elif node.node_type == self.ASTNodeType.IN_CLAUSE:
            vals = ", ".join(node.values)
            return f"({node.identifier} in [{vals}])"

        elif node.node_type == self.ASTNodeType.IDENTIFIER:
            return node.literal_or_ident

        elif node.node_type == self.ASTNodeType.LITERAL:
            return node.literal_or_ident

        else:
            raise ValueError("[x] Unknown node type in ast_to_query.")


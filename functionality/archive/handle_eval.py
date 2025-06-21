#!/usr/bin/env python3
import ply.yacc as yacc

# Get the token map from the lexer. This is required.
from lexer_eval import lexer, tokens

# Define a rule for expressions
def p_expression(p):
    '''
    expression : expression OP expression
               | LPAREN expression RPAREN
               | NUMBER
               | STRING
               | FUNCTION LPAREN arg_list RPAREN
    '''
    if len(p) == 2:  # Single number or string
        p[0] = p[1]
    elif len(p) == 4 and p[1] == '(':
        p[0] = p[2]  # Parenthesized expression
    elif p[2] == 'OP':
        p[0] = (p[2], p[1], p[3])  # Binary operation
    else:
        p[0] = (p[1], p[3])  # Function call

# Define a rule for argument lists
def p_arg_list(p):
    '''
    arg_list : arg_list COMMA expression
             | expression
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[0] = p[1] + [p[3]]

# Error rule for syntax errors
def p_error(p):
    logger.error(f"[x] Syntax error in input at token '{p.value}'!")

# Build the parser
parser = yacc.yacc()

# Test the parser
data = '''
eval newField=round(field0+field1-field2+4,2);
'''
result = parser.parse(data)
print(result)

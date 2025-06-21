#!/usr/bin/env python3
import ply.yacc as yacc
from lexer_eval import tokens, lexer  # Ensure this import matches your lexer file
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# Precedence rules for the arithmetic operators
precedence = (
    ('left', 'OP'),
    ('right', 'UMINUS'),  # Unary minus operator
)

# Dictionary to hold the names and values of fields
fields = {}

# Grammar rules and actions
def p_eval_statement(p):
    '''eval_statement : PIPE EVAL ID EQUALS expression SEMICOLON'''
    fields[p[3]] = p[5]
    logger.info(f"[i] Assigned {p[5]} to {p[3]}")

def p_expression(p):
    '''expression : expression OP expression
                  | LPAREN expression RPAREN
                  | FUNCTION LPAREN arg_list RPAREN
                  | NUMBER
                  | STRING
                  | ID'''
    if len(p) == 4:
        if p[2] in ['+', '-', '*', '/']:  # Basic arithmetic operations
            p[0] = "Arithmetic Operation"
        elif p[1] == '(' and p[3] == ')':  # Parenthesized expression
            p[0] = p[2]
    elif len(p) == 5:  # Function call
        p[0] = "Function Call"
    else:
        p[0] = p[1]  # Number, String, or ID

def p_arg_list(p):
    '''arg_list : arg_list COMMA expression
                | expression'''
    if len(p) == 4:
        p[0] = p[1] + [p[3]]
    else:
        p[0] = [p[1]]

def p_error(p):
    if p:
        logger.error(f"[x] Syntax error at '{p.value}' (line {p.lineno})")
    else:
        logger.error("[x] Syntax error at EOF")

# Build the parser
parser = yacc.yacc()

# Test it out with updated data
data = '''
| eval newField=round(field0+field1-field2+4,2);
| eval newField=concat(field0," was found with ip ",field2,".");
| eval newField=replace(field0,"somedomain","someotherdomain");
'''
result = parser.parse(data, lexer=lexer)
print(result)

# Print out the fields dictionary to see the results of the eval statements
print(fields)

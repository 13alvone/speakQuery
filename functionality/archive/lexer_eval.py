#!/usr/bin/env python3
import ply.lex as lex
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

# List of token names - this is mandatory
tokens = (
    'NUMBER',
    'STRING',
    'ID',
    'COMMA',
    'LPAREN',
    'RPAREN',
    'EQUALS',
    'OP',
    'EVAL',
    'FUNCTION',
    'SEMICOLON',
    'PIPE',
)

# Regular expression rules for simple tokens
t_COMMA   = r','
t_LPAREN  = r'\('
t_RPAREN  = r'\)'
t_EQUALS  = r'='
t_OP      = r'[+\-*/]'
t_EVAL    = r'eval'
t_SEMICOLON = r';'
t_PIPE = r'\|'

# Define a rule for strings
def t_STRING(t):
    r'"[^"]*"'
    t.value = t.value.strip('"')  # Removing the quotation marks
    return t

# Define a rule for function names
def t_FUNCTION(t):
    r'(round|concat|replace|min|max|median|avg|range|coalesce|isnull|isnotnull|len|lower|upper|capitalize|ltrim|rtrim|trim|match|dcount|mvindex|mvjoin|null|random|sqrt|substr|tonumber|tostring|typeof|urlencode|urldecode|defang)\b'
    return t

# A regular expression rule with some action code
def t_NUMBER(t):
    r'\d+(\.\d*)?'
    t.value = float(t.value) if '.' in t.value else int(t.value)
    return t

# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

# A string containing ignored characters (spaces and tabs)
t_ignore  = ' \t'

# Error handling rule
def t_error(t):
    logger.error(f"[x] Illegal character '{t.value[0]}' at line {t.lexer.lineno}")
    t.lexer.skip(1)

# Define a rule for identifiers (including field names)
def t_ID(t):
    r'[A-Za-z_][A-Za-z0-9_]*'
    return t

# Build the lexer
lexer = lex.lex()

# Test it out with an example
data = '''
eval newField=round(field0+field1-field2+4,2);
eval newField=concat(field0," was found with ip ",field2,".");
eval newField=replace(field0,"somedomain","someotherdomain");
'''

# Give the lexer some input
lexer.input(data)

# Tokenize
while True:
    tok = lexer.token()
    if not tok:
        break      # No more input
    logger.info(f'[i] {tok.type}: {tok.value} (line {tok.lineno})')

grammar speakQuery;

program: speakQuery ;

speakQuery
    : (NEWLINE)? PIPE TABLE FILE EQUALS DOUBLE_QUOTED_STRING ((COMMA)? DOUBLE_QUOTED_STRING)* (WHERE expression ((COMMA)? expression)*)? NEWLINE
    (PIPE TIMERANGE LPAREN DOUBLE_QUOTED_STRING COMMA DOUBLE_QUOTED_STRING COMMA variable RPAREN NEWLINE)? validLine+
    ;

validLine
    : PIPE directive NEWLINE
    ;

directive
    : SEARCH expression ((COMMA)? expression)*
    | EVAL (DOUBLE_QUOTED_STRING | variable) EQUALS expression (COMMA (DOUBLE_QUOTED_STRING | variable) EQUALS expression)*
    | EVENTSTATS (NEWLINE)? statsFunctionCall (AS (variable | DOUBLE_QUOTED_STRING))? (NEWLINE)? ((COMMA)? statsFunctionCall (AS (variable | DOUBLE_QUOTED_STRING))? (NEWLINE)?)* ((COUNT)? BY (variable)+)?
    | STATS (NEWLINE)? statsFunctionCall (AS (variable | DOUBLE_QUOTED_STRING))? (NEWLINE)? ((COMMA)? statsFunctionCall (AS (variable | DOUBLE_QUOTED_STRING))? (NEWLINE)?)* ((COUNT)? BY (variable)+)?
    | RENAME variable AS (variable | DOUBLE_QUOTED_STRING) ((COMMA)? variable AS (variable | DOUBLE_QUOTED_STRING))*
    | FIELDS variable ((COMMA)? variable)*
    | HEAD NUMBER
    | (REVERSE | DEDUP)
    | REX FIELD EQUALS variable (MAX_MATCH = NUMBER)? DOUBLE_QUOTED_STRING
    | REGEX variable EQUALS DOUBLE_QUOTED_STRING
    | JOIN LBRACK speakQuery RBRACK
    ;

if_then

expression
    : LPAREN expression RPAREN
    | variable
    | valueNumeric
    | DOUBLE_QUOTED_STRING
    | functionCall
    | expressionString
    | expressionNumeric
    ;

functionCall    // Ex. function(args*)
    : numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    ;

expressionString    // Ex. "String"
    : LPAREN expressionString RPAREN
    | DOUBLE_QUOTED_STRING
    ;

expressionNumeric
    : LPAREN expressionNumeric RPAREN
    | compareNumericExpr
    ;

logicalExpr     // Ex: x=1 AND y=2
    : LPAREN logicalExpr RPAREN
    | logicalExpr (AND | OR) (logicalExpr | compareNumericExpr)+
    | compareNumericExpr
    ;

compareNumericExpr      // Ex: 1 > 0
    : LPAREN compareNumericExpr RPAREN
    | compareNumericExpr (EQUALS_EQUALS | NOT_EQUALS | GT | LT | GTEQ | LTEQ) (compareNumericExpr | muldivNumericExpr)
    | muldivNumericExpr
    ;

muldivNumericExpr       // Ex. 2 * 3
    : LPAREN muldivNumericExpr RPAREN
    | muldivNumericExpr (MUL | DIV) (muldivNumericExpr | arithmeticNumericExpr) // Chaining
    | arithmeticNumericExpr
    ;

arithmeticNumericExpr   // Ex. 1 + 3
    : LPAREN arithmeticNumericExpr RPAREN
    | arithmeticNumericExpr ((PLUS | MINUS) (arithmeticNumericExpr | valueNumeric))+ // Chaining
    | valueNumeric
    ;

valueNumeric    // Ex. 3++, 3, TRUE, FALSE, 1, 0
    : unaryExpr
    | NUMBER
    | BOOLEAN
    | BOOLNUM
    ;

unaryExpr   // Ex: 1++
    : NUMBER (INC | DEC | BITWISE_NOT)
    | (INC | DEC | BITWISE_NOT) NUMBER
    ;

numericFunctionCall     // Numeric function calls
    : ROUND LPAREN (variable | expressionNumeric) COMMA (variable | expressionNumeric) RPAREN
    | MIN LPAREN (variable | expressionNumeric) RPAREN
    | MAX LPAREN (variable | expressionNumeric) RPAREN
    | AVG LPAREN (variable | expressionNumeric) RPAREN
    | SUM LPAREN (variable | expressionNumeric) RPAREN
    | RANGE LPAREN (variable | expressionNumeric) RPAREN
    | MEDIAN LPAREN (variable | expressionNumeric) RPAREN
    | MODE LPAREN (variable | expressionNumeric) RPAREN
    | DCOUNT LPAREN (variable | expressionNumeric) RPAREN
    | SQRT LPAREN (variable | expressionNumeric) RPAREN
    | RANDOM LPAREN ((variable | expressionNumeric) (COMMA (variable | expressionNumeric))?)? RPAREN
    ;

stringFunctionCall      // String function calls
    : CONCAT LPAREN (variable | expressionString) (COMMA expressionString)* RPAREN
    | REPLACE LPAREN (variable | expressionString) COMMA (variable | expressionString) COMMA (variable | expressionString) RPAREN
    | UPPER LPAREN (variable | expressionString) RPAREN
    | LOWER LPAREN (variable | expressionString) RPAREN
    | CAPITALIZE LPAREN (variable | expressionString) RPAREN
    | LEN LPAREN (variable | expressionString) RPAREN
    | URLENCODE LPAREN (variable | expressionString) RPAREN
    | URLDECODE LPAREN (variable | expressionString) RPAREN
    | DEFANG LPAREN (variable | expressionString) RPAREN
    | TRIM LPAREN (variable | expressionString) (COMMA (variable | expressionString))? RPAREN
    | RTRIM LPAREN (variable | expressionString) (COMMA (variable | expressionString))? RPAREN
    | LTRIM LPAREN (variable | expressionString) (COMMA (variable | expressionString))? RPAREN
    | SUBSTR LPAREN (variable | expressionString) COMMA (variable | expressionString) (COMMA (variable | expressionString))? RPAREN
    | MATCH LPAREN (variable | expressionString) COMMA (variable | expressionString) RPAREN
    ;

specificFunctionCall    // Other specific function calls
    : NULL LPAREN RPAREN
    | ISNULL LPAREN expression RPAREN
    | ISNOTNULL LPAREN expression RPAREN
    | TIME LPAREN expression RPAREN
    | FIELDSUMMARY LPAREN RPAREN
    | COALESCE LPAREN variable (COMMA variable)+ RPAREN
    | MVJOIN LPAREN expression (COMMA expression)+ RPAREN
    | MVINDEX LPAREN expression COMMA expression RPAREN
    | MVEXPAND LPAREN expression RPAREN
    | MACRO LPAREN variable RPAREN
    ;

statsFunctionCall  // For Stats function calls only
    : VALUES LPAREN variable RPAREN (AS (variable | expressionString))?
    | LATEST LPAREN variable RPAREN (AS (variable | expressionString))?
    | EARLIEST LPAREN variable RPAREN (AS (variable | expressionString))?
    | FIRST LPAREN variable RPAREN (AS (variable | expressionString))?
    | LAST LPAREN variable RPAREN (AS (variable | expressionString))?
    | DCOUNT LPAREN variable RPAREN (AS (variable | expressionString))?
    ;

variable
    : ID
    ;

PIPE                    : '|' ;
COMMENT                 : '#' ;
TABLE                   : 'table' ;
TICK                    : '`' ;
FIELDSUMMARY            : 'fieldsummary' ;
MAX_MATCH               : 'max_match' ;
MVEXPAND                : 'mvexpand' ;
TIME                    : 'time' ;
TIMERANGE               : 'timerange' ;
LATEST                  : 'latest' ;
EARLIEST                : 'earliest' ;
FIRST                   : 'first' ;
LAST                    : 'last' ;
MACRO                   : 'macro' ;
FILE                    : 'file' ;
NULL                    : 'null' ;
EVAL                    : 'eval' ;
EVENTSTATS              : 'eventstats' ;
STATS                   : 'stats' ;
VALUES                  : 'values' ;
WHERE                   : 'WHERE' ;
RENAME                  : 'rename' ;
REVERSE                 : 'reverse' ;
FIELD                   : 'field' ;
FIELDS                  : 'fields' ;
SEARCH                  : 'search' ;
HEAD                    : 'head' ;
REX                     : 'rex' ;
REGEX                   : 'regex' ;
DEDUP                   : 'dedup' ;
DEFANG                  : 'defang';
ROUND                   : 'round' ;
MIN                     : 'min' ;
MAX                     : 'max' ;
MEDIAN                  : 'median' ;
MODE                    : 'mode' ;
AVG                     : 'avg' ;
SUM                     : 'sum' ;
RANDOM                  : 'random' ;
SQRT                    : 'sqrt' ;
RANGE                   : 'range' ;
COUNT                   : 'count' ;
DCOUNT                  : 'dcount' ;
TONUMB                  : 'tonumb' ;
COALESCE                : 'coalesce' ;
ISNULL                  : 'isnull' ;
ISNOTNULL               : 'isnotnull' ;
LEN                     : 'len' ;
CONCAT                  : 'concat' ;
REPLACE                 : 'replace' ;
LOWER                   : 'lower' ;
UPPER                   : 'upper' ;
CAPITALIZE              : 'capitalize' ;
LTRIM                   : 'ltrim' ;
RTRIM                   : 'rtrim' ;
TRIM                    : 'trim' ;
MATCH                   : 'match' ;
MVINDEX                 : 'mvindex' ;
JOIN                    : 'join' ;
MVJOIN                  : 'mvjoin' ;
SUBSTR                  : 'substr' ;
TYPE                    : 'type' ;
URLENCODE               : 'urlencode' ;
URLDECODE               : 'urldecode' ;
NUMBER                  : [0-9]+ ('.' [0-9]+)? ;
DOUBLE_QUOTED_STRING    : '"' (~["\r\n])* '"' ;
BOOLEAN                 : 'TRUE' | 'FALSE' ;
BOOLNUM                 : '0' | '1' ;
LBRACK                  : '[' ;
RBRACK                  : ']' ;
LPAREN                  : '(' ;
RPAREN                  : ')' ;
COMMA                   : ',' ;
SEMICOLON               : ';' ;
EQUALS                  : '=' ;
EQUALS_EQUALS           : '==' ;
NOT_EQUALS              : '!=' ;
GT                      : '>' ;
LT                      : '<' ;
GTEQ                    : '>=' ;
LTEQ                    : '<=' ;
PLUS                    : '+' ;
MINUS                   : '-' ;
MUL                     : '*' ;
DIV                     : '/' ;
INC                     : '++' ;
DEC                     : '--' ;
BITWISE_NOT             : '~' ;
AND                     : 'AND' ;
NOT                     : 'NOT' ;
OR                      : 'OR' ;
BY                      : 'by' ;
AS                      : 'as' ;
ID                      : [a-zA-Z_] [a-zA-Z_0-9]* (NUMBER)?;
NEWLINE                 : '\r'? '\n' | EOF;
WS                      : [ \t]+ -> skip ; // skip spaces and tabs

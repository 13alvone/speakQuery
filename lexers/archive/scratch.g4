grammar speakQuery;

speakQuery
    : optionalWhitespace expressionSequence optionalWhitespace validLine* (EOF | NEWLINE?)
    ;

expressionSequence
    : expression (optionalWhitespace expression)*
    ;

expression
    : conjunction (optionalWhitespace OR optionalWhitespace conjunction)*
    ;

conjunction
    : comparison ( (optionalWhitespace AND optionalWhitespace | optionalWhitespace) comparison )*
    ;

comparison
    : additiveExpr (optionalWhitespace comparisonOperator optionalWhitespace additiveExpr)*
    ;

additiveExpr
    : multiplicativeExpr ( (PLUS | MINUS) multiplicativeExpr)*
    ;

multiplicativeExpr
    : unaryExpr ( (MUL | DIV) unaryExpr)*
    ;

unaryExpr
    : (NOT | PLUS | MINUS)? optionalWhitespace primary
    ;

primary
    : LPAREN optionalWhitespace expression optionalWhitespace RPAREN
    | timeClause
    | indexClause
    | inExpression
    | functionCall
    | variableName
    | DOUBLE_QUOTED_STRING
    | NUMBER
    | BOOLEAN
    ;

timeClause
    : earliestClause latestClause?
    | latestClause earliestClause?
    ;

earliestClause
    : EARLIEST EQUALS (DOUBLE_QUOTED_STRING | NUMBER) optionalWhitespace
    ;

latestClause
    : LATEST EQUALS (DOUBLE_QUOTED_STRING | NUMBER) optionalWhitespace
    ;

indexClause
    : INDEX EQUALS (DOUBLE_QUOTED_STRING | NUMBER | VARIABLE) optionalWhitespace
    ;

comparisonOperator
    : EQUALS | NOT_EQUALS | GT | LT | GTEQ | LTEQ
    ;

inExpression
    : (NOT? optionalWhitespace | optionalWhitespace NOT?) variableName IN LPAREN (expression (COMMA expression)*)? RPAREN
    ;

inputlookupInit
    : optionalWhitespace PIPE INPUTLOOKUP (variableName | DOUBLE_QUOTED_STRING) (variableName | DOUBLE_QUOTED_STRING)? optionalWhitespace
    ;

validLine
    : optionalWhitespace PIPE directive optionalWhitespace
    ;

directive
    : SEARCH (NOT? optionalWhitespace | optionalWhitespace NOT?) (expression COMMA?)+
    | WHERE (NOT? optionalWhitespace | optionalWhitespace NOT?) (expression COMMA?)+
    | EVAL (DOUBLE_QUOTED_STRING | variableName) EQUALS expression (COMMA optionalWhitespace (DOUBLE_QUOTED_STRING | variableName) EQUALS expression)*
    | TABLE variableName (COMMA? variableName)*
    | EVENTSTATS optionalWhitespace (statsAgg (COMMA? NEWLINE? statsAgg)*)? (optionalWhitespace COUNT (AS variableName)?)? (BY variableList)?
    | STATS optionalWhitespace statsAgg (COMMA? optionalWhitespace statsAgg)* (optionalWhitespace BY variableList)?
    | RENAME (optionalWhitespace variableName AS (variableName | DOUBLE_QUOTED_STRING) COMMA? optionalWhitespace)+
    | FIELDS (PLUS | MINUS)? variableName (COMMA? variableName)*
    | MVEXPAND variableName
    | LOOKUP (VARIABLE | DOUBLE_QUOTED_STRING)
    | (HEAD | LIMIT) NUMBER
    | BIN (variableName SPAN EQUALS timespan | SPAN EQUALS timespan variableName)
    | REVERSE
    | DEDUP NUMBER? (CONSECUTIVE EQUALS BOOLEAN)? variableName (COMMA? variableName)*
    | SORT (PLUS | MINUS) (variableName COMMA?)+
    | REX FIELD EQUALS variableName (MODE EQUALS SED)? (MAX_MATCH EQUALS NUMBER)? DOUBLE_QUOTED_STRING
    | REGEX variableName (EQUALS | NOT_EQUALS) (variableName | DOUBLE_QUOTED_STRING)
    | BASE64 (ENCODE | DECODE) (variableName | DOUBLE_QUOTED_STRING) (COMMA? (variableName | DOUBLE_QUOTED_STRING))*
    | BACKTICK macro BACKTICK
    | FILLNULL VALUE EQUALS (DOUBLE_QUOTED_STRING | NUMBER | variableName) (variableName COMMA?)*
    | SPATH variableName OUTPUT EQUALS variableName
    | JOIN (TYPE EQUALS (LEFT | CENTER | RIGHT))? variableName (COMMA variableName)* subsearch
    | APPEND subsearch
    | COALESCE LPAREN variableName (COMMA variableName)+ RPAREN
    | MVJOIN LPAREN expression COMMA mvDelim RPAREN
    | MVINDEX LPAREN expression COMMA mvindexIndex RPAREN
    | MVREVERSE LPAREN expression RPAREN
    | MVFIND LPAREN expression COMMA mvfindObject RPAREN
    | MVDEDUP LPAREN expression RPAREN
    | MVAPPEND LPAREN expression (COMMA (variableName | expression))+ RPAREN
    | MVFILTER LPAREN expression RPAREN
    | MVCOMBINE LPAREN expression COMMA mvDelim RPAREN
    | MVCOUNT LPAREN expression RPAREN
    | MVDC LPAREN expression RPAREN
    | MVZIP LPAREN expression (COMMA variableName)+ COMMA mvDelim RPAREN
    ;

macro
    : VARIABLE LPAREN (COMMA? (DOUBLE_QUOTED_STRING | VARIABLE))* RPAREN
    ;

statsAgg
    : statsFunctionCall (AS variableName)?
    ;

variableList
    : variableName (COMMA variableName)*
    ;

subsearch
    : LBRACK optionalWhitespace expression optionalWhitespace RBRACK
    ;

functionCall
    : numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    ;

numericFunctionCall
    : ROUND LPAREN expression (COMMA expression)? RPAREN
    | MIN LPAREN expression RPAREN
    | MAX LPAREN expression RPAREN
    | AVG LPAREN expression RPAREN
    | SUM LPAREN expression RPAREN
    | RANGE LPAREN expression RPAREN
    | MEDIAN LPAREN expression RPAREN
    | MODE LPAREN expression RPAREN
    | SQRT LPAREN expression RPAREN
    | ABS LPAREN expression RPAREN
    | RANDOM LPAREN (expression (COMMA expression)*)? RPAREN
    ;

stringFunctionCall
    : CONCAT LPAREN expression (COMMA expression)* RPAREN
    | REPLACE LPAREN expression COMMA expression COMMA expression RPAREN
    | REPEAT LPAREN expression COMMA NUMBER RPAREN
    | UPPER LPAREN expression RPAREN
    | LOWER LPAREN expression RPAREN
    | CAPITALIZE LPAREN expression RPAREN
    | LEN LPAREN expression RPAREN
    | TOSTRING LPAREN expression RPAREN
    | URLENCODE LPAREN expression RPAREN
    | URLDECODE LPAREN expression RPAREN
    | DEFANG LPAREN expression RPAREN
    | FANG LPAREN expression RPAREN
    | TRIM LPAREN expression (COMMA expression)? RPAREN
    | RTRIM LPAREN expression (COMMA expression)? RPAREN
    | LTRIM LPAREN expression (COMMA expression)? RPAREN
    | SUBSTR LPAREN expression COMMA expression COMMA expression RPAREN
    | (NOT optionalWhitespace)? MATCH LPAREN (variableName | DOUBLE_QUOTED_STRING) COMMA regexTarget RPAREN
    ;

specificFunctionCall
    : NULL LPAREN RPAREN
    | (NOT optionalWhitespace)? ISNULL LPAREN variableName RPAREN
    | TO_CRON LPAREN inputCron RPAREN
    | FROM_CRON LPAREN inputCron COMMA cronformat RPAREN
    | COALESCE LPAREN variableName (COMMA variableName)+ RPAREN
    ;

statsFunctionCall
    : COUNT
    | COUNT LPAREN expression RPAREN
    | VALUES LPAREN expression RPAREN
    | LATEST LPAREN expression RPAREN
    | EARLIEST LPAREN expression RPAREN
    | FIRST LPAREN expression RPAREN
    | LAST LPAREN expression RPAREN
    | DC LPAREN expression RPAREN
    | numericFunctionCall
    ;

regexTarget
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

mvfindObject
    : variableName
    | DOUBLE_QUOTED_STRING
    | unaryExpr
    ;

mvindexIndex
    : unaryExpr
    ;

mvDelim
    : DOUBLE_QUOTED_STRING
    ;

inputCron
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

cronformat
    : DOUBLE_QUOTED_STRING
    ;

timespan
    : NUMBER (SECONDS | MINUTES | HOURS | DAYS | WEEKS | YEARS)
    ;

variableName
    : VARIABLE
    | SINGLE_QUOTED_STRING
    | TYPE
    | VALUE
    | EARLIEST
    | LATEST
    | INDEX
    ;

optionalWhitespace
    : (NEWLINE | WS)*
    ;

COMMENT                 : '#' ~[\r\n]* NEWLINE -> skip ;
WS                      : [ \t]+ -> channel(HIDDEN) ;
NEWLINE                 : '\r'? '\n' ;

PIPE                    : '|' ;

EARLIEST                : 'earliest' ;
LATEST                  : 'latest' ;
INDEX                   : 'index' ;
NOT                     : ('NOT' | 'not') ;
AND                     : ('AND' | 'and') ;
OR                      : ('OR' | 'or') ;
BY                      : ('BY' | 'by') ;
AS                      : ('AS' | 'as') ;
IN                      : ('IN' | 'in') ;
IF                      : 'if' ;
ELSE                    : 'else' ;
CASE                    : 'case' ;
EQUALS                  : '=' ;
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
LPAREN                  : '(' ;
RPAREN                  : ')' ;
LBRACK                  : '[' ;
RBRACK                  : ']' ;
COMMA                   : ',' ;
SEMICOLON               : ';' ;
TABLE                   : 'table' ;
CONSECUTIVE             : 'consecutive' ;
REPEAT                  : 'repeat' ;
MAX_MATCH               : ('max_match' | 'MAX_MATCH') ;
MVEXPAND                : 'mvexpand' ;
REVERSE                 : 'reverse' ;
MVREVERSE               : 'mvreverse' ;
MVCOMBINE               : 'mvcombine' ;
MVFIND                  : 'mvfind' ;
DEDUP                   : 'dedup' ;
MVDEDUP                 : 'mvdedup' ;
MVFILTER                : 'mvfilter' ;
COUNT                   : 'count' ;
MVCOUNT                 : 'mvcount' ;
DC                      : 'dc' ;
MVDC                    : 'mvdc' ;
MVZIP                   : 'mvzip' ;
FIRST                   : 'first' ;
LAST                    : 'last' ;
DELIM                   : ('delim' | 'DELIM') ;
FILE                    : ('file' | 'FILE') ;
SORT                    : 'sort' ;
NULL                    : 'null' ;
EVAL                    : 'eval' ;
SPAN                    : ('span' | 'SPAN') ;
BIN                     : 'bin' ;
STATS                   : 'stats' ;
EVENTSTATS              : 'eventstats' ;
VALUE                   : ('value' | 'VALUE') ;
VALUES                  : 'values' ;
WHERE                   : ('WHERE' | 'where') ;
RENAME                  : 'rename' ;
FIELD                   : ('field' | 'FIELD') ;
FIELDS                  : 'fields' ;
FIELDSUMMARY            : 'fieldsummary' ;
APPEND                  : 'append' ;
APPENDPIPE              : 'appendpipe' ;
MVAPPEND                : 'mvappend' ;
SEARCH                  : 'search' ;
MULTISEARCH             : 'multisearch' ;
HEAD                    : 'head' ;
LIMIT                   : ('limit' | 'LIMIT') ;
REX                     : 'rex' ;
REGEX                   : 'regex' ;
LOADJOB                 : 'loadjob' ;
OUTPUT                  : ('output' | 'OUTPUT') ;
LOOKUP                  : 'lookup' ;
INPUTLOOKUP             : 'inputlookup' ;
OUTPUTLOOKUP            : 'outputlookup' ;
OUTPUTNEW               : ('outputnew' | 'OUTPUTNEW') ;
WINDOW                  : ('window' | 'WINDOW') ;
OVERWRITE               : ('overwrite' | 'OVERWRITE') ;
OVERWRITE_IF_EMPTY      : ('overwrite_if_empty' | 'OVERWRITE_IF_EMPTY') ;
CREATE_EMPTY            : ('create_empty' | 'CREATE_EMPTY') ;
FILLNULL                : 'fillnull' ;
FANG                    : 'fang' ;
DEFANG                  : 'defang' ;
TO_CRON                 : 'to_cron' ;
FROM_CRON               : 'from_cron' ;
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
COALESCE                : 'coalesce' ;
ISNULL                  : 'isnull' ;
ISNOTNULL               : 'isnotnull' ;
LEN                     : 'len' ;
SED                     : 'sed' ;
CONCAT                  : 'concat' ;
REPLACE                 : 'replace' ;
LOWER                   : 'lower' ;
UPPER                   : 'upper' ;
CAPITALIZE              : 'capitalize' ;
TRIM                    : 'trim' ;
LTRIM                   : 'ltrim' ;
RTRIM                   : 'rtrim' ;
MATCH                   : 'match' ;
MVINDEX                 : 'mvindex' ;
JOIN                    : 'join' ;
MVJOIN                  : 'mvjoin' ;
SUBSTR                  : 'substr' ;
TOSTRING                : 'tostring' ;
TYPE                    : 'type' ;
LEFT                    : 'left' ;
RIGHT                   : 'right' ;
CENTER                  : 'center' ;
ABS                     : 'abs' ;
URLENCODE               : 'urlencode' ;
URLDECODE               : 'urldecode' ;
DECODE                  : 'decode' ;
ENCODE                  : 'encode' ;
BASE64                  : 'base64' ;
SPATH                   : 'spath' ;
BOOLEAN                 : 'TRUE' | 'True' | 'true' | 'FALSE' | 'False' | 'false' ;
SECONDS                 : ('second' | 'seconds') ;
MINUTES                 : ('minute' | 'minutes') ;
HOURS                   : ('hour' | 'hours') ;
DAYS                    : ('day' | 'days') ;
WEEKS                   : ('week' | 'weeks') ;
YEARS                   : ('year' | 'years') ;
BACKTICK                : '`' ;
NUMBER                  : '-'? [0-9]+ ('.' [0-9]+)? ;
SINGLE_QUOTED_STRING    : '\'' (~['\r\n])* '\'' ;
DOUBLE_QUOTED_STRING    : '"' ( '\\' . | ~('"' | '\\' | '\r' | '\n') )* '"' ;
PERIOD                  : '.' ;
VARIABLE                : [a-zA-Z_] [a-zA-Z_0-9.]* ;

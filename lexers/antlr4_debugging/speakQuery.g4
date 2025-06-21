grammar speakQuery;

program: speakQuery ;

speakQuery
    : (NEWLINE+)? PIPE TABLE FILE EQUALS DOUBLE_QUOTED_STRING ((COMMA)? DOUBLE_QUOTED_STRING)* (WHERE expression ((COMMA)? expression)*)? (NEWLINE | EOF)
    (PIPE TIMERANGE LPAREN DOUBLE_QUOTED_STRING COMMA DOUBLE_QUOTED_STRING COMMA VARIABLE RPAREN NEWLINE)? (validLine+)? EOF
    ;

validLine
    : PIPE directive NEWLINE
    ;

directive
    : (SEARCH | WHERE) (NOT)? comparisonExpr ((COMMA)? comparisonExpr)*
    | EVAL (DOUBLE_QUOTED_STRING | VARIABLE) EQUALS expression (COMMA (DOUBLE_QUOTED_STRING | VARIABLE) EQUALS expression)*
    | STREAMSTATS (CURRENT EQUALS BOOLEAN)? (WINDOW EQUALS NUMBER)? (NEWLINE)? (statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)?)+ ((COUNT (AS (VARIABLE | DOUBLE_QUOTED_STRING))?)? BY (VARIABLE)+ (RESET_AFTER VARIABLE EQUALS (VARIABLE | unaryExpr | NUMBER | DOUBLE_QUOTED_STRING))?)?
    | EVENTSTATS (NEWLINE)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)? ((COMMA)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)?)* ((COUNT (AS (VARIABLE | DOUBLE_QUOTED_STRING))?)? BY (VARIABLE)+)?
    | STATS (NEWLINE)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)? ((COMMA)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)?)* ((COUNT)? BY (VARIABLE)+)?
    | TIMECHART (NEWLINE)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)? ((COMMA)? statsFunctionCall (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (NEWLINE)?)* ((COUNT)? BY (VARIABLE)+)?
    | RENAME VARIABLE AS (VARIABLE | DOUBLE_QUOTED_STRING) ((COMMA)? VARIABLE AS (VARIABLE | DOUBLE_QUOTED_STRING))*
    | FIELDS (PLUS | MINUS)? VARIABLE ((COMMA)? VARIABLE)*
    | MAKETABLE VARIABLE ((COMMA)? VARIABLE)*
    | LOOKUP FILE VARIABLE (AS (VARIABLE | DOUBLE_QUOTED_STRING))? (OUTPUTNEW (VARIABLE | DOUBLE_QUOTED_STRING))
    | (HEAD | LIMIT) NUMBER
    | BIN VARIABLE SPAN EQUALS timeStringValue
    | REVERSE
    | DEDUP
    | SORT (PLUS | MINUS) VARIABLE
    | REX FIELD EQUALS VARIABLE (MODE EQUALS SED)? (MAX_MATCH = NUMBER)? DOUBLE_QUOTED_STRING
    | REGEX VARIABLE (EQUALS | NOT_EQUALS) (VARIABLE | DOUBLE_QUOTED_STRING)
    | BASE64 (ENCODE | DECODE) (VARIABLE | DOUBLE_QUOTED_STRING) ((COMMA)? (VARIABLE | DOUBLE_QUOTED_STRING))*
    | SPECIAL_FUNCTION BACKTICK specialFunctionName BACKTICK  // This takes the place of for_each, allowing you to build python code for small special functions in lou of monolithic FOREACH
    | FILLNULL VALUE EQUALS VARIABLE ((COMMA)? VARIABLE)*
    | LOADJOB DOUBLE_QUOTED_STRING
    | INPUTLOOKUP DOUBLE_QUOTED_STRING
    | OUTPUTLOOKUP (OVERWRITE EQUALS BOOLEAN)? (CREATE_EMPTY EQUALS BOOLEAN)? (OVERWRITE_IF_EMPTY EQUALS BOOLEAN)? DOUBLE_QUOTED_STRING
    | SPATH VARIABLE
    | JOIN TYPE EQUALS (LEFT | CENTER | RIGHT) VARIABLE LBRACK speakQuery RBRACK
    ;

specialFunctionName
    : VARIABLE
    ;

// *************************************************************************************
// Unified Expression Handling
// *************************************************************************************

expression
    : functionCall
    | ifExpression
    | caseExpression
    | logicalExpr
    | LPAREN expression RPAREN
    | DOUBLE_QUOTED_STRING
    | VARIABLE // Place this last to ensure all other options are considered first
    ;

//expression
//    : ifExpression
//    | caseExpression
//    | logicalExpr
//    | LPAREN expression RPAREN // Encapsulated expressions have the SECOND highest precedence
//    | functionCall
//    | DOUBLE_QUOTED_STRING
//    ;

// *************************************************************************************
// Logical, Comparison, and Arithmetic Expressions
// *************************************************************************************

logicalExpr
    : logicalExpr (AND | OR) comparisonExpr
    | comparisonExpr
    ;

comparisonExpr
    : comparisonExpr (EQUALS_EQUALS | NOT_EQUALS | GT | LT | GTEQ | LTEQ) additiveExpr
    | additiveExpr
    ;

additiveExpr
    : additiveExpr (PLUS | MINUS) multiplicativeExpr
    | multiplicativeExpr
    ;

multiplicativeExpr
    : multiplicativeExpr (MUL | DIV) primaryExpr
    | primaryExpr
    ;

primaryExpr
    : functionCall // Make sure function calls are matched before variables
    | NUMBER
    | BOOLEAN
    | BOOLNUM
    | unaryExpr
    | VARIABLE
    | LPAREN expression RPAREN
    ;

// *************************************************************************************
// If, Case, IN Expressions
// *************************************************************************************

ifExpression
    : IF LPAREN expression COMMA expression COMMA expression RPAREN
    ;

caseExpression
    : CASE LPAREN (comparisonExpr COMMA expression COMMA)* expression RPAREN
    ;

inExpression
    : (NOT)? VARIABLE IN LPAREN (expression (COMMA expression)*)? RPAREN
    ;

// *************************************************************************************
// Unary Expressions and Value Handling
// *************************************************************************************
unaryExpr  // Ex: 1++
    : NUMBER (INC | DEC | BITWISE_NOT)
    | (INC | DEC | BITWISE_NOT) NUMBER
    ;

// *************************************************************************************
// Functions
// *************************************************************************************

functionCall
    : numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    ;

//functionName
//    : TOSTRING
//    | ROUND | MIN | MAX | AVG | SUM | RANGE | MEDIAN | MODE | SQRT | ABS | RANDOM
//    // ... other function names
//    ;
//
//functionCall
//    : functionName LPAREN (argList)? RPAREN
//    ;

numericFunctionCall  // Numeric function calls
    : ROUND LPAREN multivalueNumericField COMMA multivalueNumericField RPAREN
    | MIN LPAREN multivalueNumericField RPAREN
    | MAX LPAREN multivalueNumericField RPAREN
    | AVG LPAREN multivalueNumericField RPAREN
    | SUM LPAREN multivalueNumericField RPAREN
    | RANGE LPAREN multivalueNumericField RPAREN
    | MEDIAN LPAREN multivalueNumericField RPAREN
    | MODE LPAREN multivalueNumericField RPAREN
    | SQRT LPAREN multivalueNumericField RPAREN
    | ABS LPAREN multivalueNumericField RPAREN
    | RANDOM LPAREN (multivalueNumericField COMMA multivalueNumericField COMMA multivalueNumericField)? RPAREN
    ;

stringFunctionCall  // String function calls
    : CONCAT LPAREN stringFunctionTarget (COMMA stringFunctionTarget)* RPAREN  // Concatente infinate orderings of strings
    | REPLACE LPAREN stringFunctionTarget COMMA stringFunctionTarget COMMA stringFunctionTarget RPAREN
    | UPPER LPAREN stringFunctionTarget RPAREN
    | LOWER LPAREN stringFunctionTarget RPAREN
    | CAPITALIZE LPAREN stringFunctionTarget RPAREN
    | LEN LPAREN stringFunctionTarget RPAREN
    | TOSTRING LPAREN expression RPAREN
    | URLENCODE LPAREN httpStringField RPAREN
    | URLDECODE LPAREN httpStringField RPAREN
    | DEFANG LPAREN httpStringField RPAREN
    | TRIM LPAREN trimTarget (COMMA trimRemovalTarget)? RPAREN
    | RTRIM LPAREN trimTarget (COMMA trimRemovalTarget)? RPAREN
    | LTRIM LPAREN trimTarget (COMMA trimRemovalTarget)? RPAREN
    | SUBSTR LPAREN substrTarget COMMA substrStart COMMA substrLength RPAREN
    | (NOT)? MATCH LPAREN (VARIABLE | DOUBLE_QUOTED_STRING) COMMA regexTarget RPAREN
    ;

specificFunctionCall  // Other specific function calls
    : NULL LPAREN RPAREN  // Effectively, this is speakQuery's way of saying "Nothing", "None", or "Null", etc.
    | (NOT)? ISNULL LPAREN VARIABLE RPAREN  // Check each of a fields value, returning True if a value is null for each
    | TO_CRON LPAREN  COMMA inputCron // Convert a count of seconds to its equivalent cron string
    | FROM_CRON LPAREN inputCron COMMA cronformat RPAREN  // Convert from a cron string to a date range in seconds
    | FIELDSUMMARY LPAREN RPAREN  // Provides a total field summary at a given point in a speakQuery
    | COALESCE LPAREN VARIABLE (COMMA VARIABLE)+ RPAREN  // Pick the first non-null value found between the ordered, provided set
    | MVJOIN LPAREN multivalueField COMMA mvDelim RPAREN  // Join two fields by a delimiter
    | MVINDEX LPAREN multivalueField COMMA mvindexIndex RPAREN  // Return a given index number from a multivalue field
    | MVREVERSE LPAREN multivalueField RPAREN  // Revese a multivalue field's order
    | MVFIND LPAREN multivalueField COMMA mvfindObject RPAREN  // Find a string in a string
    | MVDEDUP LPAREN multivalueField RPAREN // Dedup a multivalue field
    | MVAPPEND LPAREN multivalueField (COMMA (VARIABLE | comparisonExpr))+ RPAREN  // Add new column(s)
    | MVFILTER LPAREN multivalueField RPAREN  // Remove items from a multivalue field
    | MVEXPAND LPAREN multivalueField RPAREN  // Expand a multivalue field
    | MVCOMBINE LPAREN multivalueField COMMA mvDelim RPAREN   // Combine a multivalue field by a delimiter
    | MVCOUNT LPAREN multivalueField RPAREN  // Count the entries in a multivalue field
    | MVDCOUNT LPAREN multivalueField RPAREN  // Distinct count of entries in a multivalue field
    | MVZIP LPAREN multivalueField (COMMA VARIABLE)+ COMMA mvDelim RPAREN  // Join two different fields by a delimiter
    | MACRO LPAREN executionMaro RPAREN  // Reusable subpiece of any valid speakQuery code
    ;

statsFunctionCall  // For Stats function calls only
    : VALUES LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | LATEST LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | EARLIEST LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | FIRST LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | LAST LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | DCOUNT LPAREN multivalueField RPAREN (AS (VARIABLE | DOUBLE_QUOTED_STRING))?
    | numericFunctionCall
    ;

// *************************************************************************************
// Function Parameters
// *************************************************************************************

stringFunctionTarget
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    | staticMultivalueStringField
    ;

httpStringField
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    | staticMultivalueStringField
    ;

multivalueField
    : multivalueStringField
    | multivalueNumericField
    ;

multivalueStringField
    : VARIABLE
    | staticMultivalueStringField
    ;

multivalueNumericField
    : VARIABLE
    | staticMultivalueNumericField
    ;

staticMultivalueStringField
    : LBRACK DOUBLE_QUOTED_STRING (COMMA DOUBLE_QUOTED_STRING)+ RBRACK
    | LPAREN DOUBLE_QUOTED_STRING (COMMA DOUBLE_QUOTED_STRING)+ LPAREN
    ;

staticMultivalueNumericField
    : LBRACK expression (COMMA expression)+ RBRACK
    | LPAREN expression (COMMA expression)+ RBRACK
    ;

regexTarget
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

trimTarget
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

substrTarget
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

substrStart
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

substrLength
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

trimRemovalTarget
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

mvfindObject
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    | NUMBER
    ;

mvindexIndex
    : NUMBER
    ;

mvDelim
    : DOUBLE_QUOTED_STRING
    ;

inputCron
    : VARIABLE
    | timeStringValue
    ;

cronformat
    : timeStringValue
    ;

executionMaro
    : VARIABLE
    ;

timeStringValue
    : DOUBLE_QUOTED_STRING
    ;

// *************************************************************************************
// LEXER Primatives
// *************************************************************************************

PIPE                    : '|' ;
CURRENT                 : 'current' ;
TABLE                   : 'table' ;
MAKETABLE               : 'maketable' ;
FIELDSUMMARY            : 'fieldsummary' ;
SPECIAL_FUNCTION        : 'special_function' ;
TIMECHART               : 'timechart' ;
MAX_MATCH               : 'max_match' ;
MVEXPAND                : 'mvexpand' ;
MVREVERSE               : 'mvreverse' ;
MVCOMBINE               : 'mvcombine' ;
MVFIND                  : 'mvfind' ;
MVDEDUP                 : 'mvdedup' ;
MVAPPEND                : 'mvappend' ;
MVFILTER                : 'mvfilter' ;
MVCOUNT                 : 'mvcount' ;
MVDCOUNT                : 'mvdcount' ;
MVZIP                   : 'mvzip' ;
TIMERANGE               : 'timerange' ;
LATEST                  : 'latest' ;
EARLIEST                : 'earliest' ;
FIRST                   : 'first' ;
LAST                    : 'last' ;
MACRO                   : 'macro' ;
DELIM                   : 'delim' ;
FILE                    : 'file' ;
SORT                    : 'sort' ;
NULL                    : 'null' ;
EVAL                    : 'eval' ;
SPAN                    : 'span' ;
BIN                     : 'bin' ;
STREAMSTATS             : 'streamstats' ;
RESET_AFTER             : 'reset_after' ;
EVENTSTATS              : 'eventstats' ;
STATS                   : 'stats' ;
VALUES                  : 'values' ;
VALUE                   : 'value' ;
WHERE                   : ('WHERE' | 'where') ;
RENAME                  : 'rename' ;
REVERSE                 : 'reverse' ;
FIELD                   : 'field' ;
FIELDS                  : 'fields' ;
SEARCH                  : 'search' ;
HEAD                    : 'head' ;
LIMIT                   : 'limit' ;
REX                     : 'rex' ;
REGEX                   : 'regex' ;
DEDUP                   : 'dedup' ;
LOADJOB                 : 'loadjob' ;
INPUTLOOKUP             : 'inputlookup' ;
OUTPUTLOOKUP            : 'outputlookup' ;
LOOKUP                  : 'lookup' ;
WINDOW                  : 'window' ;
OUTPUTNEW               : 'outputnew' ;
OVERWRITE_IF_EMPTY      : 'overwrite_if_empty' ;
OVERWRITE               : 'overwrite' ;
CREATE_EMPTY            : 'create_empty' ;
FILLNULL                : 'fillnull' ;
DEFANG                  : 'defang';
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
COUNT                   : 'count' ;
DCOUNT                  : 'dcount' ;
TONUMB                  : 'tonumb' ;
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
LTRIM                   : 'ltrim' ;
RTRIM                   : 'rtrim' ;
TRIM                    : 'trim' ;
MATCH                   : 'match' ;
MVINDEX                 : 'mvindex' ;
JOIN                    : 'join' ;
MVJOIN                  : 'mvjoin' ;
SUBSTR                  : 'substr' ;
TOSTRING                : 'tostring' ;
TYPE                    : 'type' ;
ABS                     : 'abs' ;
URLENCODE               : 'urlencode' ;
URLDECODE               : 'urldecode' ;
DECODE                  : 'decode' ;
ENCODE                  : 'encode' ;
BASE64                  : 'base64' ;
LEFT                    : 'left' ;
RIGHT                   : 'right' ;
CENTER                  : 'center' ;
SPATH                   : 'spath' ;
NUMBER                  : '-'? [0-9]+ ('.' [0-9]+)? ;
DOUBLE_QUOTED_STRING    : '"' (~["\r\n])* '"' ;
BOOLEAN                 : 'TRUE' | 'FALSE' ;
BOOLNUM                 : '0' | '1' ;
BACKTICK                : '`' ;
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
BY                      : 'BY' ;
AS                      : 'AS' ;
IN                      : 'IN' ;
IF                      : 'if' ;
ELSE                    : 'else' ;
CASE                    : 'case' ;
VARIABLE                : [a-zA-Z_] [a-zA-Z_0-9]* (NUMBER)?;
NEWLINE                 : '\r'? '\n' | EOF;
COMMENT                 : '#' ~[\r\n]* NEWLINE -> skip ;
WS                      : [ \t]+ -> skip ; // skip spaces and tabs

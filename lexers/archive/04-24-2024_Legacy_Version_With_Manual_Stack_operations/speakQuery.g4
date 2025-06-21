grammar speakQuery;

program: speakQuery ;

speakQuery
    : tableCall validLine* EOF?
    | tableCall timerangeCall validLine* EOF?
    ;

tableCall
    : NEWLINE* PIPE TABLE FILE EQUALS tableName (COMMA tableName)* NEWLINE (sqlDrillQuery NEWLINE)?
    | NEWLINE* PIPE INPUTLOOKUP (variableName | staticString) (variableName | staticString)? NEWLINE?
    | NEWLINE* PIPE LOADJOB (JOBNAME | staticString) NEWLINE?
    ;

datetimeField
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

sqlDrillQuery
    : DOUBLE_QUOTED_STRING
    ;

tableName
    : staticString
    ;

timerangeCall
    : PIPE TIMERANGE LPAREN staticString COMMA staticString COMMA VARIABLE RPAREN NEWLINE?
    ;

validLine
    : PIPE directive NEWLINE
    ;

directive
    : SEARCH NOT? (logicalExpr COMMA?)+
    | WHERE NOT? (logicalExpr COMMA?)+
    | EVAL (staticString | variableName) EQUALS expression (COMMA (staticString | variableName) EQUALS expression)*
    | STREAMSTATS (CURRENT EQUALS BOOLEAN)? (WINDOW EQUALS staticNumber)? (NEWLINE)? (statsFunctionCall (AS variableName COMMA? NEWLINE?)?)* (NEWLINE? COUNT (AS variableName)?)? (BY (variableName)+)? (RESET_AFTER variableName EQUALS (variableName | unaryExpr | staticString))?
    | EVENTSTATS (NEWLINE)? (statsFunctionCall (AS variableName (NEWLINE)?)?)+ (NEWLINE? COUNT (AS variableName)?)? (BY (variableName)+)?
    | STATS (NEWLINE)? (statsFunctionCall (AS variableName (NEWLINE)?)?)+ (NEWLINE? COUNT (AS variableName)?)? (BY (variableName)+)?
    | TIMECHART (NEWLINE)? (statsFunctionCall (AS variableName (NEWLINE)?)?)+ (NEWLINE? COUNT (AS variableName)?)? (BY (variableName)+)?
    | RENAME NEWLINE? (variableName AS (variableName | staticString) COMMA? NEWLINE?)+
    | FIELDS (PLUS | MINUS)? variableName ((COMMA)? variableName)*
    | MAKETABLE variableName ((COMMA)? variableName)*
    | LOOKUP filename sharedField
    | FIELDSUMMARY LPAREN RPAREN  // Provides a total field summary at a given point in a speakQuery
    | (HEAD | LIMIT) staticNumber
    | BIN variableName SPAN EQUALS timespan
    | REVERSE
    | DEDUP staticNumber? (CONSECUTIVE EQUALS BOOLEAN)? variableName (COMMA? variableName)*
    | SORT (PLUS | MINUS) (variableName COMMA?)+
    | REX FIELD EQUALS variableName (MODE EQUALS SED)? (MAX_MATCH = staticNumber)? staticString
    | REGEX variableName (EQUALS | NOT_EQUALS) (variableName | staticString)
    | BASE64 (ENCODE | DECODE) (variableName | staticString) ((COMMA)? (variableName | staticString))*
    | SPECIAL_FUNC BACKTICK specialFunctionName BACKTICK  // This takes the place of for_each, allowing you to build python code for small special functions in lou of monolithic FOREACH
    | FILLNULL VALUE EQUALS (staticString | staticNumber | variableName) (variableName (COMMA)?)*
    | OUTPUTLOOKUP (OVERWRITE EQUALS BOOLEAN)? (OVERWRITE_IF_EMPTY EQUALS BOOLEAN)? (APPEND EQUALS BOOLEAN)? (variableName | staticString)
    | SPATH variableName
    | JOIN TYPE EQUALS (LEFT | CENTER | RIGHT) variableName LBRACK speakQuery RBRACK
    | TO_EPOCH LPAREN variableName RPAREN  // Convert a string field that looks as a datetime to epoch, adding a new column to the dataframe
    | APPENDPIPE LBRACK (NEWLINE? validLine)+ RBRACK
    ;

sharedField
    : VARIABLE
    | DOUBLE_QUOTED_STRING
    ;

filename
    : JOBNAME
    | VARIABLE
    | DOUBLE_QUOTED_STRING
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
    | staticString
    | variableName // Place this last to ensure all other options are considered first
    | multivalueField
    ;

// *************************************************************************************
// Logical, Comparison, and Arithmetic Expressions
// *************************************************************************************

logicalExpr
    : LPAREN logicalExpr RPAREN
    | logicalExpr ((AND | OR) comparisonExpr)+
    | inExpression ((AND | OR) comparisonExpr)+
    | inExpression ((AND | OR) inExpression)+
    | (inExpression | comparisonExpr)
    ;

comparisonExpr
    : comparisonExpr comparisonOperator additiveExpr
    | additiveExpr
    ;

comparisonOperator
    : (EQUALS_EQUALS | NOT_EQUALS | GT | LT | GTEQ | LTEQ)
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
    | variableName
    | LPAREN expression RPAREN
    | booleanExpr
    | unaryExpr
    | staticNumber
    | staticString
    ;

booleanExpr
    : BOOLEAN
    ;

// *************************************************************************************
// If, Case, IN Expressions
// *************************************************************************************

ifExpression
    : IF LPAREN NOT? expression COMMA expression COMMA catchAllExpression RPAREN
    ;

caseExpression
    : CASE LPAREN (NEWLINE)? (caseMatch COMMA caseTrue COMMA (NEWLINE)?)+ catchAllExpression RPAREN
    ;

caseMatch
    : inExpression
    | logicalExpr
    ;

caseTrue
    : expression
    ;

inExpression
    : (NOT)? variableName IN LPAREN (expression (COMMA expression)*)? RPAREN
    ;

catchAllExpression
    : functionCall
    | additiveExpr
    | staticString
    | variableName  // Place this last to ensure all other options are considered first
    ;

// *************************************************************************************
// Unary Expressions and Value Handling
// *************************************************************************************

unaryExpr
    : unaryPrefix
    | unaryPostfix
    | staticNumber // Ensure this is considered if neither postfix nor prefix expressions match
    ;

unaryPrefix
    : (INC | DEC | BITWISE_NOT) NUMBER
    ;

unaryPostfix
    : NUMBER (INC | DEC | BITWISE_NOT)
    ;

staticNumber
    : NUMBER
    ;

staticString
    : DOUBLE_QUOTED_STRING
    ;

// *************************************************************************************
// Functions
// *************************************************************************************

functionCall
    : numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    ;

numericFunctionCall  // Numeric function calls
    : ROUND LPAREN expression COMMA expression RPAREN  // Round a number to the nearest integer specified
    | MIN LPAREN expression RPAREN  // Find the minimum of a provided list or column of values
    | MAX LPAREN expression RPAREN  // Find the maximum of a provided list or column of values
    | AVG LPAREN expression RPAREN  // Find the average of a provided list or column of values
    | SUM LPAREN expression RPAREN  // Find the sum of a provided list or column of values
    | RANGE LPAREN expression RPAREN  // Find the range of a provided list or column of values
    | MEDIAN LPAREN expression RPAREN  // Find the median of a provided list or column of values
    | MODE LPAREN expression RPAREN  // Find the mode of a provided list or column of values
    | SQRT LPAREN expression RPAREN  // Find the squre root of a provided list or column of values
    | ABS LPAREN expression RPAREN  // Find the absolute value of a provided list or column of values
    | RANDOM LPAREN (expression COMMA expression COMMA expression)? RPAREN  // Produce a random integer value if no expressions are provided, else choose a random value between the range provided, incrementing by the last provided parameter which represents fidelity or resolution.
    ;

stringFunctionCall  // String function calls
    : CONCAT LPAREN expression (COMMA expression)* RPAREN  // Concatente infinate orderings of strings
    | REPLACE LPAREN expression COMMA expression COMMA expression RPAREN  // Replace ultimately a string with a string value
    | REPEAT LPAREN expression COMMA staticNumber RPAREN  // Repeeat a typically string expression a number of times
    | UPPER LPAREN expression RPAREN  // Uppercase a string
    | LOWER LPAREN expression RPAREN  // Lowercase a string
    | CAPITALIZE LPAREN expression RPAREN  // Capitalize a string
    | LEN LPAREN expression RPAREN  // Find the character length of a string.
    | TOSTRING LPAREN expression RPAREN  // Convert a non-string object to a string/
    | URLENCODE LPAREN expression RPAREN  // Use URL encoding against string that appears as a URL
    | URLDECODE LPAREN expression RPAREN  // Use URL decodding against an encoded URL string.
    | DEFANG LPAREN expression RPAREN  // Defang a provided URL (i.e. replace all . instances with [.] for safety
    | FANG LPAREN expression RPAREN  // Perform the opposite of a DEFANG, revsering the safety mechanism
    | TRIM LPAREN expression (COMMA expression)? RPAREN  // Trim an expression from left and right of a string. Assume spaces if no second paramter provided.
    | RTRIM LPAREN expression (COMMA expression)? RPAREN  // Same as Trim, but ONLY for the right side of a string.
    | LTRIM LPAREN expression (COMMA expression)? RPAREN  // Same as Trim, but ONLY for the left side of a string.
    | SUBSTR LPAREN expression COMMA expression COMMA expression RPAREN  // Return a substring of a string by the index start and stop values specified.
    | (NOT)? MATCH LPAREN (variableName | DOUBLE_QUOTED_STRING) COMMA regexTarget RPAREN  // Return a BOOLEAN indicating whether a regex expression exists in a provided string or not.
    ;

specificFunctionCall  // Other specific function calls
    : NULL LPAREN RPAREN  // Effectively, this is speakQuery's way of saying "Nothing", "None", or "Null", etc.
    | (NOT)? ISNULL LPAREN variableName RPAREN  // Check each of a fields value, returning True if a value is null for each
    | TO_CRON LPAREN inputCron RPAREN // Convert a count of seconds to its equivalent cron string
    | FROM_CRON LPAREN inputCron COMMA cronformat RPAREN  // Convert from a cron string to a date range in seconds
    | COALESCE LPAREN variableName (COMMA variableName)+ RPAREN  // Pick the first non-null value found between the ordered, provided set
    | MVJOIN LPAREN expression COMMA mvDelim RPAREN  // Join two fields by a delimiter
    | MVINDEX LPAREN expression COMMA mvindexIndex RPAREN  // Return a given index # from a multivalue field
    | MVREVERSE LPAREN expression RPAREN  // Revese a multivalue field's order
    | MVFIND LPAREN expression COMMA mvfindObject RPAREN  // Find a string in a string
    | MVDEDUP LPAREN expression RPAREN // Dedup a multivalue field
    | MVAPPEND LPAREN expression (COMMA (variableName | comparisonExpr))+ RPAREN  // Add new column(s)
    | MVFILTER LPAREN expression RPAREN  // Remove items from a multivalue field
    | MVEXPAND LPAREN expression RPAREN  // Expand a multivalue field
    | MVCOMBINE LPAREN expression COMMA mvDelim RPAREN   // Combine a multivalue field by a delimiter
    | MVCOUNT LPAREN expression RPAREN  // Count the entries in a multivalue field
    | MVDCOUNT LPAREN expression RPAREN  // Distinct count of entries in a multivalue field
    | MVZIP LPAREN expression (COMMA variableName)+ COMMA mvDelim RPAREN  // Join two different fields by a delimiter
    | MACRO LPAREN executionMaro RPAREN  // Reusable subpiece of any valid speakQuery code
    ;

statsFunctionCall  // For Stats function calls only
    : VALUES LPAREN expression RPAREN (AS variableName)?  // Return a set of unique values as a multivalue field
    | LATEST LPAREN expression RPAREN (AS variableName)?  // Returns the latest value provided in a column
    | EARLIEST LPAREN expression RPAREN (AS variableName)?  // Returns the earliest value provided in a column
    | FIRST LPAREN expression RPAREN (AS variableName)?  // Returns the FIRST value provided in a column. Ordering is important here.
    | LAST LPAREN expression RPAREN (AS variableName)?  // Returns the LAST value provided in a column. Ordering is important here.
    | DCOUNT LPAREN expression RPAREN (AS variableName)?  // Returns the Distinct Count of values provided in a column.
    | numericFunctionCall
    ;

// *************************************************************************************
// Function Parameters
// *************************************************************************************

stringFunctionTarget
    : variableName
    | DOUBLE_QUOTED_STRING
    | staticMultivalueStringField
    ;

httpStringField
    : variableName
    | DOUBLE_QUOTED_STRING
    | staticMultivalueStringField
    ;

multivalueField
    : multivalueStringField
    | multivalueNumericField
    ;

multivalueStringField
    : variableName
    | staticMultivalueStringField
    ;

multivalueNumericField
    : variableName
    | staticMultivalueNumericField
    ;

staticMultivalueStringField
    : LBRACK DOUBLE_QUOTED_STRING (COMMA DOUBLE_QUOTED_STRING)+ RBRACK
    ;

staticMultivalueNumericField
    : LBRACK expression (COMMA expression)+ RBRACK
    ;

regexTarget
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

trimTarget
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

substrTarget
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

substrStart
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

substrLength
    : variableName
    | DOUBLE_QUOTED_STRING
    ;

trimRemovalTarget
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
    | timeStringValue
    ;

cronformat
    : timeStringValue
    ;

executionMaro
    : variableName
    ;

timeStringValue
    : DOUBLE_QUOTED_STRING
    ;

timespan
    : NUMBER (SECONDS | MINUTES | HOURS | DAYS | WEEKS | YEARS)  // seconds, minutes, hours, days, weeks
    ;

variableName
    : VARIABLE
    | SINGLE_QUOTED_STRING
    ;

// *************************************************************************************
// LEXER Primatives
// *************************************************************************************

PIPE                    : '|' ;
CURRENT                 : ('current' | 'CURRENT') ;
TABLE                   : 'table' ;
MAKETABLE               : 'maketable' ;
FIELDSUMMARY            : 'fieldsummary' ;
CONSECUTIVE             : 'consecutive' ;
SPECIAL_FUNC            : 'special_func' ;
REPEAT                  : 'repeat' ;
TIMECHART               : 'timechart' ;
MAX_MATCH               : ('max_match' | 'MAX_MATCH') ;
MVEXPAND                : 'mvexpand' ;
MVREVERSE               : 'mvreverse' ;
MVCOMBINE               : 'mvcombine' ;
TO_EPOCH                : ('TO_EPOCH' | 'to_epoch') ;
MVFIND                  : 'mvfind' ;
MVDEDUP                 : 'mvdedup' ;
MVAPPEND                : 'mvappend' ;
MVFILTER                : 'mvfilter' ;
MVCOUNT                 : 'mvcount' ;
DCOUNT                  : 'dcount' ;
MVDCOUNT                : 'mvdcount' ;
MVZIP                   : 'mvzip' ;
TIMERANGE               : 'timerange' ;
LATEST                  : 'latest' ;
EARLIEST                : 'earliest' ;
FIRST                   : 'first' ;
LAST                    : 'last' ;
MACRO                   : 'macro' ;
DELIM                   : ('delim' | 'DELIM') ;
FILE                    : ('file' | 'FILE') ;
SORT                    : 'sort' ;
NULL                    : 'null' ;
EVAL                    : 'eval' ;
SPAN                    : ('span' | 'SPAN') ;
BIN                     : 'bin' ;
STREAMSTATS             : 'streamstats' ;
RESET_AFTER             : 'reset_after' ;
EVENTSTATS              : 'eventstats' ;
STATS                   : 'stats' ;
VALUES                  : 'values' ;
VALUE                   : ('value' | 'VALUE') ;
WHERE                   : ('WHERE' | 'where') ;
RENAME                  : 'rename' ;
REVERSE                 : 'reverse' ;
FIELD                   : ('field' | 'FIELD') ;
FIELDS                  : 'fields' ;
APPENDPIPE              : 'appendpipe' ;
APPEND                  : 'append' ;
SEARCH                  : 'search' ;
HEAD                    : 'head' ;
LIMIT                   : ('limit' | 'LIMIT') ;
REX                     : 'rex' ;
REGEX                   : 'regex' ;
DEDUP                   : 'dedup' ;
LOADJOB                 : 'loadjob' ;
INPUTLOOKUP             : 'inputlookup' ;
OUTPUTNEW               : ('outputnew' | 'OUTPUTNEW') ;
OUTPUTLOOKUP            : 'outputlookup' ;
LOOKUP                  : 'lookup' ;
WINDOW                  : ('window' | 'WINDOW') ;
OVERWRITE_IF_EMPTY      : ('overwrite_if_empty' | 'OVERWRITE_IF_EMPTY') ;
OVERWRITE               : ('overwrite' | 'OVERWRITE') ;
CREATE_EMPTY            : ('create_empty' | 'CREATE_EMPTY') ;
FILLNULL                : 'fillnull' ;
DEFANG                  : 'defang';
FANG                    : 'fang' ;
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
BOOLEAN                 : ('TRUE' | 'True' | 'true' | 'FALSE' | 'False' | 'false') ;
SECONDS                 : ('second' | 'seconds') ;
MINUTES                 : ('minute' | 'minutes') ;
HOURS                   : ('hour' | 'hours') ;
DAYS                    : ('day' | 'days') ;
WEEKS                   : ('week' | 'weeks') ;
YEARS                   : ('year' | 'years') ;
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
AND                     : ('AND' | 'and') ;
NOT                     : ('NOT' | 'not');
OR                      : ('OR' | 'or') ;
BY                      : ('BY' | 'by');
AS                      : ('AS' | 'as') ;
IN                      : ('IN' | 'in') ;
IF                      : 'if' ;
ELSE                    : 'else' ;
CASE                    : 'case' ;
NUMBER                  : '-'? [0-9]+ ('.' [0-9]+)? ;
SINGLE_QUOTED_STRING    : '\'' (~[\\'\r\n])* '\'' ;
DOUBLE_QUOTED_STRING    : '"' (~["\r\n])* '"' ;
PERIOD                  : '.' ;
VARIABLE                : [a-zA-Z_] [a-zA-Z_0-9.]* (NUMBER)?;
JOBNAME                 : [0-9] [a-zA-Z_0-9\-]*;
NEWLINE                 : '\r'? '\n' | EOF;
COMMENT                 : '#' ~[\r\n]* NEWLINE -> skip ;
WS                      : [ \t]+ -> skip ; // skip spaces and tabs

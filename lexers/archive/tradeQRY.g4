grammar tradeQRY;

program: tradeQuery ;

tradeQuery
    : (NEWLINE)? PIPE TABLE FILE EQUALS stringValue ((COMMA)? stringValue)* (WHERE logicalExpr ((COMMA)? logicalExpr)*)? NEWLINE (validLine)*
    (PIPE TIMERANGE LPAREN (expressionAny | functionCall) COMMA (expressionAny | variableValue) COMMA variableValue RPAREN NEWLINE)?
    ;

validLine
    : PIPE directive NEWLINE (PIPE directive NEWLINE)*
    ;

directive
    : SEARCH logicalExpr ((COMMA)? logicalExpr)*
    | (REVERSE | DEDUP)
    | EVAL (stringValue|variableValue) EQUALS expression ((COMMA)? (stringValue|variableValue) EQUALS expression)*
    | STATS functionCall (AS valueStringOnly)? ((COMMA)? functionCall (AS valueStringOnly)?)* ((COUNT)? BY (variableValue)+)?
    | EVENTSTATS functionCall (AS valueStringOnly)? ((COMMA)? functionCall (AS valueStringOnly)?)* ((COUNT)? BY (variableValue)+)?
    | RENAME variableValue AS expressionString ((COMMA)? variableValue AS valueStringOnly)*
    | FIELDS variableValue ((COMMA)? variableValue (AS valueStringOnly)?)*
    | HEAD valueNumericResultOnly
    | REX FIELD EQUALS variableValue stringValue
    | REGEX variableValue EQUALS stringValue
    | numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    | JOIN LBRACK tradeQuery RBRACK
    ;

expression
    : expressionNumeric
    | expressionString
    | expressionAny
    ;

// *************************************************************************************
// String Expression Path
// *************************************************************************************

expressionString
    : stringFunctionCall
    | logicalStringExpr
    ;

logicalStringExpr
    : LPAREN logicalStringExpr RPAREN
    | arithmeticStringExpr (AND arithmeticStringExpr | OR arithmeticStringExpr)*
    ;

arithmeticStringExpr
    : LPAREN arithmeticStringExpr RPAREN
    | arithmeticStringExpr (PLUS (factorStringExpr | arithmeticStringExpr))+  // Chaining
    | factorStringExpr
    ;

factorStringExpr
    : arrayValue
    | LPAREN expressionString RPAREN
    | value // Treat any result as a string here, regardless of type.
    ;

// *************************************************************************************
// Numeric Expression Path
// *************************************************************************************

expressionNumeric
    : LPAREN expressionNumeric RPAREN
    | numericFunctionCall
    | compareNumericExpr
    ;

compareNumericExpr
    : LPAREN compareNumericExpr RPAREN
    | compareNumericExpr comparisonOperator arithmeticNumericResultOnlyExpr // Chaining
    | arithmeticNumericResultOnlyExpr
    ;

arithmeticNumericResultOnlyExpr
    : LPAREN arithmeticNumericResultOnlyExpr RPAREN
    | arithmeticExpr ((PLUS | MINUS) (termNumericResultOnlyExpr | arithmeticExpr))+ // Chaining
    | termNumericResultOnlyExpr
    ;

termNumericResultOnlyExpr
    : LPAREN termNumericResultOnlyExpr RPAREN
    | termNumericResultOnlyExpr (MUL | DIV) factorNumericResultOnlyExpr // Chaining
    | factorNumericResultOnlyExpr
    ;

factorNumericResultOnlyExpr
    : arrayValueNumeric
    | LPAREN expressionNumeric RPAREN
    | valueNumericResultOnly
    ;

// *************************************************************************************
// Any/ALL Expression Path
// *************************************************************************************

expressionAny // Enforcing order for ALL and/or ANY collection of statments, if they are put together.
    : functionCall
    | logicalExpr
    ;

logicalExpr
    : LPAREN logicalExpr RPAREN
    | compareExpr (AND compareExpr | OR compareExpr)*
    ;

compareExpr
    : LPAREN compareExpr RPAREN
    | arithmeticExpr (comparisonOperator arithmeticExpr)?
    ;

arithmeticExpr
    : LPAREN arithmeticExpr RPAREN
    | arithmeticExpr ((PLUS | MINUS) (termExpr | arithmeticExpr))+  // Chaining
    | termExpr
    ;

termExpr
    : LPAREN termExpr RPAREN
    | termExpr (MUL | DIV) factorExpr // Chaining
    | factorExpr
    ;

factorExpr
    : arrayValue
    | LPAREN expressionAny RPAREN
    | value
    ;

// *************************************************************************************
// Value Path Terminations
// *************************************************************************************

value
    : unaryExpr
    | functionCall
    | numericValue
    | booleanValue
    | stringValue
    | variableValue
    ;

valueStringOnly
    : stringValue
    ;

valueNumericResultOnly
    : unaryExpr
    | numericFunctionCall
    | numericValue
    | BOOLNUM
    | variableValue
    ;

unaryExpr
    : numericValue (INC | DEC | BITWISE_NOT)
    | (INC | DEC | BITWISE_NOT) numericValue
    | '-' numericValue
    ;

// *************************************************************************************
// Value Constructs
// *************************************************************************************

numericValue: NUMBER ;
booleanValue: BOOLEAN | BOOLNUM ;
stringValue : DOUBLE_QUOTED_STRING ;
arrayValue : LBRACK expressionAny (COMMA expressionAny)+ RBRACK ;
arrayValueNumeric : LBRACK expressionNumeric (COMMA expressionNumeric)+ RBRACK ;
variableValue : ID ;

// *************************************************************************************
// Operator Constructs
// *************************************************************************************

comparisonOperator
    : EQUALS_EQUALS
    | NOT_EQUALS
    | GT
    | LT
    | GTEQ
    | LTEQ
    ;

logicalOperator
    : AND
    | OR
    ;

// *************************************************************************************
// Functions
// *************************************************************************************

functionCall
    : numericFunctionCall
    | stringFunctionCall
    | specificFunctionCall
    ;

// Numeric function calls
numericFunctionCall
    : ROUND LPAREN (expressionNumeric | numericFunctionCall) COMMA (expressionNumeric | numericFunctionCall) RPAREN
    | MIN LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | MAX LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | AVG LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | SUM LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | RANGE LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | MEDIAN LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | MODE LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | DCOUNT LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | SQRT LPAREN (expressionNumeric | numericFunctionCall) RPAREN
    | RANDOM LPAREN ((expressionNumeric | numericFunctionCall) (COMMA (expressionNumeric | numericFunctionCall))?)? RPAREN
    ;

// String function calls
stringFunctionCall
    : CONCAT LPAREN (expressionString | functionCall) (COMMA (expressionString | functionCall))* RPAREN
    | REPLACE LPAREN (expressionString | functionCall) COMMA (expressionString | functionCall) COMMA (expressionString | functionCall) RPAREN
    | UPPER LPAREN (expressionString | functionCall) RPAREN
    | LOWER LPAREN (expressionString | functionCall) RPAREN
    | CAPITALIZE LPAREN (expressionString | functionCall) RPAREN
    | LEN LPAREN (expressionString | functionCall) RPAREN
    | URLENCODE LPAREN (expressionString | functionCall) RPAREN
    | URLDECODE LPAREN (expressionString | functionCall) RPAREN
    | DEFANG LPAREN (expressionString | functionCall) RPAREN
    | TRIM LPAREN (expressionString | functionCall) (COMMA (expressionString | functionCall))? RPAREN
    | RTRIM LPAREN (expressionString | functionCall) (COMMA (expressionString | functionCall))? RPAREN
    | LTRIM LPAREN (expressionString | functionCall) (COMMA (expressionString | functionCall))? RPAREN
    | SUBSTR LPAREN (expressionString | functionCall) COMMA (expressionString | functionCall) (COMMA (expressionString | functionCall))? RPAREN
    | MATCH LPAREN (expressionString | functionCall) COMMA (expressionString | functionCall) RPAREN
    ;

// Other specific function calls
specificFunctionCall
    : NULL LPAREN RPAREN
    | ISNULL LPAREN (expressionAny | functionCall) RPAREN
    | ISNOTNULL LPAREN (expressionAny | functionCall) RPAREN
    | TIME LPAREN (expressionAny | functionCall) RPAREN
    | TIMERANGE LPAREN (expressionAny | functionCall) COMMA (expressionAny | variableValue) COMMA variableValue RPAREN // Initial Timerange
    | FIELDSUMMARY LPAREN RPAREN
    | COALESCE LPAREN (expressionAny | functionCall) (COMMA (expressionAny | functionCall))+ RPAREN
    | MVJOIN LPAREN (expressionAny | functionCall) (COMMA (expressionAny | functionCall))+ RPAREN
    | MVINDEX LPAREN (expressionAny | functionCall) COMMA (expressionAny | functionCall) RPAREN
    | MVEXPAND LPAREN (expressionAny | functionCall) RPAREN
    | VALUES LPAREN variableValue RPAREN (AS stringValue)?      // For Stats
    | LATEST LPAREN variableValue RPAREN (AS stringValue)?      // For Stats
    | EARLIEST LPAREN variableValue RPAREN (AS stringValue)?    // For Stats
    | FIRST LPAREN variableValue RPAREN (AS stringValue)?       // For Stats
    | LAST LPAREN variableValue RPAREN (AS stringValue)?        // For Stats
    | MACRO LPAREN stringValue RPAREN
    ;

// LEXER Primatives --------------------------------------
PIPE                    : '|' ;
COMMENT                 : '#' ;
TABLE                   : 'table' ;
TICK                    : '`' ;
FIELDSUMMARY           : 'fieldsummary' ;
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
BY                      : 'by' ;
AS                      : 'as' ;
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
AND                     : 'AND' ;
OR                      : 'OR' ;
ID                      : [a-zA-Z_] [a-zA-Z_0-9]* (NUMBER)?;
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
NEWLINE                 : '\r'? '\n' | EOF;
WS                      : [ \t]+ -> skip ; // skip spaces and tabs

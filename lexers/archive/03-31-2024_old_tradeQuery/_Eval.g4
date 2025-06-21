grammar Eval;

program: expression NEWLINE ;

expression
    : expressionString
    | expressionNumeric
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
    : arithmeticStringExpr (AND arithmeticStringExpr | OR arithmeticStringExpr)*
    ;

arithmeticStringExpr
    : arithmeticStringExpr (PLUS (factorStringExpr | arithmeticStringExpr))+  // Chaining
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
    : numericFunctionCall
    | compareNumericExpr
    ;

compareNumericExpr
    : compareNumericExpr comparisonOperator arithmeticNumbericResultOnlyExpr // Chaining
    | arithmeticNumbericResultOnlyExpr
    ;

arithmeticNumbericResultOnlyExpr
    : arithmeticExpr ((PLUS | MINUS) (termNumbericResultOnlyExpr | arithmeticExpr))+ // Chaining
    | termNumbericResultOnlyExpr
    ;

termNumbericResultOnlyExpr
    : termNumbericResultOnlyExpr (MUL | DIV) factorNumbericResultOnlyExpr // Chaining
    | factorNumbericResultOnlyExpr
    ;

factorNumbericResultOnlyExpr
    : arrayValueNumeric
    | LPAREN expressionNumeric RPAREN
    | valueNumbericResultOnly
    ;

// *************************************************************************************
// Any/ALL Expression Path
// *************************************************************************************

expressionAny // Enforcing order for ALL and/or ANY collection of statments, if they are put together.
    : functionCall
    | logicalExpr
    ;

logicalExpr
    : compareExpr (AND compareExpr | OR compareExpr)*
    ;

compareExpr
    : arithmeticExpr (comparisonOperator arithmeticExpr)?
    ;

arithmeticExpr
    : arithmeticExpr ((PLUS | MINUS) (termExpr | arithmeticExpr))+  // Chaining
    | termExpr
    ;

termExpr
    : termExpr (MUL | DIV) factorExpr // Chaining
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

valueNumbericResultOnly
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
stringValue : SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING ;
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
    : ROUND LPAREN expressionNumeric COMMA expressionNumeric RPAREN
    | MIN LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | MAX LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | AVG LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | SUM LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | RANGE LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | MEDIAN LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    | SQRT LPAREN expressionNumeric RPAREN
    | RANDOM LPAREN RPAREN // No Args
    | TONUMBER LPAREN expressionNumeric RPAREN
    | DCOUNT LPAREN expressionNumeric (COMMA expressionNumeric)* RPAREN
    ;

// String function calls
stringFunctionCall
    : CONCAT LPAREN expressionString (COMMA expressionString)* RPAREN
    | REPLACE LPAREN expressionString COMMA expressionString COMMA expressionString RPAREN
    | UPPER LPAREN expressionString RPAREN
    | LOWER LPAREN expressionString RPAREN
    | CAPITALIZE LPAREN expressionString RPAREN
    | TRIM LPAREN expressionString (COMMA expressionString)? RPAREN
    | RTRIM LPAREN expressionString (COMMA expressionString)? RPAREN
    | LTRIM LPAREN expressionString (COMMA expressionString)? RPAREN
    | SUBSTR LPAREN expressionString COMMA expressionString (COMMA expressionString)? RPAREN
    | LEN LPAREN expressionString RPAREN
    | TOSTRING LPAREN expressionString RPAREN
    | MATCH LPAREN expressionString COMMA expressionString RPAREN
    | URLENCODE LPAREN expressionString RPAREN
    | URLDECODE LPAREN expressionString RPAREN
    | DEFANG LPAREN expressionString RPAREN
    | TYPEOF LPAREN expressionString RPAREN
    ;

// Other specific function calls
specificFunctionCall
    : NULL LPAREN RPAREN
    | ISNULL LPAREN expressionAny RPAREN
    | ISNOTNULL LPAREN expressionAny RPAREN
    | COALESCE LPAREN expressionAny (COMMA expressionAny)+ RPAREN
    | MVJOIN LPAREN expressionAny (COMMA expressionAny)+ RPAREN
    | MVINDEX LPAREN expressionAny COMMA expressionAny RPAREN
    ;

// LEXER Primatives --------------------------------------
NULL: 'null' ;
DEFANG: 'defang';
ROUND: 'round' ;
MIN: 'min' ;
MAX: 'max' ;
MEDIAN: 'median' ;
AVG: 'avg' ;
SUM: 'sum' ;
RANDOM: 'random' ;
SQRT: 'sqrt' ;
RANGE: 'range' ;
DCOUNT: 'dcount' ;
TONUMBER: 'tonumber' ;
COALESCE: 'coalesce' ;
ISNULL: 'isnull' ;
ISNOTNULL: 'isnotnull' ;
LEN: 'len' ;
CONCAT: 'concat' ;
REPLACE: 'replace' ;
LOWER: 'lower' ;
UPPER: 'upper' ;
CAPITALIZE: 'capitalize' ;
LTRIM: 'ltrim' ;
RTRIM: 'rtrim' ;
TRIM: 'trim' ;
MATCH: 'match' ;
MVINDEX: 'mvindex' ;
MVJOIN: 'mvjoin' ;
SUBSTR: 'substr' ;
TOSTRING: 'tostring' ;
TYPEOF: 'typeof' ;
URLENCODE: 'urlencode' ;
URLDECODE: 'urldecode' ;
EVAL: 'eval' ;
AND: 'AND' ;
OR: 'OR' ;
ID: [a-zA-Z_] [a-zA-Z_0-9]* (NUMBER)?;
NUMBER: [0-9]+ ('.' [0-9]+)? ;
SINGLE_QUOTED_STRING : '\'' (~['\r\n])* '\'' ;
DOUBLE_QUOTED_STRING : '"' (~["\r\n])* '"' ;
BOOLEAN: 'TRUE' | 'FALSE' ;
BOOLNUM: '0' | '1' ;
LBRACK: '[' ;
RBRACK: ']' ;
LPAREN: '(' ;
RPAREN: ')' ;
COMMA: ',' ;
SEMICOLON: ';' ;
EQUALS: '=' ;
EQUALS_EQUALS: '==' ;
NOT_EQUALS: '!=' ;
GT: '>' ;
LT: '<' ;
GTEQ: '>=' ;
LTEQ: '<=' ;
PLUS: '+' ;
MINUS: '-' ;
MUL: '*' ;
DIV: '/' ;
INC: '++' ;
DEC: '--' ;
BITWISE_NOT: '~' ;
NEWLINE: '\r'? '\n' ;
WS: [ \t]+ -> skip ; // skip spaces and tabs

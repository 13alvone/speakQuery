grammar logicalClause;

logicalClause
    : multiLogicalExpression
    | singleLogicalExpression
    ;

multiLogicalExpression
    : singleLogicalExpression ((AND | OR) (singleLogicalExpression | singleComparison))*
    ;

singleLogicalExpression
    : LPAREN singleLogicalExpression RPAREN
    | singleComparison (AND | OR) singleComparison
    ;

singleComparison
    : variableName comparisonOperand comparisonSingleValue
    | variableName inOperand comparisonMultiValue
    ;

comparisonSingleValue
    : variableName
    | stringValue
    | numericValue
    ;

comparisonMultiValue
    : mvVariable
    | mvString
    | mvNumber
    | mvMixed
    ;

mvString
    : LPAREN stringValue (COMMA stringValue)+ RPAREN
    ;

mvNumber
    : LPAREN numericValue (COMMA numericValue)+ RPAREN
    ;

mvVariable
    : LPAREN variableName (COMMA variableName)+ RPAREN
    ;

mvMixed
    : LPAREN (stringValue | numericValue | variableName) (COMMA (stringValue | numericValue | variableName))+ RPAREN
    ;

stringValue
    : DOUBLE_QUOTED_STRING
    ;

numericValue
    : NUMBER
    ;

comparisonOperand
    : (EQ | NOTEQ | LTEQ | GTEQ | LT | GT | CONTAINS)
    ;

inOperand
    : IN
    ;

variableName
    : ID
    | SINGLE_QUOTED_STRING
    ;

WHERE                   : ('where' | 'WHERE') ;
AND                     : ('and' | 'AND') ;
OR                      : ('or' | 'OR') ;
NOT                     : ('not' | 'NOT') ;
IN                      : ('in' | 'IN') ;
EQ                      : '==' ;
NOTEQ                   : '!=' ;
LT                      : '<' ;
GT                      : '>' ;
LTEQ                    : '<=' ;
GTEQ                    : '>=' ;
CONTAINS                : '~=' ;
COMMA                   : ',' ;
SEMI                    : ';' ;
LPAREN                  : '(' ;
RPAREN                  : ')' ;
NUMBER                  : '-'? [0-9]+ ('.' [0-9]+)? ;
SINGLE_QUOTED_STRING    : '\'' (~[\\'\r\n])* '\'' ;
DOUBLE_QUOTED_STRING    : '"' (~["\r\n])* '"' ;
NEWLINE                 : '\r'? '\n' | EOF;
COMMENT                 : '#' ~[\r\n]* NEWLINE -> skip ;
ID                      : [a-zA-Z_][a-zA-Z_0-9]* ;
WS                      : [ \t]+ -> skip ; // skip spaces and tabs

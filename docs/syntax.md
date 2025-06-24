# SpeakQuery Syntax Reference

The query language is defined by `lexers/speakQuery.g4`. This document summarizes
its grammar and provides examples for common directives.

## Expressions

Expressions form the building blocks of filters and calculations. An expression
can contain logical operators and arithmetic:

```
expression       -> conjunction (OR conjunction)*
conjunction      -> comparison (AND comparison)*
comparison       -> additiveExpr (comparisonOperator additiveExpr)*
additiveExpr     -> multiplicativeExpr ((PLUS | MINUS) multiplicativeExpr)*
multiplicativeExpr -> unaryExpr ((MUL | DIV) unaryExpr)*
unaryExpr        -> (NOT | PLUS | MINUS)? primary
primary          -> '(' expression ')' | timeClause | indexClause |
                    inExpression | functionCall | variableName |
                    DOUBLE_QUOTED_STRING | NUMBER | BOOLEAN
```

`timeClause`, `indexClause` and `inExpression` allow specifying time ranges,
indexes and set membership respectively. Functions and variables may appear
inside any expression.

## Directives

Queries begin with an initial expression or a pipeline input command
(`| inputlookup` or `| loadjob`) followed by one or more pipe-delimited
**directives**. Directives implement filtering, evaluation and aggregation.
Key directives include:

- `search` / `where` – filter events using expressions
- `eval` – assign new fields using expressions
- `stats`, `eventstats`, `streamstats` – compute aggregations
- `timechart` – produce time-series aggregations
- `table` / `maketable` – select or create tables
- `rename`, `fields`, `sort`, `head`/`limit`, `dedup`, `lookup`, `join`,
  `append`, `appendpipe` and many multivalue (`mv*`) commands
- `rex` / `regex` – extract or filter using regular expressions
- `fillnull`, `spath`, `bin`, `reverse`, `base64`, `outputlookup` and others

Directive syntax often accepts expressions, variable lists or subsearches (a
bracketed `[...]` expression sequence).

## Functions

Functions appear within expressions. They are grouped as numeric, string or
special functions. Examples include:

- Numeric: `round`, `min`, `max`, `avg`, `sum`, `median`, `random`, `sqrt`
- String: `concat`, `replace`, `repeat`, `upper`, `lower`, `trim`, `substr`,
  `match`
- Special: `null()`, `isnull()`, `coalesce()`, `if_()`, `case()`, `tonumber()`,
  `to_cron()`, `from_cron()`
- Statistical: `count`, `values`, `latest`, `earliest`, `first`, `last`, `dc`

## Tokens

The grammar defines keywords and operators such as `AND`, `OR`, `NOT`, `BY`,
`IN`, arithmetic symbols (`+`, `-`, `*`, `/`), comparison operators (`=`,
`!=`, `>`, `<`, `>=`, `<=`), parentheses, brackets, commas and pipes.
Literals include numbers, single or double quoted strings, booleans and
variables (`[a-zA-Z_][a-zA-Z_0-9.]*`).

## Examples

The following examples demonstrate common directives in action:

```spl
index="logs/main.log" | search status=200
index="sales/data.parquet" | where revenue > 1000
index="metrics/parquet" | eval total=price * quantity
index="metrics/parquet" | stats avg(responseTime) as avg_resp by endpoint
index="traffic/parquet" | timechart span=1d count by pageURL
index="users/data.parquet" | join userID [ search index="profiles/data.parquet" | table userID userName ]
index="transactions/parquet" | lookup user_info.csv userID OUTPUT userName userEmail
```

For the full grammar see `lexers/speakQuery.g4`.

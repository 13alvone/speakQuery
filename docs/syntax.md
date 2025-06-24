# SpeakQuery Syntax Reference

The query language syntax is generated from [`lexers/speakQuery.g4`](../lexers/speakQuery.g4). This reference focuses on how to compose queries and use the main directives.

## Basic Query Structure

A query starts with either an initial expression or a pipeline input command (`| inputlookup` or `| loadjob`). Subsequent directives are separated by the pipe (`|`) character. Example:

```spl
index="logs/app.log" status="error" | stats count by host
```

## Expressions

Expressions provide filtering and calculations. They support logical and arithmetic operators:

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

`timeClause`, `indexClause` and `inExpression` specify time ranges, indexes and set membership. Functions and variables may appear anywhere inside an expression.

## Directives

### search / where
Filter events using expressions.

```spl
index="metrics" | search status="critical" AND errorCode=500
```

### eval
Assign or transform fields.

```spl
index="metrics" | eval total=price*quantity, tag=upper(category)
```

### stats and related aggregations
Create summaries with `stats`, `eventstats` or `streamstats`.

```spl
index="metrics" | stats avg(duration) as avg_dur by endpoint
```

### timechart
Produce time-series aggregations.

```spl
index="traffic" | timechart span=1h count by status
```

### multivalue operations
Use `mv*` commands to manipulate multivalue fields.

```spl
index="items" | mvexpand values | mvjoin(",", values)
```

### lookups and joins
Enrich results from external data.

```spl
index="orders" | lookup product.csv productId OUTPUT productName | join userId [ search index="users" | table userId userName ]
```

### regex and rex
Extract or filter with regular expressions.

```spl
index="logs" | rex field=message "error=(?<code>\d+)" | regex host!="test"
```

### output commands
Write search results to files.

```spl
index="metrics" | stats count by host | outputlookup results.csv
```

## Functions

### Numeric
Examples include `round`, `min`, `max`, `avg`, `sum`, `median`, `random`, `sqrt`.

```spl
... | eval ratio=round(bytes_sent/bytes_received,2)
```

### String
Common string helpers are `concat`, `replace`, `repeat`, `upper`, `lower`, `trim`, `substr`, `match`.

```spl
... | eval clean=trim(lower(name))
```

### Special
Utilities such as `null()`, `isnull()`, `coalesce()`, `if_()`, `case()`, `tonumber()`, `to_cron()`, `from_cron()`.

```spl
... | eval safe_num=coalesce(tonumber(size),0)
```

## Subsearches and Macros

Subsearches are bracketed sequences that run separately and feed their results to the outer query.

```spl
index="servers" [ earliest="-1d" action="reboot" index="maintenance" | stats count by host | fields host ] | stats count by host
```

Macros allow reusable logic. They are invoked using backticks.

```spl
index="logs" | `my_custom_macro(arg1="value")`
```

## Example Queries

The following concise samples are adapted from `lexers/example_valid_complex_queries.yaml`:

```spl
(index="system_logs/error_tracking" (status="error" OR status="critical") errorCode IN (403,404)) OR (index="finance_logs/transactions" transactionStatus="pending" amount>1000)
| eval note="example 1"
```

```spl
index="database_logs/operations" (systemStatus="degraded" AND serviceType="database") OR (status="down" AND region="APAC")
[ earliest="2024-04-01" latest="2024-06-30" action="reboot" index="server_logs/maintenance.parquet" | stats count by hostname | fields hostname ]
| eval insight="example bracket"
```

```spl
earliest="2023-12-01" latest="2023-12-31" action="login" success=false retryCount>=3 index="security/authentication" | eval note="login failures"
```

For the full grammar see [`lexers/speakQuery.g4`](../lexers/speakQuery.g4).

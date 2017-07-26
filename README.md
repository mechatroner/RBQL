# RBQL Description
RBQL is minimalistic but powerful SQL-like language that supports "select" queries with python expressions.
RBQL works with tsv and csv files, so you don't need a database to use it.
RBQL is similar to "awk" unix tool.

## Main Features
* Use python expressions inside "select", "where" and "order by" statements
* Use "a1", "a2", ... , "aN" as column names to write select queries
* Output entries appear in the same order as in input unless "ORDER BY" is provided.
* "lnum" variable holds entry line number
* Input csv/tsv table may contain varying number of entries (but select query must be written in a way that prevents output of missing values)

## Supported SQL Keywords (Keywords are case insensitive)
* select 
* where 
* order by
* desc/asc
* distinct

## Special variables
* `a1`, `a2`, ... , `aN` - column names
* `*` - whole line/entry
* `lnum` - line number (1-based)
* `flen` - number of columns in current line/entry

## Query examples

* `select * where lnum <= 10` - this is an equivalent of bash command "head -n 10", lnum is 1-based')
* `select a1, a4` - this is an equivalent of bash command "cut -f 1,4"
* `select * order by int(a2) desc` - this is an equivalent of bash command "sort -k2,2 -r -n"
* `select * order by random.random()` - random sort, this is an equivalent of bash command "sort -R"
* `select lnum, *` - enumerate lines, lnum is 1-based
* `select * where re.match(".*ab.*", a1) is not None` - select entries where first column has "ab" pattern

## rbql.py command line interface
CLI is self explanatory.
See `./rbql.py --help` for all options

### Examples
```
./rbql.py --query 'select a1, a2 order by a1' < input.tsv > output.tsv
```

## Requirements
* python2 or python3


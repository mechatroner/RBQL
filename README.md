# RBQL (RainBow Query Language) Description
RBQL is a technology which provides SQL-like language that supports _SELECT_ and _UPDATE_ queries with Python or JavaScript expressions.

### Main Features
* Use Python or Java Script expressions inside _SELECT_, _UPDATE_, _WHERE_ and _ORDER BY_ statements
* Result set of any query immediately becomes a first-class table on it's own.
* Output entries appear in the same order as in input unless _ORDER BY_ is provided.
* Input csv/tsv spreadsheet may contain varying number of entries (but select query must be written in a way that prevents output of missing values)
* Works out of the box, no external dependencies.

### Supported SQL Keywords (Keywords are case insensitive)

* SELECT \[ TOP _N_ \] \[ DISTINCT [ COUNT ] \]
* UPDATE \[ SET \]
* WHERE
* ORDER BY ... [ DESC | ASC ]
* [ [ STRICT ] LEFT | INNER ] JOIN
* GROUP BY
* LIMIT _N_

#### Keywords rules
All keywords have the same meaning as in SQL queries. You can check them [online](https://www.w3schools.com/sql/default.asp)
But there are also two new keywords: _DISTINCT COUNT_ and _STRICT LEFT JOIN_:
* _DISTINCT COUNT_ is like _DISTINCT_, but adds a new column to the "distinct" result set: number of occurences of the entry, similar to _uniq -c_ unix command.
* _STRICT LEFT JOIN_ is like _LEFT JOIN_, but generates an error if any key in left table "A" doesn't have exactly one matching key in the right table "B".

Some other rules:
* _UPDATE SET_ is synonym to _UPDATE_, because in RBQL there is no need to specify the source table.
* _UPDATE_ has the same semantic as in SQL, but it is actually a special type of _SELECT_ query.
* _JOIN_ statements must have the following form: _<JOIN\_KEYWORD> (/path/to/table.tsv | table_name ) ON ai == bj_
* _TOP_ and _LIMIT_ have identical semantic.

### Special variables

| Variable Name          | Variable Type | Variable Description                 |
|------------------------|---------------|--------------------------------------|
| _a1_, _a2_,..., _a{N}_   |string         | Value of i-th column                 |
| _b1_, _b2_,..., _b{N}_   |string         | Value of i-th column in join table B |
| _NR_                     |integer        | Line number (1-based)                |
| _NF_                     |integer        | Number of fields in line             |

### Aggregate functions and queries
RBQL supports the following aggregate functions, which can also be used with _GROUP BY_ keyword:

_COUNT_, _MIN_, _MAX_, _SUM_, _AVG_, _VARIANCE_, _MEDIAN_

**Limitations:**
* Aggregate function are CASE SENSITIVE and must be CAPITALIZED.
* It is illegal to use aggregate functions inside Python (or JS) expressions. Although you can use expressions inside aggregate functions.
  E.g. `MAX(float(a1) / 1000)` - legal; `MAX(a1) / 1000` - illegal.

### Examples of RBQL queries

#### With Python expressions

* `select top 100 a1, int(a2) * 10, len(a4) where a1 == "Buy" order by int(a2)`
* `select * order by random.random()` - random sort, this is an equivalent of bash command _sort -R_

#### With JavaScript expressions

* `select top 100 a1, a2 * 10, a4.length where a1 == "Buy" order by parseInt(a2)`
* `select * order by Math.random()` - random sort, this is an equivalent of bash command _sort -R_


# Other

### cli_rbql.py script

Usage example:

```
./cli_rbql.py --query "select a1, a2 order by a1" < input.tsv
```
To find out more about cli_rbql.py and available options, execute:
```
./cli_rbql.py -h
```

### How does it work?
Python module rbql.py parses RBQL query, creates a new python worker module, then imports and executes it.

### Some more examples of RBQL queries:

#### With Python expressions

* `select top 20 len(a1) / 10, a2 where a2 in ["car", "plane", "boat"]` - use Python's "in" to emulate SQL's "in"
* `select len(a1) / 10, a2 where a2 in ["car", "plane", "boat"] limit 20`
* `update set a3 = 'US' where a3.find('of America') != -1`
* `select * where NR <= 10` - this is an equivalent of bash command "head -n 10", NR is 1-based')
* `select a1, a4` - this is an equivalent of bash command "cut -f 1,4"
* `select * order by int(a2) desc` - this is an equivalent of bash command "sort -k2,2 -r -n"
* `select NR, *` - enumerate lines, NR is 1-based
* `select * where re.match(".*ab.*", a1) is not None` - select entries where first column has "ab" pattern
* `select a1, b1, b2 inner join ./countries.txt on a2 == b1 order by a1, a3` - an example of join query
* `select distinct count len(a1) where a2 != 'US'`
* `select MAX(a1), MIN(a1) where a2 != 'US' group by a2, a3`

#### With JavaScript expressions

* `select top 20 a1.length / 10, a2 where ["car", "plane", "boat"].indexOf(a2) > -1`
* `select a1.length / 10, a2 where ["car", "plane", "boat"].indexOf(a2) > -1 limit 20`
* `update set a3 = 'US' where a3.indexOf('of America') != -1`
* `select * where NR <= 10` - this is an equivalent of bash command "head -n 10", NR is 1-based')
* `select a1, a4` - this is an equivalent of bash command "cut -f 1,4"
* `select * order by parseInt(a2) desc` - this is an equivalent of bash command "sort -k2,2 -r -n"
* `select * order by Math.random()` - random sort, this is an equivalent of bash command "sort -R"
* `select NR, *` - enumerate lines, NR is 1-based
* `select a1, b1, b2 inner join ./countries.txt on a2 == b1 order by a1, a3` - an example of join query
* `select distinct count a1.length where a2 != 'US'`
* `select MAX(a1), MIN(a1) where a2 != 'US' group by a2, a3`


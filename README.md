# RBQL: Rainbow Query Language

RBQL is a technology for (not only) CSV file processing. It provides SQL-like language that supports SELECT queries with Python or JavaScript expressions.  
RBQL is distributed with CLI apps, text editor plugins, Python and JS libraries and can work in web browsers.  
RBQL core module is very generic and can process all kinds of objects and record formats, but the most popular RBQL implementation works with CSV files.  

[Official Site](https://rbql.org/)

#### Supported formats
Below is the matrix of input data formats that RBQL supports out of the box:  

|Input Format          | Python | JS |
|----------------------|--------|----|
|CSV, TSV, etc         | +      | +  |
|Sqlite databases      | +      | -  |
|Native 2D array/lists | +      | +  |

If you want to use RBQL as a library you can implement reading of additional custom formats.  

### Main Features

* Use Python or JavaScript expressions inside _SELECT_, _UPDATE_, _WHERE_ and _ORDER BY_ statements
* Result set of any query immediately becomes a first-class table on its own
* No need to provide FROM statement in the query - input table is defined by the current context
* Supports input tables with an inconsistent number of fields per record
* Output records appear in the same order as in input unless _ORDER BY_ is provided
* Each record has a unique NR (record number) identifier
* Supports all main SQL keywords
* Supports aggregate functions and GROUP BY queries
* Provides some new useful query modes which traditional SQL engines do not have
* Supports both _TOP_ and _LIMIT_ keywords
* Supports user-defined functions (UDF)
* Works out of the box, no external dependencies

#### Limitations:

* RBQL doesn't support nested queries, but they can be emulated with consecutive queries
* Number of tables in all JOIN queries is always 2 (input table and join table), use consecutive queries to join 3 or more tables

### Supported SQL Keywords (Keywords are case insensitive)

* SELECT
* UPDATE
* WHERE
* ORDER BY ... [ DESC | ASC ]
* [ LEFT | INNER ] JOIN
* DISTINCT
* GROUP BY
* TOP _N_
* LIMIT _N_

All keywords have the same meaning as in SQL queries. You can check them [online](https://www.w3schools.com/sql/default.asp)  


### RBQL variables
RBQL for CSV files provides the following variables which you can use in your queries:

* _a1_, _a2_,..., _a{N}_  
   Variable type: **string**  
   Description: value of i-th field in the current record in input table  
* _b1_, _b2_,..., _b{N}_  
   Variable type: **string**  
   Description: value of i-th field in the current record in join table B  
* _NR_  
   Variable type: **integer**  
   Description: Record number (1-based)  
* _NF_  
   Variable type: **integer**  
   Description: Number of fields in the current record  
* _a.name_, _b.Person_age_, ... _a.{Good_alphanumeric_column_name}_  
   Variable type: **string**  
   Description: Value of the field referenced by it's "name". You can use this notation if the field in the first (header) CSV line has a "good" alphanumeric name  
* _a["object id"]_, _a['9.12341234']_, _b["%$ !! 10 20"]_ ... _a["Arbitrary column name!"]_  
   Variable type: **string**  
   Description: Value of the field referenced by it's "name". You can use this notation to reference fields by arbitrary values in the first (header) CSV line, even when there is no header at all  


#### Notes:
* You can mix all variable types in a single query, example:
  ```select a1, b2 JOIN /path/to/b.csv ON a['Item Id'] == b.Identifier WHERE NR > 1 and int(a.Weight) * 100 > int(b["weight of the item"])```
* Referencing fields by header names does not automatically skip the header line (you can use `where NR > 1` trick to skip it)
* If you want to use RBQL as a library for your own app you can define your own custom variables and do not have to support the above mentioned CSV-related variables.


### UPDATE statement

_UPDATE_ query produces a new table where original values are replaced according to the UPDATE expression, so it can also be considered a special type of SELECT query. This prevents accidental data loss from poorly written queries.  
_UPDATE SET_ is synonym to _UPDATE_, because in RBQL there is no need to specify the source table.  


### Aggregate functions and queries

RBQL supports the following aggregate functions, which can also be used with _GROUP BY_ keyword:  
_COUNT_, _ARRAY_AGG_, _MIN_, _MAX_, _SUM_, _AVG_, _VARIANCE_, _MEDIAN_  

Limitation: aggregate functions inside Python (or JS) expressions are not supported. Although you can use expressions inside aggregate functions.  
E.g. `MAX(float(a1) / 1000)` - valid; `MAX(a1) / 1000` - invalid.  
There is a workaround for the limitation above for _ARRAY_AGG_ function which supports an optional parameter - a callback function that can do something with the aggregated array. Example:  
`select a2, ARRAY_AGG(a1, lambda v: sorted(v)[:5]) group by a2` - Python; `select a2, ARRAY_AGG(a1, v => v.sort().slice(0, 5)) group by a2` - JS


### JOIN statements

Join table B can be referenced either by its file path or by its name - an arbitrary string which the user should provide before executing the JOIN query.  
RBQL supports _STRICT LEFT JOIN_ which is like _LEFT JOIN_, but generates an error if any key in the left table "A" doesn't have exactly one matching key in the right table "B".  
Limitation: _JOIN_ statements can't contain Python/JS expressions and must have the following form: _<JOIN\_KEYWORD> (/path/to/table.tsv | table_name ) ON a... == b... [AND a... == b... [AND ... ]]_


### SELECT EXCEPT statement

SELECT EXCEPT can be used to select everything except specific columns. E.g. to select everything but columns 2 and 4, run: `SELECT * EXCEPT a2, a4`  
Traditional SQL engines do not support this query mode.


### UNNEST() operator
UNNEST(list) takes a list/array as an argument and repeats the output record multiple times - one time for each value from the list argument.  
Example: `SELECT a1, UNNEST(a2.split(';'))`  


### LIKE() function
RBQL does not support LIKE operator, instead it provides "like()" function which can be used like this:
`SELECT * where like(a1, 'foo%bar')`


### User Defined Functions (UDF)

RBQL supports User Defined Functions  
You can define custom functions and/or import libraries in two special files:  
* `~/.rbql_init_source.py` - for Python
* `~/.rbql_init_source.js` - for JavaScript


## Examples of RBQL queries

#### With Python expressions

* `select top 100 a1, int(a2) * 10, len(a4) where a1 == "Buy" order by int(a2) desc`
* `select * order by random.random() where NR > 1` - skip header record and random sort
* `select len(a.vehicle_price) / 10, a2 where NR > 1 and a['Vehicle type'] in ["car", "plane", "boat"] limit 20` - referencing columns by names from header record, skipping the header and using Python's "in" to emulate SQL's "in"
* `update set a3 = 'NPC' where a3.find('Non-playable character') != -1`
* `select NR, *` - enumerate records, NR is 1-based
* `select * where re.match(".*ab.*", a1) is not None` - select entries where first column has "ab" pattern
* `select a1, b1, b2 inner join ./countries.txt on a2 == b1 order by a1, a3` - example of join query
* `select MAX(a1), MIN(a1) where a.Name != 'John' group by a2, a3` - example of aggregate query
* `select *a1.split(':')` - Using Python3 unpack operator to split one column into many. Do not try this with other SQL engines!

#### With JavaScript expressions

* `select top 100 a1, a2 * 10, a4.length where a1 == "Buy" order by parseInt(a2) desc`
* `select * order by Math.random() where NR > 1` - skip header record and random sort
* `select top 20 a.vehicle_price.length / 10, a2 where NR > 1 and ["car", "plane", "boat"].indexOf(a['Vehicle type']) > -1 limit 20` - referencing columns by names from header record and skipping the header
* `update set a3 = 'NPC' where a3.indexOf('Non-playable character') != -1`
* `select NR, *` - enumerate records, NR is 1-based
* `select a1, b1, b2 inner join ./countries.txt on a2 == b1 order by a1, a3` - example of join query
* `select MAX(a1), MIN(a1) where a.Name != 'John' group by a2, a3` - example of aggregate query
* `select ...a1.split(':')` - Using JS "destructuring assignment" syntax to split one column into many. Do not try this with other SQL engines!


## RBQL design principles and architecture
RBQL core idea is based on dynamic code generation and execution with [exec](https://docs.python.org/3/library/functions.html#exec) and [eval](https://www.w3schools.com/jsref/jsref_eval.asp) functions.
Here are the main steps that RBQL engine performs when processing a query:
1. Shallow parsing: split the query into logical expressions such as "SELECT", "WHERE", "ORDER BY", etc.
2. Embed the expression segments into the main loop template code
3. Execute the hydrated loop code

Here you can find a very basic working script (only 15 lines of Python code) which implements this idea: [mini_rbql.py](https://github.com/mechatroner/mini-rbql/blob/master/mini_rbql.py)

The diagram below gives an overview of the main RBQL components and data flow:
![RBQL Diagram](https://i.imgur.com/KDQHoVM.png)


### Advantages of RBQL over traditional SQL engines
* Provides power and flexibility of general purpose Python and JS languages in relational expressions (including regexp, math, file system, json, xml, random and many other libraries that these languages provide)
* Can work with different data sources including CSV files, sqlite tables, native 2D arrays/lists (traditional SQL engines are usually tightly coupled with their databases)
* Supports both TOP and LIMIT keywords
* Provides additional NR (record number) variable which is especially useful for input sources where record order is well defined (such as CSV files)
* Supports tables with variable number of values in records
* Allows to generate result sets with variable number of values in records e.g. by using split() function and unpack operator (Python) / destructuring assignment (JS)
* UPDATE is a special case of SELECT query - this prevents accidental data loss
* Almost nonexistent entry barrier both for SQL users and JS/Python users
* No need to use FROM statement - the table name is defined by the context. This improves query typing speed and allows immediate autocomplete for variables inside SELECT statement (in traditional SQL engines autocomplete will not work until you write FROM statement, which goes after SELECT statement)
* SELECT, WHERE, ORDER BY, and other statements can be rearranged in any way you like
* Supports EXCEPT statement
* Provides a fully-functional client-side browser demo application
* Integration with popular text editors (VSCode, Vim, Sublime Text, Atom)
* Small, maintainable, dependency-free, eco-friendly and hackable code base: RBQL engine fits into a single file with less than 2000 LOC


### FAQ
#### How do I skip header record in CSV files?

You can use the following trick: add `... where NR > 1 ...` to your query  

And if you are doing math operation you can modify your query like this, example:  
`select int(a3) * 1000, a2` -> `select int(a3) * 1000 if NR > 1 else a3, a2`  


### References

* [RBQL: Official Site](https://rbql.org/)
* RBQL is integrated with Rainbow CSV extensions in [Vim](https://github.com/mechatroner/rainbow_csv), [VSCode](https://marketplace.visualstudio.com/items?itemName=mechatroner.rainbow-csv), [Sublime Text](https://packagecontrol.io/packages/rainbow_csv) and [Atom](https://atom.io/packages/rainbow-csv) editors.
* [RBQL in npm](https://www.npmjs.com/package/rbql): `$ npm install -g rbql`
* [RBQL in PyPI](https://pypi.org/project/rbql/): `$ pip install rbql`

RBQL is both a library and a command line tool which provides SQL-like language with JavaScript expressions

## Table of Contents
1. [RBQL as browser library](#using-rbql-as-browser-library)
2. [RBQL as Node library](#using-rbql-as-node-library)
3. [RBQL as command line tool](#using-rbql-as-command-line-tool)
4. [RBQL language description](#language-description)


# Using RBQL as a browser library

## Installation:
In order to make RBQL work in browser as a library for your App you need just one single file: `rbql.js`
To get it you can either use npm:  

```
$ npm install rbql
```
Now you can just source rbql.js and it will work:  
```
<script src="rbql.js"></script>
```

## API description

The following two functions are avilable in the browser version:  

1. [rbql.query_table(...)](#rbqlquery_table)  
2. [rbql.query(...)](#rbqlquery)  

### rbql.query_table(...)
Run user query against input array of records and put the result set in the output array:  

```
async function query_table(user_query, input_table, output_table, output_warnings, join_table=null, input_column_names=null, join_column_names=null, output_column_names=null, normalize_column_names=true)
```

#### Parameters:  

* _user_query_: **string**  
  query that user of your app manually enters in some kind of input field.  
* _input_table_: **array**  
  an array with input records  
* _output_table_: **array**  
  an array where to output records would be pushed  
* _output_warnings_: **array**  
  Warnings will be stored here after the query completion. If no warnings - the array would be empty
* _join_table_: **array**  
  an array with join table records so that user can use join table B in input queries  
* _input_column_names_: **array**  
  Names of _input_table_ columns which users of the app can use in their queries
* _join_column_names_: **array**  
  Names of _join_table_ columns which users of the app can use in their queries
* _output_column_names_: **array**  
  Output column names will be stored in this array after the query completion.
* _normalize_column_names_: **boolean**  
  If set to true - column names provided with _input_column_names_ and _join_column_names_ will be normalized to "a" and "b" prefix forms e.g. "Age" -> "a.Age", "Sale price" -> "b['Sale price']".  
  If set to false - column names can be used in user queries "as is".  


### rbql.query(...)
Allows to run queries against any kind of structured data.  
You will have to implement special wrapper classes for your custom data structures and pass them to the `rbql.query(...)` function.  

```
async function query(user_query, input_iterator, output_writer, output_warnings, join_tables_registry=null)
```

#### Parameters:  

* _user_query_: **string**  
  query that user of your app manually enters in some kind of input field.  
* _input_iterator_:  **RBQLInputIterator**  
  special object which iterates over input records. E.g. over a remote table 
  Examples of classes which support **RBQLInputIterator** interface: **TableIterator**, **CSVRecordIterator** (these classes can be found in RBQL source code)
* _output_writer_:  **RBQLOutputWriter**  
  special object which stores output records somewhere. E.g. to an array  
  Examples of classes which support **RBQLOutputWriter** interface: **TableWriter**, **CSVWriter** (these classes can be found in RBQL source code)
* _output_warnings_: **array**  
  Warnings will be stored here after the query completion. If no warnings - the array would be empty
* _join_tables_registry_: **RBQLJoinTableRegistry**  
  special object which provides **RBQLInputIterator** iterators for join tables (e.g. table "B") which user can refer to in their queries.  
  Examples of classes which support **RBQLJoinTableRegistry** interface: **SingleTableRegistry**, **FileSystemCSVRegistry** (these classes can be found in RBQL source code)


## Usage:

#### "Hello world" web test in RBQL  
Very simple test to make sure that RBQL library works:  

```
<!DOCTYPE html>
<html><head>
<script src="../../rbql-js/rbql.js"></script>
<script>
    let output_table = [];
    let warnings = [];
    let error_handler = function(exception) {
        console.log('RBQL finished with error: ' + String(exception));
    }
    let success_handler = function() {
        console.log('warnings: ' + JSON.stringify(warnings));
        console.log('output table: ' + JSON.stringify(output_table));
    }
    rbql.query_table('select a2 + " test", a1 limit 2', [[1, 'foo'], [2, 'bar'], [3, 'hello']], output_table, warnings).then(success_handler).catch(error_handler);
</script>
<title>RBQL Generic Test</title>
</head><body>
<div><span>Open browser console</span></div>
</body></html>
```

Save the code above as `rbql_test.html`; put `rbql.js` in the same folder; open `rbql_test.html` in your browser and make sure that console output contains the expected result.  


#### "JSFiddle" demo test  
A little more advanced, but still very simple demo test with [JSFiddle](https://jsfiddle.net/mechatroner/kpuwc83x/)
It uses the same `rbql.js` script file.


# Using RBQL as Node library

## Installation:
```
$ npm install rbql
```

## API description

The following 3 functions are avilable in Node version:  

1. [rbql.query_csv(...)](#rbqlquery_csv)  
2. [rbql.query_table(...)](#rbqlquery_table) - identical to browser version
3. [rbql.query(...)](#rbqlquery) - identical to browser version


### rbql.query_csv(...)

Run user query against input_path CSV file and save it as output_path CSV file.  

```
async function rbql.query_csv(user_query, input_path, input_delim, input_policy, output_path, output_delim, output_policy, csv_encoding, output_warnings, with_headers=false, comment_prefix=null)
```

#### Parameters:
* _user_query_: **string**  
  query that user of your application manually enters in some kind of input field.  
* _input_path_: **string**  
  path of the input csv table  
* _input_delim_: **string**  
  field separator character in input table  
* _input_policy_: **string**  
  allowed values: `'simple'`, `'quoted'`  
  along with input_delim defines CSV dialect of input table. "quoted" means that separator can be escaped inside double quoted fields  
* _output_path_: **string**  
  path of the output csv table  
* _output_delim_: **string**  
  same as input_delim but for output table  
* _output_policy_: **string**  
  same as input_policy but for output table  
* _csv_encoding_: **string**  
  allowed values: `'binary'`, `'utf-8'`  
  encoding of input, output and join tables (join table can be defined inside the user query)  
* _output_warnings_: **array**  
  Warnings will be stored here after the query completion. If no warnings - the array would be empty
* _with_headers_: **boolean**  
  If set to `true` treat the first records in input (and join) file as header.
* _comment_prefix_: **string**  
  Treat lines starting with the prefix as comments and skip them.


## Usage:

#### Example of query_table() usage:  
```
const rbql = require('rbql')
let input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
];
let user_query = 'SELECT a1, a2 % 1000 WHERE a3 != "USA" LIMIT 3';
let output_table = [];
let warnings = [];
let error_handler = function(exception) {
    console.log('Error: ' + String(exception));
}
let success_handler = function() {
    console.log('warnings: ' + JSON.stringify(warnings));
    console.log('output table: ' + JSON.stringify(output_table));
}
rbql.query_table(user_query, input_table, output_table, warnings).then(success_handler).catch(error_handler);
```

#### Example of query_csv() usage:  
```
const rbql = require('rbql');
let user_query = 'SELECT a1, parseInt(a2) % 1000 WHERE a3 != "USA" LIMIT 5';
let error_handler = function(exception) {
    console.log('Error: ' + String(exception));
}
let warnings = [];
let success_handler = function() {
    if (warnings.length)
        console.log('warnings: ' + JSON.stringify(warnings));
    console.log('output table: output.csv');
}
rbql.query_csv(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8', warnings).then(success_handler).catch(error_handler);
```


You can also check rbql-js cli app code as a usage example: [rbql-js cli source code](https://github.com/mechatroner/RBQL/blob/master/rbql-js/cli_rbql.js)  



# Using RBQL as command line tool


### Installation:
To use RBQL as CLI app you can install it in global (-g) mode:  
```
$ npm install -g rbql
```

RBQL can also be installed locally with `$ npm install rbql`, but then you would have to run it with `$ npx rbql-js ...` instead of `$ rbql-js ...`.


### Usage (non-interactive mode):

```
$ rbql-js --query "select a1, a2 order by a1" < input.tsv
```

### Usage (interactive mode):
In interactive mode rbql-js will show input table preview so it is easier to type SQL-like query.  
```
$ rbql-js --input input.csv --output result.csv
```


# Language description

### Main Features

* Use JavaScript expressions inside _SELECT_, _UPDATE_, _WHERE_ and _ORDER BY_ statements
* Supports multiple input formats
* Result set of any query immediately becomes a first-class table on its own
* No need to provide FROM statement in the query when the input table is defined by the current context.
* Supports all main SQL keywords
* Supports aggregate functions and GROUP BY queries
* Supports user-defined functions (UDF)
* Provides some new useful query modes which traditional SQL engines do not have
* Lightweight, dependency-free, works out of the box

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
* AS

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
   Description: Value of the field referenced by it's "name". You can use this notation if the field in the header has a "good" alphanumeric name  
* _a["object id"]_, _a['9.12341234']_, _b["%$ !! 10 20"]_ ... _a["Arbitrary column name!"]_  
   Variable type: **string**  
   Description: Value of the field referenced by it's "name". You can use this notation to reference fields by arbitrary values in the header


### UPDATE statement

_UPDATE_ query produces a new table where original values are replaced according to the UPDATE expression, so it can also be considered a special type of SELECT query.

### Aggregate functions and queries

RBQL supports the following aggregate functions, which can also be used with _GROUP BY_ keyword:  
_COUNT_, _ARRAY_AGG_, _MIN_, _MAX_, _ANY_VALUE_, _SUM_, _AVG_, _VARIANCE_, _MEDIAN_  

Limitation: aggregate functions inside JavaScript expressions are not supported. Although you can use expressions inside aggregate functions.  
E.g. `MAX(float(a1) / 1000)` - valid; `MAX(a1) / 1000` - invalid.  
There is a workaround for the limitation above for _ARRAY_AGG_ function which supports an optional parameter - a callback function that can do something with the aggregated array. Example:  
`SELECT a2, ARRAY_AGG(a1, v => v.sort().slice(0, 5)) GROUP BY a2`


### JOIN statements

Join table B can be referenced either by its file path or by its name - an arbitrary string which the user should provide before executing the JOIN query.  
RBQL supports _STRICT LEFT JOIN_ which is like _LEFT JOIN_, but generates an error if any key in the left table "A" doesn't have exactly one matching key in the right table "B".  
Table B path can be either relative to the working dir, relative to the main table or absolute.  
Limitation: _JOIN_ statements can't contain JavaScript expressions and must have the following form: _<JOIN\_KEYWORD> (/path/to/table.tsv | table_name ) ON a... == b... [AND a... == b... [AND ... ]]_

### SELECT EXCEPT statement

SELECT EXCEPT can be used to select everything except specific columns. E.g. to select everything but columns 2 and 4, run: `SELECT * EXCEPT a2, a4`  
Traditional SQL engines do not support this query mode.


### UNNEST() operator
UNNEST(list) takes a list/array as an argument and repeats the output record multiple times - one time for each value from the list argument.  
Example: `SELECT a1, UNNEST(a2.split(';'))`  


### LIKE() function
RBQL does not support LIKE operator, instead it provides "like()" function which can be used like this:
`SELECT * where like(a1, 'foo%bar')`


### WITH (header) and WITH (noheader) statements
You can set whether the input (and join) CSV file has a header or not using the environment configuration parameters which could be `--with_headers` CLI flag or GUI checkbox or something else.
But it is also possible to override this selection directly in the query by adding either `WITH (header)` or `WITH (noheader)` statement at the end of the query.
Example: `select top 5 NR, * with (header)`


### Pipe syntax for query chaining
You can chain consecutive queries via pipe `|` syntax. Example:
```
SELECT a2 AS region, count(*) AS cnt GROUP BY a2 | SELECT * ORDER BY a.cnt DESC
```


### User Defined Functions (UDF)

RBQL supports User Defined Functions  
You can define custom functions and/or import libraries in a special file: `~/.rbql_init_source.js`


## Examples of RBQL queries

* `SELECT TOP 100 a1, a2 * 10, a4.length WHERE a1 == "Buy" ORDER BY parseInt(a2) DESC`
* `SELECT a.id, a.weight / 1000 AS weight_kg`
* `SELECT * ORDER BY Math.random()` - random sort
* `SELECT TOP 20 a.vehicle_price.length / 10, a2 WHERE parseInt(a.vehicle_price) < 500 && ["car", "plane", "boat"].indexOf(a['Vehicle type']) > -1 limit 20` - referencing columns by names from header
* `UPDATE SET a3 = 'NPC' WHERE a3.indexOf('Non-playable character') != -1`
* `SELECT NR, *` - enumerate records, NR is 1-based
* `SELECT a1, b1, b2 INNER JOIN ./countries.txt ON a2 == b1 ORDER BY a1, a3` - example of join query
* `SELECT MAX(a1), MIN(a1) WHERE a.Name != 'John' GROUP BY a2, a3` - example of aggregate query
* `SELECT ...a1.split(':')` - Using JS "destructuring assignment" syntax to split one column into many. Do not try this with other SQL engines!


### References

* [RBQL: Official Site](https://rbql.org/)
RBQL is integrated with Rainbow CSV extensions in [Vim](https://github.com/mechatroner/rainbow_csv), [VSCode](https://marketplace.visualstudio.com/items?itemName=mechatroner.rainbow-csv), [Sublime Text](https://packagecontrol.io/packages/rainbow_csv) editors.
* [RBQL in PyPI](https://pypi.org/project/rbql/): `$ pip install rbql`
* Rainbow CSV extension with integrated RBQL in [Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=mechatroner.rainbow-csv)  
* Rainbow CSV extension with integrated RBQL in [Vim](https://github.com/mechatroner/rainbow_csv)  
* Rainbow CSV extension with integrated RBQL in [Sublime Text 3](https://packagecontrol.io/packages/rainbow_csv)  

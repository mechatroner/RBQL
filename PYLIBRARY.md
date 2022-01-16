# Using RBQL as python library

rbql module provides a set of query functions and an IPython "magic" command.


#### Installation:
```
$ pip install rbql
```

## API

rbql library provides 4 main functions that you can use:  

1. [rbql.query_table(...)](#rbqlquery_table)  
2. [rbql.query_csv(...)](#rbqlquery_csv)  
3. [rbql.query_pandas_dataframe(...)](#rbqlquery_pandas_dataframe)  
4. [rbql.query(...)](#rbqlquery)  


### rbql.query_table(...)

Run user query against a list of records and put the result set in the output list.  

#### Signature:  
  
`rbql.query_table(user_query, input_table, output_table, output_warnings, join_table=None, input_column_names=None, join_column_names=None, output_column_names=None, normalize_column_names=True)`


#### Parameters: 
* _user_query_: **string**  
  Query that user of your app manually enters in some kind of input field  
* _input_table_: **list**  
  List with input records  
* _output_table_: **list**  
  Output records will be stored here after the query completion
* _output_warnings_: **list**  
  Warnings will be stored here after the query completion. If no warnings - the list would be empty
* _join_table_: **list**  
  List with join table so that user can use join table B in input queries  
* _input_column_names_: **list**  
  Names of _input_table_ columns which users of the app can use in their queries
* _join_column_names_: **list**  
  Names of _join_table_ columns which users of the app can use in their queries
* _output_column_names_: **list**  
  Empty list or None: Output column names will be stored in this list after the query completion.
* _normalize_column_names_: **boolean**  
  If set to True - column names provided with _input_column_names_ and _join_column_names_ will be normalized to "a" and "b" prefix forms e.g. "Age" -> "a.Age", "Sale price" -> "b['Sale price']".  
  If set to False - column names can be used in user queries "as is".  


#### Usage example:
```
import rbql
input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
]
user_query = 'SELECT a1, a2 % 1000 WHERE a3 != "USA" LIMIT 3'
output_table = []
warnings = []
rbql.query_table(user_query, input_table, output_table, warnings)
for record in output_table:
    print(','.join([str(v) for v in record]))
```



### rbql.query_csv(...)

Run user query against input_path CSV file and save it as output_path CSV file.  

#### Signature:  
  
`rbql.query_csv(user_query, input_path, input_delim, input_policy, output_path, output_delim, output_policy, csv_encoding, output_warnings, with_headers, comment_prefix=None)`  
  
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
  allowed values: `'latin-1'`, `'utf-8'`  
  encoding of input, output and join tables (join table can be defined inside the user query)  
* _output_warnings_: **list**  
  warnings will be stored here after the query completion. If no warnings - the list would be empty
* _with_headers_: **boolean**  
  if set to `true` treat the first record in input (and join) file as header.
* _comment_prefix_: **string**  
  ignore lines in input and join tables that start with the comment prefix, e.g. "#" or ">>"

#### Usage example

```
import rbql
user_query = 'SELECT a1, int(a2) % 1000 WHERE a3 != "USA" LIMIT 5'
warnings = []
rbql.query_csv(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8', warnings)
print(open('output.csv').read())
```


### rbql.query_pandas_dataframe(...)

Run user query against pandas dataframe and return a new dataframe with the result.  

#### Signature:  
  
`rbql.query_pandas_dataframe(user_query, input_dataframe, output_warnings=None, join_dataframe=None, normalize_column_names=True)`  
  
#### Parameters:
* _user_query_: **string**  
  query that user of your application manually enters in some kind of input field.  
* _input_dataframe_: **pandas.DataFrame**  
  input dataframe
* _output_warnings_: **list**  
  warnings will be stored here after the query completion. If no warnings - the list would be empty
* _join_dataframe_: **pandas.DataFrame**  
  dataframe with join table
* _normalize_column_names_: **boolean**  
  If set to True - column names provided with _input_column_names_ and _join_column_names_ will be normalized to "a" and "b" prefix forms e.g. "Age" -> "a.Age", "Sale price" -> "b['Sale price']".  
  If set to False - column names can be used in user queries "as is".  

#### Usage example

```
import pandas
import rbql
input_dataframe = pandas.DataFrame([
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
], columns=['name', 'year', 'country'])
user_query = 'SELECT a.name, a.year % 1000 WHERE a.country != "France" LIMIT 3'
result_dataframe = rbql.query_pandas_dataframe(user_query, input_dataframe)
print(result_dataframe)
```

### rbql.query(...)

Allows to run queries against any kind of structured data.  
You will have to implement special wrapper classes for your custom data structures and pass them to the `rbql.query(...)` function.  

#### Signature:
  
`query(user_query, input_iterator, output_writer, output_warnings, join_tables_registry=None)`  
  
#### Parameters:
* _user_query_: **string**  
  query that user of your app manually enters in some kind of input field.  
* _input_iterator_:  **RBQLInputIterator**  
  special object which iterates over input records. E.g. over remote table  
* _output_writer_:  **RBQLOutputWriter**  
  special object which stores output records somewhere. E.g. to a python list  
* _output_warnings_: **list**  
  warnings will be stored here after the query completion. If no warnings - the list would be empty
* _join_tables_registry_: **RBQLTableRegistry**  
  special object which provides **RBQLInputIterator** iterators for join tables (e.g. table "B") which users can refer to in their queries.  


#### Usage example
See `rbql.query(...)` usage in RBQL [tests](https://github.com/mechatroner/RBQL/blob/master/test/test_rbql.py)  


## IPython/Jupyter "%rbql" magic command
The rbql module also provide `%rbql` "magic" command which can be used to query pandas dataframes inside IPython/Jupyter notebooks.
#### Syntax
`%rbql <query>`
#### Usage example
```
from vega_datasets import data
my_cars = data.cars()
%load_ext rbql
result_df = %rbql SELECT a.Name, a.Weight_in_lbs / 1000, a.Horsepower FROM my_cars WHERE a.Horsepower > 100 ORDER BY a.Weight_in_lbs DESC LIMIT 15
result_df
```
You can run this and other queries iniside the [demo Google Colab notebook](https://colab.research.google.com/drive/1_cFPtnQUxILP0RE2_DBlqIfXaEzT-oZ6?usp=sharing)

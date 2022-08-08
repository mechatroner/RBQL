# TODO

## Generic TODO

* We can do good-enough header autodetection in CSV files to show warnings when we have a high degree of confidence that the file has header but user didn't skip it and vise versa

* Catch exceptions in user expression to report the exact place where it occured: "SELECT" expression, "WHERE" expression, etc

* Consider supporting explicit column names variables like "host" or "name" or "surname" - just parse all variable-looking sequences from the query and match them against available column names from the header, but skip all symbol defined in rbql_engine.py/rbql.js, user init code and python/js builtin keywords (show warning on intersection)

* Gracefuly handle unknown encoding: generate RbqlIOHandlingError

* Show warning when csv fields contain trailing spaces, at least in join mode

* Support custom (virtual) headers for CSV version

* Allow to use NL variable in RBQL queries for CSV version

* Add "inconsistent number of fields in output table" warning. Useful for queries like this: `*a1.split("|")` or `...a1.split("|")`, where num of fields in a1 is variable

* Add RBQL iterators for json lines ( https://jsonlines.org/ ) and xml-by-line files

* Add RBQL file-system iterator to be able to query files like fselect does

* Support 'AS' keyword

* Consider disabling a1, a2 etc variables when header is enabled. This is to make sure that the user knows what query mode they are in.

* Make debug_mode local variable

* Check ambiguous column names internally inside rbql_engine after resolving input_iterator and join_iterator, get rid of external checks.

* Create reasonable API for converting table_ids (from query text, both join and main) to table_uri for external usage e.g. to fetch column names for autocomplete.

* Setup test coverage framework and metrics.

* Support ASC/DESC in the middler of ORDER BY statements, see #35

## Python TODO

* Use ast module to improve parsing of parse_attribute_variables / parse_dictionary_variables, like it was done for select parsing

* Optimize performance: optional compilation depending on python2/python3

* Optimize performance: replace `smart_split()` with `polymorphic_split()` in rbql_csv.py

## IPython TODO

* Figure out how to color SQL keywords such as "SELECT", "WHERE", etc for ipython

## JavaScript TODO

* Get rid of TopWriter for non-top/limit queries in JavaScript version like it was done for Python.

* Find a way to make query_context local in JavaScript version like it was done for Python.

## Upstream TODO

Enable better error reporting in V8 engine eval() function, see https://bugs.chromium.org/p/v8/issues/detail?id=2589

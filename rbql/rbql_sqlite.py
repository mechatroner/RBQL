# -*- coding: utf-8 -*-

# This module allows to query sqlite databases using RBQL

from __future__ import unicode_literals
from __future__ import print_function

# FIXME test table with NULL values
# FIXME test db with a single table


import re
from . import rbql_engine


class RbqlIOHandlingError(Exception):
    pass


class SqliteRecordIterator:
    def __init__(self, db_connection, table_name, variable_prefix='a'): # FIXME consider adding support for multiple variable_prefixes i.e. "a" and <table_name>
        self.db_connection = db_connection
        self.table_name = table_name
        self.variable_prefix = variable_prefix
        self.cursor = self.db_connection.cursor()
        if re.match('^[a-zA-Z0-9_]*$', table_name) is None:
            raise RbqlIOHandlingError('Unable to use "{}": input table name can contain only alphanumeric characters and underscore'.format(table_name))
        self.cursor.execute('SELECT * FROM {}'.format(table_name))

    def get_column_names(self):
        column_names = [description[0] for description in self.cursor.description]
        return column_names

    def get_variables_map(self, query_text):
        variable_map = dict()
        rbql_engine.parse_basic_variables(query_text, self.variable_prefix, variable_map)
        rbql_engine.parse_array_variables(query_text, self.variable_prefix, variable_map)
        rbql_engine.map_variables_directly(query_text, self.get_column_names(), variable_map) # FIXME add flag to avoid throwing an exception when one of the variable_map entries can't be used as a variable. Just make sure it is not in the query and skip it.
        return variable_map

    def get_record(self):
        record_tuple = self.cursor.fetchone()
        if record_tuple is None:
            return None
        # We need to convert tuple to list here because otherwise we won't be able to concatinate lists in expressions with star `*` operator
        return list(record_tuple)

    def get_all_records(self, num_rows=None):
        # TODO consider to use TOP in the sqlite query when num_rows is not None
        if num_rows is None:
            return self.cursor.fetchall()
        result = []
        for i in range(num_rows):
            row = self.cursor.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def get_warnings(self):
        return []


class SqliteDbRegistry:
    def __init__(self, db_connection):
        self.db_connection = db_connection

    def get_iterator_by_table_id(self, table_id):
        self.record_iterator = SqliteRecordIterator(db_connection, table_id, 'b')
        return self.record_iterator

    def finish(self, output_warnings):
        pass



# -*- coding: utf-8 -*-

# This module allows to query sqlite databases using RBQL

from __future__ import unicode_literals
from __future__ import print_function


from . import rbql_engine


class SqliteRecordIterator:
    def __init__(self, db_path, table_name, variable_prefix='a'): # FIXME consider adding support for multiple variable_prefixes i.e. "a" and <table_name>
        self.db_path = db_path
        self.table_name = table_name
        import sqlite3
        self.connection = sqlite3.connect(db_path)
        self.cursor = self.connection.cursor()
        self.cursor.execute('SELECT * FROM {}'.format(table_name)) #FIXME sanitize table_name


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
        return self.cursor.fetchone()


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

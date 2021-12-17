# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

from . import rbql_engine


class DataframeIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, table, column_names=None, normalize_column_names=True, variable_prefix='a'):
        self.table = table
        self.normalize_column_names = normalize_column_names
        self.variable_prefix = variable_prefix
        self.NR = 0
        # TODO include `Index` into the list of addressable variable names.
        self.column_names = list(table.columns)
        self.table_itertuples = self.table.itertuples(index=False)

    def get_variables_map(self, query_text):
        variable_map = dict()
        parse_basic_variables(query_text, self.variable_prefix, variable_map)
        parse_array_variables(query_text, self.variable_prefix, variable_map)
        if self.normalize_column_names:
            parse_dictionary_variables(query_text, self.variable_prefix, self.column_names, variable_map)
            parse_attribute_variables(query_text, self.variable_prefix, self.column_names, 'column names list', variable_map)
        else:
            map_variables_directly(query_text, self.column_names, variable_map)
        return variable_map

    def get_record(self):
        try:
            record = next(self.table_itertuples)
        except StopIteration:
            return None
        self.NR += 1
        return record

    def get_warnings(self):
        return []

    def get_header(self):
        return self.column_names


class DataframeWriter(rbql_engine.RBQLOutputWriter):
    def __init__(self, external_table):
        self.table = external_table
        self.header = None

    def write(self, fields):
        self.table.append(fields)
        return True

    def set_header(self, header):
        self.header = header


class SingleDataframeRegistry(rbql_engine.RBQLTableRegistry):
    def __init__(self, table, column_names=None, normalize_column_names=True, table_name='b'):
        self.table = table
        self.column_names = column_names
        self.normalize_column_names = normalize_column_names
        self.table_name = table_name

    def get_iterator_by_table_id(self, table_id):
        if table_id.lower() != self.table_name:
            raise RbqlParsingError('Unable to find join table: "{}"'.format(table_id)) # UT JSON
        return DataframeIterator(self.table, self.column_names, self.normalize_column_names, 'b')


def query_table(query_text, input_table, output_table, output_warnings, join_table=None, input_column_names=None, join_column_names=None, output_column_names=None, normalize_column_names=True, user_init_code=''):
    if not normalize_column_names and input_column_names is not None and join_column_names is not None:
        ensure_no_ambiguous_variables(query_text, input_column_names, join_column_names)
    input_iterator = DataframeIterator(input_table, input_column_names, normalize_column_names)
    output_writer = DataframeWriter(output_table)
    join_tables_registry = None if join_table is None else SingleDataframeRegistry(join_table, join_column_names, normalize_column_names)
    query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code=user_init_code)
    if output_column_names is not None:
        assert len(output_column_names) == 0, '`output_column_names` param must be an empty list or None'
        if output_writer.header is not None:
            for column_name in output_writer.header:
                output_column_names.append(column_name)


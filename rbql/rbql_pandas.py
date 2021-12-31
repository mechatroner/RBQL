# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

from . import rbql_engine


def get_dataframe_column_names_for_rbql(dataframe):
    import pandas
    if isinstance(dataframe.columns, pandas.RangeIndex) or not len(dataframe.columns):
        return None
    return [str(v) for v in list(dataframe.columns)]


class DataframeIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, table, normalize_column_names=True, variable_prefix='a'):
        self.table = table
        self.normalize_column_names = normalize_column_names
        self.variable_prefix = variable_prefix
        self.NR = 0
        # TODO include `Index` into the list of addressable variable names.
        self.column_names = get_dataframe_column_names_for_rbql(table)
        self.table_itertuples = self.table.itertuples(index=False)

    def get_variables_map(self, query_text):
        variable_map = dict()
        rbql_engine.parse_basic_variables(query_text, self.variable_prefix, variable_map)
        rbql_engine.parse_array_variables(query_text, self.variable_prefix, variable_map)
        if self.column_names is not None:
            if self.normalize_column_names:
                rbql_engine.parse_dictionary_variables(query_text, self.variable_prefix, self.column_names, variable_map)
                rbql_engine.parse_attribute_variables(query_text, self.variable_prefix, self.column_names, 'column names list', variable_map)
            else:
                rbql_engine.map_variables_directly(query_text, self.column_names, variable_map)
        return variable_map

    def get_record(self):
        try:
            record = next(self.table_itertuples)
        except StopIteration:
            return None
        self.NR += 1
        # Convert to list because `record` has `Pandas` type.
        return list(record)

    def get_warnings(self):
        return []

    def get_header(self):
        return self.column_names


class DataframeWriter(rbql_engine.RBQLOutputWriter):
    def __init__(self):
        self.header = None
        self.output_rows = []
        self.result = None

    def write(self, fields):
        self.output_rows.append(fields)
        return True

    def set_header(self, header):
        self.header = header

    def finish(self):
        import pandas as pd
        self.result = pd.DataFrame(self.output_rows, columns=self.header)


class SingleDataframeRegistry(rbql_engine.RBQLTableRegistry):
    def __init__(self, table, normalize_column_names=True, table_name='b'):
        self.table = table
        self.normalize_column_names = normalize_column_names
        self.table_name = table_name

    def get_iterator_by_table_id(self, table_id):
        if table_id.lower() != self.table_name:
            raise rbql_engine.RbqlParsingError('Unable to find join table: "{}"'.format(table_id))
        return DataframeIterator(self.table, self.normalize_column_names, 'b')


def query_dataframe(query_text, input_table, output_warnings, join_table=None, normalize_column_names=True, user_init_code=''):
    if not normalize_column_names and join_table is not None:
        input_columns = get_dataframe_column_names_for_rbql(input_table)
        join_columns = get_dataframe_column_names_for_rbql(join_table)
        if input_columns is not None and join_columns is not None:
            rbql_engine.ensure_no_ambiguous_variables(query_text, input_columns, join_columns)
    input_iterator = DataframeIterator(input_table, normalize_column_names)
    output_writer = DataframeWriter()
    join_tables_registry = None if join_table is None else SingleDataframeRegistry(join_table, normalize_column_names)
    rbql_engine.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code=user_init_code)
    return output_writer.result

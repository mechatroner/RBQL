# -*- coding: utf-8 -*-


# This module is to support JSON Lines format: https://jsonlines.org/


from __future__ import unicode_literals
from __future__ import print_function

import re
import os
import sys
import json

from . import rbql_engine
from . import rbql_csv
from . import csv_utils


# Json input doesn't need to support json join because star * operator wouldn't make any sense in this case, 
# the only way to concatenate JSONs is to put both of them inside a 2-element array, like this [json_1, json_2], but this is not practical
# Another problem is with reading JOIN table - we would have to adjust variable -> index mapping code and instead of index use attribute, because JSON doesn't have column indices unlike CSV.


class JSONLinesRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, comment_prefix=None, table_name='input', variable_prefix='a'):
        # We need a text stream not bytes because Python3.5 requires json.loads() argument to be string (bytes is supported only starting from Python3.6)
        self.stream = rbql_csv.encode_input_stream(stream, 'utf-8')
        self.table_name = table_name
        self.variable_prefix = variable_prefix
        self.comment_prefix = comment_prefix if (comment_prefix is not None and len(comment_prefix)) else None

        self.exhausted = False
        self.NR = 0 # Record number
        self.NL = 0 # Line number (NL != NR when the JSON Lines file has comments)


    def get_variables_map(self, query_text):
        # FIXME
        variable_map = dict()
        return variable_map


    def get_record(self):
        while True:
            line = self.stream.readline()
            if not line:
                return None
            if self.comment_prefix is None or not line.startswith(self.comment_prefix):
                break
        self.NR += 1
        record = json.loads(line)
        return record


    def get_all_records(self, num_rows=None):
        result = []
        while True:
            record = self.get_record()
            if record is None:
                break
            result.append(record)
            if num_rows is not None and len(result) >= num_rows:
                break
        return result


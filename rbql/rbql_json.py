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


# FIXME consider using python built-in readline function instead


class JSONLinesRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, comment_prefix=None, table_name='input', variable_prefix='a'):
        # We need a text stream not bytes because Python3.5 requires json.loads() argument to be string (bytes is supported only starting from Python3.6)
        self.stream = rbql_csv.encode_input_stream(stream, 'utf-8')
        self.table_name = table_name
        self.variable_prefix = variable_prefix
        self.comment_prefix = comment_prefix if (comment_prefix is not None and len(comment_prefix)) else None

        self.buffer = ''
        self.detected_line_separator = '\n'
        self.exhausted = False
        self.NR = 0 # Record number
        self.NL = 0 # Line number (NL != NR when the JSON Lines file has comments)
        self.chunk_size = chunk_size
        self.fields_info = dict()


    def get_variables_map(self, query_text):
        # FIXME
        variable_map = dict()
        return variable_map


    def _get_row_from_buffer(self):
        str_before, separator, str_after = csv_utils.extract_line_from_data(self.buffer)
        if separator is None:
            return None
        if separator == '\r' and str_after == '':
            one_more = self.stream.read(1)
            if one_more == '\n':
                separator = '\r\n'
            else:
                str_after = one_more
        self.detected_line_separator = separator
        self.buffer = str_after
        return str_before


    def _read_until_found(self):
        if self.exhausted:
            return
        chunks = []
        while True:
            chunk = self.stream.read(self.chunk_size)
            if not chunk:
                self.exhausted = True
                break
            chunks.append(chunk)
            if csv_utils.newline_rgx.search(chunk) is not None:
                break
        self.buffer += ''.join(chunks)


    def get_row_simple(self):
        try:
            row = self._get_row_from_buffer()
            if row is None:
                self._read_until_found()
                row = self._get_row_from_buffer()
                if row is None:
                    assert self.exhausted
                    if not len(self.buffer):
                        return None
                    row = self.buffer
                    self.buffer = ''
            self.NL += 1
            return row
        except UnicodeDecodeError:
            raise rbql_engine.RbqlIOHandlingError('Unable to decode input data as UTF-8')

    
    def get_record(self):
        while True:
            line = self.get_row_simple()
            if line is None:
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


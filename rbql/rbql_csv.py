#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import re
import importlib
import codecs
import tempfile
import random
import shutil
import time
from collections import defaultdict

from . import engine
from . import csv_utils


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def csv_run(query, input_stream, input_delim, input_policy, output_stream, output_delim, output_policy, csv_encoding, custom_init_path=None, convert_only_dst=None):
    try:
        if input_delim == '"' and input_policy == 'quoted':
            raise csv_utils.CSVHandlingError('Double quote delimiter is incompatible with "quoted" policy')
        if input_delim != ' ' and input_policy == 'whitespace':
            raise csv_utils.CSVHandlingError('Only whitespace " " delim is supported with "whitespace" policy')

        if not is_ascii(query) and csv_encoding == 'latin-1':
            # FIXME add unit test
            raise csv_utils.CSVHandlingError('To use non-ascii characters in query enable UTF-8 encoding instead of latin-1/binary')

        user_init_code = ''
        default_init_source_path = os.path.join(os.path.expanduser('~'), '.rbql_init_source.py')
        if custom_init_path is not None:
            user_init_code = engine.read_user_init_code(custom_init_path)
        elif os.path.exists(default_init_source_path):
            user_init_code = engine.read_user_init_code(default_init_source_path)

        join_tables_registry = csv_utils.FileSystemCSVRegistry(input_delim, input_policy, csv_encoding)
        input_iterator = csv_utils.CSVRecordIterator(input_stream, csv_encoding, input_delim, input_policy)
        output_writer = csv_utils.CSVWriter(output_stream, csv_encoding, output_delim, output_policy)
        error_info, warnings = engine.generic_run(query, input_iterator, output_writer, join_tables_registry, user_init_code, convert_only_dst)
        join_tables_registry.finish()
        return (error_info, warnings)
    except Exception as e:
        error_info = engine.exception_to_error_info(e)
        return (error_info, [])



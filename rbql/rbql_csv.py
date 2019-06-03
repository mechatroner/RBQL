#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import re
import importlib
import tempfile
import random
import shutil
import time
from collections import defaultdict

from . import engine
from . import csv_utils


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def read_user_init_code(rbql_init_source_path):
    with open(rbql_init_source_path) as src:
        return src.read()


def csv_run(query, input_path, input_delim, input_policy, output_path, output_delim, output_policy, csv_encoding, custom_init_path=None, convert_only_dst=None):
    output_stream, close_output_on_finish = (None, False)
    input_stream, close_input_on_finish = (None, False)
    try:
        output_stream, close_output_on_finish = (sys.stdout, False) if output_path is None else (open(output_path, 'wb'), True)
        input_stream, close_input_on_finish = (sys.stdin, False) if input_path is None else (open(input_path, 'rb'), True)

        if input_delim == '"' and input_policy == 'quoted':
            raise csv_utils.RbqlIOHandlingError('Double quote delimiter is incompatible with "quoted" policy')
        if input_delim != ' ' and input_policy == 'whitespace':
            raise csv_utils.RbqlIOHandlingError('Only whitespace " " delim is supported with "whitespace" policy')

        if not is_ascii(query) and csv_encoding == 'latin-1':
            raise csv_utils.RbqlIOHandlingError('To use non-ascii characters in query enable UTF-8 encoding instead of latin-1/binary')

        user_init_code = ''
        default_init_source_path = os.path.join(os.path.expanduser('~'), '.rbql_init_source.py')
        if custom_init_path is not None:
            user_init_code = read_user_init_code(custom_init_path)
        elif os.path.exists(default_init_source_path):
            user_init_code = read_user_init_code(default_init_source_path)

        join_tables_registry = csv_utils.FileSystemCSVRegistry(input_delim, input_policy, csv_encoding)
        input_iterator = csv_utils.CSVRecordIterator(input_stream, close_input_on_finish, csv_encoding, input_delim, input_policy)
        output_writer = csv_utils.CSVWriter(output_stream, close_output_on_finish, csv_encoding, output_delim, output_policy)
        error_info, warnings = engine.generic_run(query, input_iterator, output_writer, join_tables_registry, user_init_code, convert_only_dst)
        join_tables_registry.finish()
        return (error_info, warnings)
    except Exception as e:
        error_info = engine.exception_to_error_info(e)
        return (error_info, [])
    finally:
        if close_input_on_finish:
            input_stream.close()
        if close_output_on_finish:
            output_stream.close()



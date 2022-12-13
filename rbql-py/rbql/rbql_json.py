# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import codecs
import io
import json
from errno import EPIPE

from . import rbql_engine
from . import csv_utils


PY3 = sys.version_info[0] == 3
polymorphic_xrange = range if PY3 else xrange

debug_mode = False

try:
    broken_pipe_exception = BrokenPipeError
except NameError: # Python 2
    broken_pipe_exception = IOError


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def read_user_init_code(rbql_init_source_path):
    with open(rbql_init_source_path) as src:
        return src.read()


def encode_input_stream(stream, encoding):
    if encoding is None:
        return stream
    if PY3:
        # Reference: https://stackoverflow.com/a/16549381/2898283
        # typical stream (e.g. sys.stdin) in Python 3 is actually a io.TextIOWrapper but with some unknown encoding
        try:
            return io.TextIOWrapper(stream.buffer, encoding=encoding)
        except AttributeError:
            # BytesIO doesn't have "buffer"
            return io.TextIOWrapper(stream, encoding=encoding)
    else:
        # Reference: https://stackoverflow.com/a/27425797/2898283
        # Python 2 streams don't have stream.buffer and therefore we can't use io.TextIOWrapper. Instead we use codecs
        return codecs.getreader(encoding)(stream)


def encode_output_stream(stream, encoding):
    if encoding is None:
        return stream
    if PY3:
        try:
            return io.TextIOWrapper(stream.buffer, encoding=encoding)
        except AttributeError:
            # BytesIO doesn't have "buffer"
            return io.TextIOWrapper(stream, encoding=encoding)
    else:
        return codecs.getwriter(encoding)(stream)


def remove_utf8_bom(line, assumed_source_encoding):
    if assumed_source_encoding == 'latin-1' and len(line) >= 3 and line[:3] == '\xef\xbb\xbf':
        return line[3:]
    # TODO consider replacing "utf-8" with "utf-8-sig" to automatically remove BOM, see https://stackoverflow.com/a/44573867/2898283
    if assumed_source_encoding == 'utf-8' and len(line) >= 1 and line[0] == u'\ufeff':
        return line[1:]
    return line


def find_table_path(main_table_dir, table_id):
    # If table_id is a relative path it could be relative either to the current directory or to the main table dir.
    candidate_path = os.path.expanduser(table_id)
    if os.path.exists(candidate_path):
        return candidate_path
    if main_table_dir and not os.path.isabs(candidate_path):
        candidate_path = os.path.join(main_table_dir, candidate_path)
        if os.path.exists(candidate_path):
            return candidate_path
    return None


class CSVWriter(rbql_engine.RBQLOutputWriter):
    def __init__(self, stream, close_stream_on_finish, encoding, delim, policy, line_separator='\n', colorize_output=False):
        assert encoding in ['utf-8', 'latin-1', None]
        self.stream = encode_output_stream(stream, encoding)
        self.line_separator = line_separator
        self.delim = delim
        self.sub_array_delim = '|' if delim != '|' else ';'
        self.broken_pipe = False
        self.close_stream_on_finish = close_stream_on_finish
        self.polymorphic_preprocess = None
        self.polymorphic_join = self.join_by_delim 
        self.check_separators_after_join = False
        self.colors = None
        if policy == 'simple' or policy == 'whitespace':
            if colorize_output:
                self.polymorphic_preprocess = self.check_separators_in_fields_before_join
            else:
                self.check_separators_after_join = True
        elif policy == 'quoted':
            self.polymorphic_preprocess = self.quote_fields
        elif policy == 'quoted_rfc':
            self.polymorphic_preprocess = self.quote_fields_rfc
        elif policy == 'monocolumn':
            colorize_output = False
            self.polymorphic_preprocess = self.ensure_single_field
            self.polymorphic_join = self.monocolumn_join
        else:
            raise RuntimeError('unknown output csv policy')

        if colorize_output:
            self.colors = init_ansi_terminal_colors()

        self.none_in_output = False
        self.delim_in_simple_output = False
        self.header_len = None


    def set_header(self, header):
        if header is not None:
            self.header_len = len(header)
            self.write(header)


    def monocolumn_join(self, fields):
        return fields[0]


    def check_separators_in_fields_before_join(self, fields):
        if ''.join(fields).find(self.delim) != -1:
            self.delim_in_simple_output = True


    def check_separator_in_fields_after_join(self, output_line, num_fields_expected):
        num_fields_calculated = output_line.count(self.delim) + 1
        if num_fields_calculated != num_fields_expected:
            self.delim_in_simple_output = True


    def join_by_delim(self, fields):
        return self.delim.join(fields)


    def write(self, fields):
        if self.header_len is not None and len(fields) != self.header_len:
            raise rbql_engine.RbqlIOHandlingError('Inconsistent number of columns in output header and the current record: {} != {}'.format(self.header_len, len(fields)))
        self.normalize_fields(fields)

        if self.polymorphic_preprocess is not None:
            self.polymorphic_preprocess(fields)

        if self.colors is not None:
            self.colorize_fields(fields)

        out_line = self.polymorphic_join(fields)

        if self.check_separators_after_join:
            self.check_separator_in_fields_after_join(out_line, len(fields))

        try:
            self.stream.write(out_line)
            if self.colors is not None:
                self.stream.write(ansi_reset_color_code)
            self.stream.write(self.line_separator)
            return True
        except broken_pipe_exception as exc:
            if broken_pipe_exception == IOError:
                if exc.errno != EPIPE:
                    raise
            self.broken_pipe = True
            return False


    def colorize_fields(self, fields):
        for i in polymorphic_xrange(len(fields)):
            fields[i] = self.colors[i % len(self.colors)] + fields[i]


    def quote_fields(self, fields):
        for i in polymorphic_xrange(len(fields)):
            fields[i] = csv_utils.quote_field(fields[i], self.delim)


    def quote_fields_rfc(self, fields):
        for i in polymorphic_xrange(len(fields)):
            fields[i] = csv_utils.rfc_quote_field(fields[i], self.delim)


    def ensure_single_field(self, fields):
        if len(fields) > 1:
            raise rbql_engine.RbqlIOHandlingError('Unable to use "Monocolumn" output format: some records have more than one field')


    def normalize_fields(self, fields):
        for i in polymorphic_xrange(len(fields)):
            if PY3 and isinstance(fields[i], str):
                continue
            elif not PY3 and isinstance(fields[i], basestring):
                continue
            elif fields[i] is None:
                fields[i] = ''
                self.none_in_output = True
            elif isinstance(fields[i], list):
                self.normalize_fields(fields[i])
                fields[i] = self.sub_array_delim.join(fields[i])
            else:
                fields[i] = str(fields[i])


    def _write_all(self, table):
        for record in table:
            self.write(record[:])
        self.finish()


    def finish(self):
        if self.broken_pipe:
            return
        if self.close_stream_on_finish:
            self.stream.close()
        else:
            try:
                self.stream.flush() # This flush still can throw if all flushes before were sucessfull! And the exceptions would be printed anyway, even if it was explicitly catched just couple of lines after.
                # Basically this fails if output is small and this is the first flush after the pipe was broken e.g. second flush if piped to head -n 1
                # Here head -n 1 finished after the first flush, and the final explict flush here just killing it
            except broken_pipe_exception as exc:
                if broken_pipe_exception == IOError:
                    if exc.errno != EPIPE:
                        raise
                # In order to avoid BrokenPipeError from being printed as a warning to stderr, we need to perform this magic below. See:
                # Explanation 1: https://stackoverflow.com/a/35761190/2898283
                # Explanation 2: https://bugs.python.org/issue11380
                try:
                    sys.stdout.close()
                except Exception:
                    pass


    def get_warnings(self):
        result = list()
        if self.none_in_output:
            result.append('None values in output were replaced by empty strings')
        if self.delim_in_simple_output:
            result.append('Some output fields contain separator')
        return result


class JsonLinesRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, encoding, delim, policy, comment_prefix=None, table_name='input', variable_prefix='a', chunk_size=1024):
        assert encoding in ['utf-8', 'latin-1', None]
        self.encoding = encoding
        self.stream = encode_input_stream(stream, encoding)
        self.delim = delim
        self.policy = policy
        self.table_name = table_name
        self.variable_prefix = variable_prefix
        self.comment_prefix = comment_prefix if (comment_prefix is not None and len(comment_prefix)) else None

        self.buffer = ''
        self.detected_line_separator = '\n'
        self.exhausted = False
        self.NR = 0 # Record number
        self.NL = 0 # Line number
        self.chunk_size = chunk_size


    def get_variables_map(self, query_text):
        # We would map the 0-th element of the record to variable 'a' (or 'b') and in the reader we would wrap the json into a single-element array to avoid modifying the engine code.
        return {self.variable_prefix: rbql_engine.VariableInfo(initialize=True, index=0)}

    def get_header(self):
        return None

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
            if self.NL == 1:
                clean_line = remove_utf8_bom(row, self.encoding)
                if clean_line != row:
                    row = clean_line
            return row
        except UnicodeDecodeError:
            raise rbql_engine.RbqlIOHandlingError('Unable to decode input table as UTF-8. Use binary (latin-1) encoding instead')

    
    def get_record(self):
        while True:
            line = self.get_row_simple()
            if line is None:
                return None
            if self.comment_prefix is None or not line.startswith(self.comment_prefix):
                break
        self.NR += 1
        record = json.loads(line)
        return [record]


    #def get_all_records(self, num_rows=None):
    #    result = []
    #    while True:
    #        record = self.get_record()
    #        if record is None:
    #            break
    #        result.append(record)
    #        if num_rows is not None and len(result) >= num_rows:
    #            break
    #    return result


    def get_warnings(self):
        return []


class FileSystemCSVRegistry(rbql_engine.RBQLTableRegistry):
    def __init__(self, input_file_dir, delim, policy, encoding, has_header, comment_prefix):
        self.input_file_dir = input_file_dir
        self.delim = delim
        self.policy = policy
        self.encoding = encoding
        self.record_iterator = None
        self.input_stream = None
        self.has_header = has_header
        self.comment_prefix = comment_prefix
        self.table_path = None

    def get_iterator_by_table_id(self, table_id, single_char_alias):
        self.table_path = find_table_path(self.input_file_dir, table_id)
        if self.table_path is None:
            raise rbql_engine.RbqlIOHandlingError('Unable to find join table "{}"'.format(table_id))
        self.input_stream = open(self.table_path, 'rb')
        self.record_iterator = JsonLinesRecordIterator(self.input_stream, self.encoding, self.delim, self.policy, self.has_header, comment_prefix=self.comment_prefix, table_name=table_id, variable_prefix=single_char_alias)
        return self.record_iterator

    def finish(self):
        if self.input_stream is not None:
            self.input_stream.close()

    def get_warnings(self):
        result = []
        if self.record_iterator is not None and self.has_header:
            result.append('The first record in JOIN file {} was also treated as header (and skipped)'.format(os.path.basename(self.table_path))) # UT JSON CSV
        return result


def query_csv(query_text, input_path, input_delim, input_policy, output_path, output_delim, output_policy, csv_encoding, output_warnings, with_headers, comment_prefix=None, user_init_code='', colorize_output=False):
    output_stream, close_output_on_finish = (None, False)
    input_stream, close_input_on_finish = (None, False)
    join_tables_registry = None
    try:
        output_stream, close_output_on_finish = (sys.stdout, False) if output_path is None else (open(output_path, 'wb'), True)
        input_stream, close_input_on_finish = (sys.stdin, False) if input_path is None else (open(input_path, 'rb'), True)

        if input_delim == '"' and input_policy == 'quoted':
            raise rbql_engine.RbqlIOHandlingError('Double quote delimiter is incompatible with "quoted" policy')
        if input_delim != ' ' and input_policy == 'whitespace':
            raise rbql_engine.RbqlIOHandlingError('Only whitespace " " delim is supported with "whitespace" policy')

        if not is_ascii(query_text) and csv_encoding == 'latin-1':
            raise rbql_engine.RbqlIOHandlingError('To use non-ascii characters in query enable UTF-8 encoding instead of latin-1/binary')

        if (not is_ascii(input_delim) or not is_ascii(output_delim)) and csv_encoding == 'latin-1':
            raise rbql_engine.RbqlIOHandlingError('To use non-ascii separators enable UTF-8 encoding instead of latin-1/binary')

        default_init_source_path = os.path.join(os.path.expanduser('~'), '.rbql_init_source.py')
        if user_init_code == '' and os.path.exists(default_init_source_path):
            user_init_code = read_user_init_code(default_init_source_path)

        input_file_dir = None if not input_path else os.path.dirname(input_path)
        join_tables_registry = FileSystemCSVRegistry(input_file_dir, input_delim, input_policy, csv_encoding, with_headers, comment_prefix)
        input_iterator = JsonLinesRecordIterator(input_stream, csv_encoding, input_delim, input_policy, with_headers, comment_prefix=comment_prefix)
        output_writer = CSVWriter(output_stream, close_output_on_finish, csv_encoding, output_delim, output_policy, colorize_output=colorize_output)
        if debug_mode:
            rbql_engine.set_debug_mode()
        rbql_engine.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code)
    finally:
        if close_input_on_finish:
            input_stream.close()
        if close_output_on_finish:
            output_stream.close()
        if join_tables_registry:
            join_tables_registry.finish()
            output_warnings += join_tables_registry.get_warnings()


def set_debug_mode():
    global debug_mode
    debug_mode = True

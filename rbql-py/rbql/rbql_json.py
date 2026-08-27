import json
import sys
import os
import io
import re

from . import rbql_engine
from . import csv_utils
from . import rbql_csv

debug_mode = False

def set_debug_mode():
    global debug_mode
    debug_mode = True


def get_json_object_to_write(header, fields):
    if len(fields) == 1:
        return fields[0]
    result = dict()
    for i in range(len(fields)):
        key_name = header[i] if i < len(header) else 'col{}'.format(i)
        result[key_name] = fields[i]
    return result


def finalize_stream(stream, close_stream_on_finish):
    if close_stream_on_finish:
        stream.close()
        return
    try:
        stream.flush()
    except BrokenPipeError as exc:
        try:
            sys.stdout.close()
        except Exception:
            pass


def deduplicate_header_keys(header):
    # This algorithm is O(N^2) but we don't expect to have a lot of keys.
    # FIXME unit-test this function
    unique_keys = list()
    deduplicated_keys = set()
    for h in header:
        unique_key = h
        dedup_counter = 1
        while unique_key in unique_keys:
            dedup_counter += 1
            unique_key = '{}_{}'.format(h, dedup_counter)
        assert unique_key not in unique_keys
        unique_keys.append(unique_key)
        if dedup_counter != 1:
            deduplicated_keys.add(h)
    return (unique_keys, deduplicated_keys)


class JsonLinesWriter(rbql_engine.RBQLOutputWriter):
    def __init__(self, stream, close_stream_on_finish, encoding, line_separator='\n'):
        assert encoding in ['utf-8', 'latin-1', None]
        self.stream = rbql_csv.encode_output_stream(stream, encoding)
        self.line_separator = line_separator
        self.close_stream_on_finish = close_stream_on_finish
        self.broken_pipe = False
        self.header = []

    def write(self, fields):
        object_to_write = get_json_object_to_write(self.header, fields)
        try:
            json_str = json.dumps(object_to_write, ensure_ascii=False, default=str)
        except TypeError as e:
            raise rbql_engine.RbqlIOHandlingError('Error serializing object to JSON: {}'.format(e))

        try:
            self.stream.write(json_str)
            self.stream.write(self.line_separator)
            return True
        except BrokenPipeError as exc:
            self.broken_pipe = True
            return False

    def finish(self):
        if self.broken_pipe:
            return
        finalize_stream(self.stream, self.close_stream_on_finish)

    # FIXME apparently this not always gets called, we should mark all json files to have headers like csv `with headers` flag.
    # FIXME make sure it works with multistep paths
    def set_header(self, header):
        if header is None:
            return
        # Json objects don't allow duplicate keys so we have to dedup them to make sure they are unique to avoid accidental data loss.
        self.header, self.deduplicated_keys = deduplicate_header_keys(header)

    def get_warnings(self):
        warnings = []
        if len(self.deduplicated_keys) != 0:
            sorted_keys = sorted(['"{}"'.format(v) for v in list(self.deduplicated_keys)])
            warnings.append('Deduplicated output json keys to avoid data loss: {}'.format(', '.join(sorted_keys)))
        return warnings


# FIXME add a unit test with 2 columns with the same name - json should skip them.
# FIXME since json skips columns with the same name consider implementing column deduplication in the output header to avoid silently dropping them.


class JsonArrayObjectRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, encoding, table_name='input', variable_prefix='a'):
        assert encoding in ['utf-8', 'latin-1', None]
        self.encoding = encoding
        self.stream = rbql_csv.encode_input_stream(stream, encoding)
        self.table_name = table_name
        self.variable_prefix = variable_prefix

        self.NR = 0 # Record number
        self.NL = 0 # Line number
        try:
            self.json_object = json.load(self.stream)
        except json.decoder.JSONDecodeError as e:
            raise rbql_engine.RbqlIOHandlingError('Unable to parse input as JSON: {}'.format(e))
        if not isinstance(self.json_object, list):
            raise rbql_engine.RbqlIOHandlingError('Input JSON root node must be array in array iteration mode')
        self.NL = 1 # The object has to start at the first line so we just keep NL at 1.

    def get_header(self):
        # FIXME consider if this is a hack or not. Test queries with stars like `SELECT a.*, a.*` or `SELECT *`.
        # We might not need this (i.e. we can have it return None as the default impl) if we support the write_header flag, see the rbql_engine.py comments.
        return [self.variable_prefix + '1']

    def get_record(self):
        if self.NR >= len(self.json_object):
            return None
        self.NR += 1
        return [self.json_object[self.NR - 1]]


class JsonArrayObjectWriter(rbql_engine.RBQLOutputWriter):
    def __init__(self, stream, close_stream_on_finish, encoding, line_separator='\n', pretty_indent=None):
        assert encoding in ['utf-8', 'latin-1', None]
        self.stream = rbql_csv.encode_output_stream(stream, encoding)
        self.line_separator = line_separator
        self.close_stream_on_finish = close_stream_on_finish
        self.broken_pipe = False
        self.header = []
        self.pretty_indent=pretty_indent
        self.num_records_written = 0
        self.deduplicated_keys = set()

    def write(self, fields):
        object_to_write = get_json_object_to_write(self.header, fields)
        try:
            # Intentionally do not split json_str to add extra pretty indents because the root level is a flat array and shifting everything right just reduces density for the sake of dubious consistency. 
            json_str = json.dumps(object_to_write, ensure_ascii=False, default=str, indent=self.pretty_indent)
        except TypeError as e:
            raise rbql_engine.RbqlIOHandlingError('Error serializing object to JSON: {}'.format(e))

        try:
            if self.num_records_written == 0:
                self.stream.write('[')
                self.stream.write(self.line_separator)
            else:
                self.stream.write(',')
                self.stream.write(self.line_separator)
            self.stream.write(json_str)
            self.num_records_written += 1
            return True
        except BrokenPipeError as exc:
            self.broken_pipe = True
            return False

    def finish(self):
        if self.broken_pipe:
            return
        if self.num_records_written == 0:
            # Output an empty array if no entries were produced
            self.stream.write('[')
        self.stream.write(self.line_separator)
        self.stream.write(']')
        self.stream.write(self.line_separator) # POSIX requires a newline at the end of text files.
        finalize_stream(self.stream, self.close_stream_on_finish)

    # FIXME apparently this not always gets called, we should mark all json files to have headers like csv `with headers` flag.
    # FIXME make sure it works with multistep paths
    def set_header(self, header):
        if header is None:
            return
        # Json objects don't allow duplicate keys so we have to dedup them to make sure they are unique to avoid accidental data loss.
        self.header, self.deduplicated_keys = deduplicate_header_keys(header)

    def get_warnings(self):
        warnings = []
        if len(self.deduplicated_keys) != 0:
            sorted_keys = sorted(['"{}"'.format(v) for v in list(self.deduplicated_keys)])
            warnings.append('Deduplicated output json keys to avoid data loss: {}'.format(', '.join(sorted_keys)))
        return warnings


# NOTE: using json lines format as input is essentially equivalent to `select json.loads(a1) | select a1['name']` type of query.
class JsonLinesRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, encoding, table_name='input', variable_prefix='a', chunk_size=1024):
        assert encoding in ['utf-8', 'latin-1', None]
        self.encoding = encoding
        self.stream = rbql_csv.encode_input_stream(stream, encoding)
        self.table_name = table_name
        self.variable_prefix = variable_prefix

        self.buffer = ''
        self.exhausted = False
        self.NR = 0 # Record number
        self.NL = 0 # Line number
        self.chunk_size = chunk_size
        self.utf8_bom_removed = False

    def get_header(self):
        # FIXME consider if this is a hack or not. Test queries with stars like `SELECT a.*, a.*` or `SELECT *`.
        # We might not need this (i.e. we can have it return None as the default impl) if we support the write_header flag, see the rbql_engine.py comments.
        return [self.variable_prefix + '1']

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

    def get_row(self):
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
                clean_line = rbql_csv.remove_utf8_bom(row, self.encoding)
                if clean_line != row:
                    row = clean_line
                    self.utf8_bom_removed = True
            return row
        except UnicodeDecodeError:
            raise rbql_engine.RbqlIOHandlingError('Unable to decode input table as UTF-8. Use binary (latin-1) encoding instead')

    def get_record(self):
        while True:
            line = self.get_row()
            if line is None:
                return None
            line = line.strip()
            if not line:
                continue
            try:
                json_obj = json.loads(line)
                self.NR += 1
                return [json_obj]
            except json.JSONDecodeError as e:
                raise rbql_engine.RbqlIOHandlingError('Error decoding JSON in {} table at record {}, line {}: {}'.format(self.table_name, self.NR + 1, self.NL, str(e)))

    def get_warnings(self):
        result = []
        if self.utf8_bom_removed:
            result.append('UTF-8 Byte Order Mark (BOM) was found and skipped in {} table'.format(self.table_name))
        return result


# TODO we might want the output to optionally be CSV too. 
def query_json(query_text, input_path, output_path, output_warnings, user_init_code='', input_json_lines=True, output_json_lines=True, pretty_indent=None):
    output_stream, close_output_on_finish = (None, False)
    input_stream, close_input_on_finish = (None, False)
    join_tables_registry = None
    try:
        output_stream, close_output_on_finish = (sys.stdout, False) if output_path is None else (open(output_path, 'wb'), True)
        input_stream, close_input_on_finish = (sys.stdin, False) if input_path is None else (open(input_path, 'rb'), True)

        default_init_source_path = os.path.join(os.path.expanduser('~'), '.rbql_init_source.py')
        if user_init_code == '' and os.path.exists(default_init_source_path):
            user_init_code = rbql_csv.read_user_init_code(default_init_source_path)
        input_iterator = None
        if input_json_lines:
            input_iterator = JsonLinesRecordIterator(input_stream, 'utf-8', table_name='input', variable_prefix='a')
        else:
            input_iterator = JsonArrayObjectRecordIterator(input_stream, 'utf-8', table_name='input', variable_prefix='a')
        if output_json_lines:
            output_writer = JsonLinesWriter(output_stream, close_output_on_finish, 'utf-8')
        else:
            output_writer = JsonArrayObjectWriter(output_stream, close_output_on_finish, 'utf-8', pretty_indent=pretty_indent)
        if debug_mode:
            rbql_engine.set_debug_mode()
        rbql_engine.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code)
    finally:
        if close_input_on_finish:
            input_stream.close()
        if close_output_on_finish:
            output_stream.close()


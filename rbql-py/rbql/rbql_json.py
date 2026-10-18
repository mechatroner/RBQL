import json
import sys
import os
import io
import re

from collections import deque

from . import rbql_engine
from . import csv_utils
from . import rbql_csv

debug_mode = False

def set_debug_mode():
    global debug_mode
    debug_mode = True

# FIXME merge all iterators into a single one (use pre-parsing approach from js impl) and all writers into one.

backslash_char = '\\'

class RbqlJsonParsingError(Exception):
    pass

class UnparsedJsonBlob:
    def __init__(self):
        self.bracket_stack = []
        self.inside_string = False
        

# We call it JsonFinalizer in js
class BalancingIterator:
    def __init__(self, unnest_mode=False):
        self.buf = ''
        self.unnest_mode = unnest_mode
        self.json_blobs = deque()
        self.current = UnparsedJsonBlob()
        self.open_bracket_skipped = False if self.unnest_mode else True
        self.closing_bracket_skipped = False

    def add_fragment(self, fragment):
        seek_start_pos = len(self.buf)
        self.buf += fragment
        self._parse_forward(seek_start_pos)

    def get_current_blob(self):
        return self.json_blobs[0]

    def advance(self):
        return self.json_blobs.popleft()

    def empty(self):
        return len(self.json_blobs) == 0

    def has_trailing_data(self):
        return len(self.buf)

    def finish(self):
        if self.unnest_mode and not self.closing_bracket_skipped:
            raise RbqlJsonParsingError("Closing square bracket `]` at the end is required in unnest mode")
        if not self.current.inside_string and len(self.current.bracket_stack) == 0:
            self._consume_fragment(self.buf)
            self.buf = ''

    def _consume_fragment(self, fragment):
        fragment = fragment.strip()
        if len(fragment):
            self.json_blobs.append(fragment)
            self.current = UnparsedJsonBlob() 

    def skip_open_square_bracket(self, idx):
        while idx < len(self.buf) and self.buf[idx].isspace():
            idx += 1
        if idx >= len(self.buf):
            return idx
        if self.buf[idx] != '[':
            # FIXME ut this
            raise RbqlJsonParsingError("Input data must be wrapped in JSON array in unnest mode. Expected `[` got `{}` char".format(self.buf[idx]))
        self.open_bracket_skipped = True
        return idx + 1

    def skip_trailing_spaces(self, idx):
        while idx < len(self.buf):
            if self.buf[idx].isspace():
                idx += 1
                continue
            # FIXME ut this
            raise RbqlJsonParsingError("Trailing data after the closing `]` bracket in unnest mode")
        return idx

    def _parse_forward(self, seek_start_pos):
        i = seek_start_pos
        if not self.open_bracket_skipped:
            i = self.skip_open_square_bracket(i)
            self.buf = self.buf[i:]
            i = 0
        while i < len(self.buf):
            if self.closing_bracket_skipped:
                self.skip_trailing_spaces(i)
                return
            object_can_end = False
            end_offset = 0 # This is to be able to drop trailing `,` or `]` in the unnest mode.
            if self.current.inside_string:
                if self.buf[i] == '"':
                    # It is safe to check buf[i - 1] because we somehow got into the string so we must have parsed something already.
                    # And if self.buf[i - 1] == backslash_char it is also safe to read self.buf[i - 2] by the same logic.
                    if self.buf[i - 1] != backslash_char or self.buf[i - 2] == backslash_char:
                        self.current.inside_string = False
                        # This is to handle a sequence of json strings like this `"str1""str2""str3"` - strings don't have to be separated with whitespaces.
                        object_can_end = True
            elif self.buf[i] == '"':
                self.current.inside_string = True
            elif self.buf[i] == '{' or self.buf[i] == '[':
                self.current.bracket_stack.append(self.buf[i])
            elif self.buf[i] == '}':
                if len(self.current.bracket_stack) == 0 or self.current.bracket_stack.pop() != '{':
                    raise RbqlJsonParsingError("Unbalanced closing curly bracket `}`")
                object_can_end = True
            elif self.buf[i] == ']':
                if self.unnest_mode and len(self.current.bracket_stack) == 0 and not self.closing_bracket_skipped:
                    self.closing_bracket_skipped = True
                    end_offset = 1
                elif len(self.current.bracket_stack) == 0 or self.current.bracket_stack.pop() != '[':
                    raise RbqlJsonParsingError("Unbalanced closing square bracket `]`")
                object_can_end = True
            elif self.buf[i] == ',' and self.unnest_mode:
                # TODO This would make a sequence of commas a valid noop span, consider fixing it.
                end_offset = 1
                object_can_end = True
            elif self.buf[i].isspace():
                object_can_end = True
            i += 1
            if object_can_end and len(self.current.bracket_stack) == 0:
                fragment = self.buf[:i - end_offset]
                self.buf = self.buf[i:]
                i = 0
                self._consume_fragment(fragment)


def get_json_object_to_write(header, fields):
    if len(fields) == 1:
        return fields[0]
    if len(header) and len(fields) != len(header):
        raise rbql_engine.RbqlIOHandlingError('Inconsistent number of columns in output header and the current record: {} != {}'.format(len(header), len(fields)))
    result = dict()
    for i in range(len(fields)):
        key_name = header[i] if i < len(header) else 'col{}'.format(i + 1)
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
    unique_keys = list()
    deduplicated_keys = set()
    for h in header:
        unique_key = h
        dedup_counter = 1
        while unique_key in unique_keys:
            dedup_counter += 1
            unique_key = '{}_{}'.format(h, dedup_counter)
        unique_keys.append(unique_key)
        if dedup_counter != 1:
            deduplicated_keys.add(h)
    return (unique_keys, sorted(list(deduplicated_keys)))


class JsonWriter(rbql_engine.RBQLOutputWriter):
    # By setting `wrap_in_json_array` to false (and pretty_indent) we get json-stream writer
    def __init__(self, stream, close_stream_on_finish, encoding, line_separator='\n', pretty_indent=None, wrap_in_json_array=True):
        assert encoding in ['utf-8', 'latin-1', None]
        self.stream = rbql_csv.encode_output_stream(stream, encoding)
        self.line_separator = line_separator
        self.close_stream_on_finish = close_stream_on_finish
        self.broken_pipe = False
        self.pretty_indent=pretty_indent
        self.num_records_written = 0
        self.header = []
        self.deduplicated_keys = []
        self.wrap_in_json_array = wrap_in_json_array

    def write(self, fields):
        object_to_write = get_json_object_to_write(self.header, fields)
        try:
            # Intentionally do not split json_str to add extra pretty indents because the root level is a flat array and shifting everything right just reduces density for the sake of dubious consistency. 
            json_str = json.dumps(object_to_write, ensure_ascii=False, default=str, indent=self.pretty_indent)
        except TypeError as e:
            raise rbql_engine.RbqlIOHandlingError('Error serializing object to JSON: {}'.format(e))

        try:
            if self.num_records_written == 0:
                if self.wrap_in_json_array:
                    self.stream.write('[')
                    self.stream.write(self.line_separator)
            else:
                if self.wrap_in_json_array:
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
        if self.wrap_in_json_array:
            if self.num_records_written == 0:
                # Output an empty array if no entries were produced.
                self.stream.write('[')
            self.stream.write(self.line_separator)
            self.stream.write(']')
            self.stream.write(self.line_separator) # POSIX requires a newline at the end of text files.
        else:
            if self.num_records_written > 0:
                self.stream.write(self.line_separator) # POSIX requires a newline at the end of text files.
        finalize_stream(self.stream, self.close_stream_on_finish)

    def set_header(self, header):
        if header is None:
            return
        # Json objects don't allow duplicate keys so we have to dedup them to make sure they are unique to avoid accidental data loss.
        self.header, self.deduplicated_keys = deduplicate_header_keys(header)

    def get_warnings(self):
        warnings = []
        if len(self.deduplicated_keys) != 0:
            sorted_keys = sorted(['"{}"'.format(v) for v in self.deduplicated_keys])
            warnings.append('Deduplicated output json keys to avoid data loss: {}'.format(', '.join(sorted_keys)))
        return warnings


# NOTE: using json lines format as input is essentially equivalent to `select json.loads(a1) | select a1['name']` type of query.
class JsonStreamRecordIterator(rbql_engine.RBQLInputIterator):
    def __init__(self, stream, encoding, table_name='input', variable_prefix='a', chunk_size=16384, unnest_mode=False):
        assert encoding in ['utf-8', 'latin-1', None]
        self.encoding = encoding
        self.stream = rbql_csv.encode_input_stream(stream, encoding)
        self.table_name = table_name
        self.variable_prefix = variable_prefix

        self.exhausted = False
        self.chunk_size = chunk_size
        self.num_chunks_read_internal_stat = 0 # This is for unit tests only
        self.record_number = 0
        self.balancing_iterator = BalancingIterator(unnest_mode=unnest_mode)

    def get_header(self):
        # Returning "a1" as a column name instead of "a" would have a side effect because it would initialize a.a1 and a['a1'] values in rbql engine.
        return [self.variable_prefix]
    
    def get_exhausted(self):
        if self.balancing_iterator.has_trailing_data():
            raise rbql_engine.RbqlIOHandlingError("Trailing data in {} stream that can't be parsed as JSON objects after record {}".format(self.table_name, self.record_number))
        return None

    def get_and_advance(self):
        blob = self.balancing_iterator.advance()
        self.record_number += 1
        json_obj = None
        try:
            json_obj = json.loads(blob)
        except json.JSONDecodeError as e:
            raise rbql_engine.RbqlIOHandlingError('Unable to parse {} record {} as JSON: {}'.format(table_name, self.record_number, e))
        return [json_obj]

    def get_record(self):
        # FIXME add error handling - we need to add info to the message at what entry it has failed
        while True:
            if self.exhausted:
                return self.get_exhausted()
            if not self.balancing_iterator.empty():
                return self.get_and_advance()
            chunk = self.stream.read(self.chunk_size)
            if not chunk:
                self.exhausted = True
                self.balancing_iterator.finish()
                if not self.balancing_iterator.empty():
                    return self.get_and_advance()
            else:
                self.balancing_iterator.add_fragment(chunk)
                self.num_chunks_read_internal_stat += 1



# TODO we might want the output to optionally be CSV too. 
# FIXME we need to adjust the interface - it should be unwrap_array: bool, wrap_into_array: bool, pretty_indent: int
def query_json(query_text, input_path, output_path, output_warnings, user_init_code='', input_json_lines=True, output_json_lines=True, pretty_indent=None, input_json_stream=False, output_json_stream=False):
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
            input_iterator = JsonStreamRecordIterator(input_stream, 'utf-8', table_name='input', variable_prefix='a')
        elif input_json_stream:
            input_iterator = JsonStreamRecordIterator(input_stream, 'utf-8', table_name='input', variable_prefix='a')
        else:
            input_iterator = JsonStreamRecordIterator(input_stream, 'utf-8', table_name='input', variable_prefix='a', unnest_mode=True)

        output_writer = None
        if output_json_lines:
            output_writer = JsonWriter(output_stream, close_output_on_finish, 'utf-8', pretty_indent=None, wrap_in_json_array=False)
        elif output_json_stream:
            output_writer = JsonWriter(output_stream, close_output_on_finish, 'utf-8', pretty_indent=pretty_indent, wrap_in_json_array=False)
        else:
            output_writer = JsonWriter(output_stream, close_output_on_finish, 'utf-8', pretty_indent=pretty_indent)

        if debug_mode:
            rbql_engine.set_debug_mode()
        rbql_engine.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code)
    finally:
        if close_input_on_finish:
            input_stream.close()
        if close_output_on_finish:
            output_stream.close()


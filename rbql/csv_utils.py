from __future__ import unicode_literals
from __future__ import print_function
import sys
import os
import re
import io
import codecs


PY3 = sys.version_info[0] == 3


newline_rgx = re.compile('(?:\r\n)|\r|\n')

field_regular_expression = '"((?:[^"]*"")*[^"]*)"'
field_rgx = re.compile(field_regular_expression)
field_rgx_external_whitespaces = re.compile(' *'+ field_regular_expression + ' *')


user_home_dir = os.path.expanduser('~')
table_names_settings_path = os.path.join(user_home_dir, '.rbql_table_names')


class RbqlIOHandlingError(Exception):
    pass


def normalize_delim(delim):
    if delim == 'TAB':
        return '\t'
    if delim == r'\t':
        return '\t'
    return delim


def get_encoded_stdin(encoding_name):
    if PY3:
        return io.TextIOWrapper(sys.stdin.buffer, encoding=encoding_name)
    else:
        return codecs.getreader(encoding_name)(sys.stdin)


def get_encoded_stdout(encoding_name):
    if PY3:
        return io.TextIOWrapper(sys.stdout.buffer, encoding=encoding_name)
    else:
        return codecs.getwriter(encoding_name)(sys.stdout)


class InputStreamManager:
    def __init__(self, input_path, csv_encoding):
        self.input_path = input_path
        self.csv_encoding = csv_encoding
        self.stream = None

    def __enter__(self):
        if self.input_path:
            self.stream = codecs.open(self.input_path, encoding=self.csv_encoding)
        else:
            self.stream = get_encoded_stdin(self.csv_encoding)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.input_path:
            try:
                self.stream.close()
            except Exception:
                pass


class OutputStreamManager:
    def __init__(self, output_path, csv_encoding):
        self.output_path = output_path
        self.csv_encoding = csv_encoding
        self.stream = None

    def __enter__(self):
        if self.output_path:
            self.stream = codecs.open(self.output_path, 'w', encoding=self.csv_encoding)
        else:
            self.stream = get_encoded_stdout(self.csv_encoding)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.output_path:
                self.stream.close()
            else:
                self.stream.flush()
        except Exception:
            pass



def extract_next_field(src, dlm, preserve_quotes, allow_external_whitespaces, cidx, result):
    warning = False
    rgx = field_rgx_external_whitespaces if allow_external_whitespaces else field_rgx
    match_obj = rgx.match(src, cidx)
    if match_obj is not None:
        match_end = match_obj.span()[1]
        if match_end == len(src) or src[match_end] == dlm:
            if preserve_quotes:
                result.append(match_obj.group(0))
            else:
                result.append(match_obj.group(1).replace('""', '"'))
            return (match_end + 1, False)
        warning = True
    uidx = src.find(dlm, cidx)
    if uidx == -1:
        uidx = len(src)
    field = src[cidx:uidx]
    warning = warning or field.find('"') != -1
    result.append(field)
    return (uidx + 1, warning)



def split_quoted_str(src, dlm, preserve_quotes=False):
    assert dlm != '"'
    if src.find('"') == -1: # Optimization for most common case
        return (src.split(dlm), False)
    result = list()
    cidx = 0
    warning = False
    allow_external_whitespaces = dlm != ' '
    while cidx < len(src):
        extraction_report = extract_next_field(src, dlm, preserve_quotes, allow_external_whitespaces, cidx, result)
        cidx = extraction_report[0]
        warning = warning or extraction_report[1]

    if src[-1] == dlm:
        result.append('')
    return (result, warning)


def split_whitespace_separated_str(src, preserve_whitespaces=False):
    rgxp = re.compile(" *[^ ]+ *") if preserve_whitespaces else re.compile("[^ ]+")
    result = []
    for m in rgxp.finditer(src):
        result.append(m.group())
    return result


def smart_split(src, dlm, policy, preserve_quotes):
    if policy == 'simple':
        return (src.split(dlm), False)
    if policy == 'whitespace':
        return (split_whitespace_separated_str(src, preserve_quotes), False)
    if policy == 'monocolumn':
        return ([src], False)
    return split_quoted_str(src, dlm, preserve_quotes)


def extract_line_from_data(data):
    mobj = newline_rgx.search(data)
    if mobj is None:
        return (None, None, data)
    pos_start, pos_end = mobj.span()
    str_before = data[:pos_start]
    str_after = data[pos_end:]
    return (str_before, mobj.group(0), str_after)


def remove_utf8_bom(line, assumed_source_encoding):
    if assumed_source_encoding == 'latin-1' and len(line) >= 3 and line[:3] == '\xef\xbb\xbf':
        return line[3:]
    if assumed_source_encoding == 'utf-8' and len(line) >= 1 and line[0] == u'\ufeff':
        return line[1:]
    return line


def str6(obj):
    # We have to use this function because str() for python2.7 tries to ascii-encode unicode strings
    if PY3 and isinstance(obj, str):
        return obj
    if not PY3 and isinstance(obj, basestring):
        return obj
    return str(obj)


def quote_field(src, delim):
    if src.find('"') != -1 or src.find(delim) != -1:
        escaped = src.replace('"', '""')
        escaped = '"{}"'.format(escaped)
        return escaped
    return src


def unquote_field(field):
    field_rgx_external_whitespaces = re.compile('^ *"((?:[^"]*"")*[^"]*)" *$')
    match_obj = field_rgx_external_whitespaces.match(field)
    if match_obj is not None:
        return match_obj.group(1).replace('""', '"')
    return field


def unquote_fields(fields):
    return [unquote_field(f) for f in fields]


def try_read_index(index_path):
    lines = []
    try:
        with open(index_path) as f:
            lines = f.readlines()
    except Exception:
        return []
    result = list()
    for line in lines:
        line = line.rstrip('\r\n')
        record = line.split('\t')
        result.append(record)
    return result


def get_index_record(index_path, key):
    records = try_read_index(index_path)
    for record in records:
        if len(record) and record[0] == key:
            return record
    return None


def find_table_path(table_id):
    candidate_path = os.path.expanduser(table_id)
    if os.path.exists(candidate_path):
        return candidate_path
    name_record = get_index_record(table_names_settings_path, table_id)
    if name_record is not None and len(name_record) > 1 and os.path.exists(name_record[1]):
        return name_record[1]
    return None


class CSVWriter:
    def __init__(self, dst, delim, policy):
        self.dst = dst
        self.delim = delim
        if policy == 'simple':
            self.join_func = self.simple_join
        elif policy == 'quoted':
            self.join_func = self.quoted_join
        elif policy == 'monocolumn':
            self.join_func = self.mono_join
        elif policy == 'whitespace':
            self.join_func = self.simple_join
        else:
            raise RuntimeError('unknown output csv policy')

        self.none_in_output = False
        self.delim_in_simple_output = False


    def quoted_join(self, fields):
        return self.delim.join([quote_field(f, self.delim) for f in fields])


    def mono_join(self, fields):
        if len(fields) > 1:
            raise RbqlIOHandlingError('Unable to use "Monocolumn" output format: some records have more than one field')
        return fields[0]


    def simple_join(self, fields):
        res = self.delim.join([f for f in fields])
        num_fields = res.count(self.delim)
        if num_fields + 1 != len(fields):
            self.delim_in_simple_output = True
        return res


    def replace_none_values(self, fields):
        i = 0
        while i < len(fields):
            if fields[i] is None:
                fields[i] = ''
                self.none_in_output = True
            i += 1


    def write(self, fields):
        self.replace_none_values(fields)
        fields = [str6(f) for f in fields]
        self.dst.write(self.join_func(fields, self.delim))


    def finish(self):
        try:
            self.dst.flush()
        except Exception:
            pass


    def get_warnings(self):
        result = list()
        if self.none_in_output:
            result.append('None values in output were replaced by empty strings')
        if self.delim_in_simple_output:
            result.append('Some output fields contain separator')
        return result



def make_inconsistent_num_fields_warning(table_name, inconsistent_records_info):
    assert len(inconsistent_records_info) > 1
    inconsistent_records_info = inconsistent_records_info.items()
    inconsistent_records_info = sorted(inconsistent_records_info, key=lambda v: v[1])
    num_fields_1, record_num_1 = inconsistent_records_info[0]
    num_fields_2, record_num_2 = inconsistent_records_info[1]
    warn_msg = 'Number of fields in "{}" table is not consistent: '.format(table_name)
    warn_msg += 'e.g. record {} -> {} fields, record {} -> {} fields'.format(record_num_1, num_fields_1, record_num_2, num_fields_2)
    return warn_msg



class CSVRecordIterator:
    # Possibly can add presort_for_merge_join(key_index) method.
    # CSV tables are usually small, no need to use Merge algorithm. Also true when B is small (fits in memory) and A is big
    # Potentially this can be useful if someone decides to use RBQL for MapReduce tables when rhs table B is very big.

    def __init__(self, src, encoding, delim, policy, table_name='input', chunk_size=1024):
        assert encoding in ['utf-8', 'latin-1']
        self.src = src
        self.encoding = encoding
        self.delim = delim
        self.policy = policy
        self.table_name = table_name

        self.buffer = ''
        self.detected_line_separator = '\n'
        self.exhausted = False
        self.NR = 0
        self.chunk_size = chunk_size
        self.fields_info = dict()

        self.utf8_bom_removed = False
        self.first_defective_line = None # TODO use line # instead of record # when "\n" is done


    def finish(self):
        pass


    def _get_row_from_buffer(self):
        str_before, separator, str_after = extract_line_from_data(self.buffer)
        if separator is None:
            return None
        if separator == '\r' and str_after == '':
            one_more = self.src.read(1)
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
            chunk = self.src.read(self.chunk_size)
            if not chunk:
                self.exhausted = True
                break
            chunks.append(chunk)
            if newline_rgx.search(chunk) is not None:
                break
        self.buffer += ''.join(chunks)


    def get_row(self):
        # FIXME make sure this function does not raise UnicodeDecodeError. write UT
        row = self._get_row_from_buffer()
        if row is not None:
            return row
        self._read_until_found()
        row = self._get_row_from_buffer()
        if row is None:
            assert self.exhausted
            if self.buffer:
                tmp = self.buffer
                self.buffer = ''
                return tmp
            return None
        return row


    def get_record(self):
        try:
            line = self.get_row()
            if line is None:
                return None
            if self.NR == 0:
                clean_line = remove_utf8_bom(line, self.encoding)
                if clean_line != line:
                    line = clean_line
                    self.utf8_bom_removed = True
            self.NR += 1
            record, warning = smart_split(line, self.delim, self.policy, preserve_quotes=False)
            if warning and self.first_defective_line is None:
                self.first_defective_line = self.NR
            num_fields = len(record)
            if num_fields not in self.fields_info:
                self.fields_info[num_fields] = self.NR
            return record
        except UnicodeDecodeError:
            # FIXME make sure this function raise on binary input. write UT
            assert self.encoding == 'utf-8', 'Unexpected UnicodeDecodeError with {} encoding'.format(self.encoding)
            raise RbqlIOHandlingError('Unable to decode input table as UTF-8. Use binary (latin-1) encoding instead.')


    def get_warnings(self):
        result = list()
        if self.utf8_bom_removed:
            result.append('UTF-8 Byte Order Mark (BOM) was found and skipped in {} table'.format(self.table_name))
        if self.first_defective_line is not None:
            result.append('Defective double quote escaping in {} table. E.g. at line {}'.format(self.table_name, self.first_defective_line))
        if len(self.fields_info) > 1:
            result.append(make_inconsistent_num_fields_warning(self.table_name, self.fields_info))
        return result


class FileSystemCSVRegistry:
    def __init__(self, delim, policy, csv_encoding):
        self.delim = delim
        self.policy = policy
        self.csv_encoding = csv_encoding

    def get_iterator_by_table_id(self, table_id):
        table_path = find_table_path(table_id)
        if table_path is None:
            raise RbqlIOHandlingError('Unable to find join table: "{}"'.format(table_id))
        src = codecs.open(table_path, encoding=self.csv_encoding)
        record_iterator = CSVRecordIterator(src, self.csv_encoding, self.delim, self.policy, table_name=table_id)
        return record_iterator



from __future__ import unicode_literals
from __future__ import print_function
import re
from collections import defaultdict

newline_rgx = re.compile('(?:\r\n)|\r|\n')

field_regular_expression = '"((?:[^"]*"")*[^"]*)"'
field_rgx = re.compile(field_regular_expression)
field_rgx_external_whitespaces = re.compile(' *'+ field_regular_expression + ' *')

# FIXME rename to rb_csv_utils.py

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


def quoted_join(fields, delim):
    return delim.join([quote_field(f, delim) for f in fields])


def mono_join(fields, delim):
    # FIXME
    if enable_monocolumn_csv_ux_optimization_hack and '__RBQLMP__input_policy' == 'monocolumn':
        global output_switch_to_csv
        if output_switch_to_csv is None:
            output_switch_to_csv = (len(fields) > 1)
        assert output_switch_to_csv == (len(fields) > 1), 'Monocolumn optimization logic failure'
        if output_switch_to_csv:
            return quoted_join(fields, ',')
        else:
            return fields[0]
    if len(fields) > 1:
        raise RbqlRuntimeError('Unable to use "Monocolumn" output format: some records have more than one field')
    return fields[0]


def simple_join(fields, delim):
    res = delim.join([f for f in fields])
    num_fields = res.count(delim)
    if num_fields + 1 != len(fields):
        global delim_in_simple_output
        delim_in_simple_output = True
    return res


def try_flush(dst_stream):
    try:
        dst_stream.flush()
    except Exception:
        pass


class CSVWriter:
    def __init__(self, dst, delim, policy):
        self.dst = dst
        self.delim = delim
        self.none_in_output = False
        if policy == 'simple':
            self.join_func = simple_join
        elif policy == 'quoted':
            self.join_func = quoted_join
        elif policy == 'monocolumn':
            self.join_func = mono_join
        elif policy == 'whitespace':
            self.join_func = simple_join
        else:
            raise RuntimeError('unknown output csv policy')


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
        self.dst.write(self.join_func(fields, delim))


    def finish(self):
        try_flush(self.dst)


class CSVRecordIterator:
    def __init__(self, src, encoding, delim, policy, chunk_size=1024):
        self.src = src
        self.encoding = encoding
        self.delim = delim
        self.policy = policy

        self.buffer = ''
        self.detected_line_separator = '\n'
        self.exhausted = False
        self.utf8_bom_removed = False
        self.NR = 0
        self.first_defective_line = None # TODO use line # instead of record # when "\n" is done
        self.chunk_size = chunk_size


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


    # In the future we can add presort_for_merge_join(key_index) method
    # CSV tables are usually small, no need to use Merge algorithm
    # Potentially this can be useful if someone decides to use RBQL for MapReduce tables when rhs table B is very big.
    # No need to use this if B is small (fits in memory) and A is big

    def get_record(self):
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
            self.first_defective_line = NR
        return record

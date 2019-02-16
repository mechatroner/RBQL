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


# TODO consider moving RBQL aggregators and related code into another module. Maybe even in rbql.py
class NumHandler:
    def __init__(self):
        self.is_int = True
    
    def parse(self, str_val):
        if not self.is_int:
            return float(str_val)
        try:
            return int(str_val)
        except ValueError:
            self.is_int = False
            return float(str_val)


class MinAggregator:
    def __init__(self):
        self.stats = dict()
        self.num_handler = NumHandler()

    def increment(self, key, val):
        val = self.num_handler.parse(val)
        cur_aggr = self.stats.get(key)
        if cur_aggr is None:
            self.stats[key] = val
        else:
            self.stats[key] = min(cur_aggr, val)

    def get_final(self, key):
        return self.stats[key]


class MaxAggregator:
    def __init__(self):
        self.stats = dict()
        self.num_handler = NumHandler()

    def increment(self, key, val):
        val = self.num_handler.parse(val)
        cur_aggr = self.stats.get(key)
        if cur_aggr is None:
            self.stats[key] = val
        else:
            self.stats[key] = max(cur_aggr, val)

    def get_final(self, key):
        return self.stats[key]


class CountAggregator:
    def __init__(self):
        self.stats = defaultdict(int)

    def increment(self, key, val):
        self.stats[key] += 1

    def get_final(self, key):
        return self.stats[key]


class SumAggregator:
    def __init__(self):
        self.stats = defaultdict(int)
        self.num_handler = NumHandler()

    def increment(self, key, val):
        val = self.num_handler.parse(val)
        self.stats[key] += val

    def get_final(self, key):
        return self.stats[key]


def pretty_format(val):
    # FIXME get rid of this. Return value instead of string
    if val == 0:
        return '0.0'
    if abs(val) < 1:
        return str(val)
    formatted = "{0:.6f}".format(val)
    if formatted.find('.') != -1:
        formatted = formatted.rstrip('0')
    if formatted.endswith('.'):
        formatted += '0'
    return formatted


class AvgAggregator:
    def __init__(self):
        self.stats = dict()

    def increment(self, key, val):
        val = float(val)
        cur_aggr = self.stats.get(key)
        if cur_aggr is None:
            self.stats[key] = (val, 1)
        else:
            cur_sum, cur_cnt = cur_aggr
            self.stats[key] = (cur_sum + val, cur_cnt + 1)

    def get_final(self, key):
        final_sum, final_cnt = self.stats[key]
        avg = float(final_sum) / final_cnt
        return pretty_format(avg)


class VarianceAggregator:
    def __init__(self):
        self.stats = dict()

    def increment(self, key, val):
        val = float(val)
        cur_aggr = self.stats.get(key)
        if cur_aggr is None:
            self.stats[key] = (val, val ** 2, 1)
        else:
            cur_sum, cur_sum_of_squares, cur_cnt = cur_aggr
            self.stats[key] = (cur_sum + val, cur_sum_of_squares + val ** 2, cur_cnt + 1)

    def get_final(self, key):
        final_sum, final_sum_of_squares, final_cnt = self.stats[key]
        variance = float(final_sum_of_squares) / final_cnt - (float(final_sum) / final_cnt) ** 2
        return pretty_format(variance)


class FoldAggregator:
    def __init__(self, post_proc):
        self.stats = defaultdict(list)
        self.post_proc = post_proc

    def increment(self, key, val):
        self.stats[key].append(val)

    def get_final(self, key):
        res = self.stats[key]
        return self.post_proc(res)


class MedianAggregator:
    def __init__(self):
        self.stats = defaultdict(list)
        self.num_handler = NumHandler()

    def increment(self, key, val):
        val = self.num_handler.parse(val)
        self.stats[key].append(val)

    def get_final(self, key):
        sorted_vals = sorted(self.stats[key])
        assert len(sorted_vals)
        m = int(len(sorted_vals) / 2)
        if len(sorted_vals) % 2:
            return sorted_vals[m]
        else:
            a = sorted_vals[m - 1]
            b = sorted_vals[m]
            return a if a == b else (a + b) / 2.0


class SubkeyChecker:
    def __init__(self):
        self.subkeys = dict()

    def increment(self, key, subkey):
        old_subkey = self.subkeys.get(key)
        if old_subkey is None:
            self.subkeys[key] = subkey
        elif old_subkey != subkey:
            raise RuntimeError('Unable to group by "{}", different values in output: "{}" and "{}"'.format(key, old_subkey, subkey))

    def get_final(self, key):
        return self.subkeys[key]

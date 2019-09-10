#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import argparse
import random
import unittest
import re
import tempfile
import time
import importlib
import codecs
import io
import subprocess
import json
import shutil
import copy

import rbql
from rbql import rbql_csv
from rbql import csv_utils


#This module must be both python2 and python3 compatible

PY3 = sys.version_info[0] == 3


########################################################################################################
# Below are some generic functions
########################################################################################################


line_separators = ['\n', '\r\n', '\r']


script_dir = os.path.dirname(os.path.abspath(__file__))


def normalize_warnings(warnings):
    # TODO move into a common test lib module e.g. "tests_common.py"
    result = []
    for warning in warnings:
        if warning.find('Number of fields in "input" table is not consistent') != -1:
            result.append('inconsistent input records')
        elif warning.find('Defective double quote escaping') != -1:
            result.append('defective double quote escaping')
        elif warning.find('None values in output were replaced by empty strings') != -1:
            result.append('null values in output were replaced')
        elif warning == 'UTF-8 Byte Order Mark (BOM) was found and skipped in input table':
            result.append('BOM removed from input')
        else:
            assert False, 'unknown warning'
    return result


def calc_file_md5(fname):
    import hashlib
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


polymorphic_unichr = chr if PY3 else unichr


def xrange6(x):
    if PY3:
        return range(x)
    return xrange(x)


def natural_random(low, high):
    if low <= 0 and high >= 0 and random.randint(0, 2) == 0:
        return 0
    k = random.randint(0, 8)
    if k < 2:
        return low + k
    if k > 6:
        return high - 8 + k
    return random.randint(low, high)


def randomly_quote_field(src, delim):
    if src.find('"') != -1 or src.find(delim) != -1 or src.find('\n') != -1 or src.find('\r') != -1 or random.randint(0, 1) == 1:
        spaces_before = ' ' * random.randint(0, 2) if delim != ' ' else ''
        spaces_after = ' ' * random.randint(0, 2) if delim != ' ' else ''
        escaped = src.replace('"', '""')
        return '{}"{}"{}'.format(spaces_before, escaped, spaces_after)
    return src


def simple_join(fields, delim):
    result = delim.join(fields)
    if result.count(delim) + 1 != len(fields):
        raise RuntimeError('Unable to use simple policy')
    return result


def random_whitespace_join(fields):
    result = ' ' * random.randint(0, 5)
    for f in fields:
        result += f + ' ' * random.randint(1, 5)
    return result


def randomly_join_quoted(fields, delim):
    efields = list()
    for field in fields:
        efields.append(randomly_quote_field(field, delim))
    assert csv_utils.unquote_fields(efields) == fields
    return delim.join(efields)


def random_smart_join(fields, delim, policy):
    if policy == 'simple':
        return simple_join(fields, delim)
    elif policy == 'whitespace':
        assert delim == ' '
        return random_whitespace_join(fields)
    elif policy in ['quoted', 'quoted_rfc']:
        assert delim != '"'
        return randomly_join_quoted(fields, delim)
    elif policy == 'monocolumn':
        assert len(fields) == 1
        return fields[0]
    else:
        raise RuntimeError('Unknown policy')


def find_in_table(table, token):
    for row in table:
        for col in row:
            if col.find(token) != -1:
                return True
    return False


def table_to_csv_string_random(table, delim, policy):
    line_separator = random.choice(line_separators)
    result = line_separator.join([random_smart_join(row, delim, policy) for row in table])
    if random.choice([True, False]):
        result += line_separator
    return result


def string_to_randomly_encoded_stream(src_str):
    encoding = random.choice(['utf-8', 'latin-1', None])
    if encoding is None:
        return (io.StringIO(src_str), None)
    return (io.BytesIO(src_str.encode(encoding)), encoding)


def write_and_parse_back(table, encoding, delim, policy):
    writer_stream = io.BytesIO() if encoding is not None else io.StringIO()
    line_separator = random.choice(line_separators)
    writer = rbql_csv.CSVWriter(writer_stream, False, encoding, delim, policy, line_separator)
    writer._write_all(table)
    assert not len(writer.get_warnings())
    writer_stream.seek(0)
    record_iterator = rbql_csv.CSVRecordIterator(writer_stream, True, encoding, delim=delim, policy=policy)
    parsed_table = record_iterator._get_all_records()
    return parsed_table



########################################################################################################
# Below are some ad-hoc functions:
########################################################################################################


def make_random_decoded_binary_csv_entry(min_len, max_len, restricted_chars):
    strlen = random.randint(min_len, max_len)
    char_codes = list(range(256))
    if restricted_chars is None:
        restricted_chars = []
    restricted_chars = [ord(c) for c in restricted_chars]
    char_codes = [i for i in char_codes if i not in restricted_chars]
    data_bytes = list()
    for i in xrange6(strlen):
        data_bytes.append(random.choice(char_codes))
    binary_data = bytes(bytearray(data_bytes))
    decoded_binary = binary_data.decode('latin-1')
    assert decoded_binary.encode('latin-1') == binary_data
    return decoded_binary


def generate_random_decoded_binary_table(max_num_rows, max_num_cols, restricted_chars):
    num_rows = natural_random(1, max_num_rows)
    num_cols = natural_random(1, max_num_cols)
    good_keys = ['Hello', 'Avada Kedavra ', ' ??????', '128', '3q295 fa#(@*$*)', ' abc defg ', 'NR', 'a1', 'a2']
    result = list()
    good_column = random.randint(0, num_cols - 1)
    for r in xrange6(num_rows):
        result.append(list())
        for c in xrange6(num_cols):
            if c == good_column:
                result[-1].append(random.choice(good_keys))
            else:
                result[-1].append(make_random_decoded_binary_csv_entry(0, 20, restricted_chars))
    return result


def make_random_unicode_entry(min_len, max_len, restricted_chars):
    strlen = random.randint(min_len, max_len)
    restricted_codes = [ord(c) for c in restricted_chars]
    # Create a list of unicode characters within the range 0000-D7FF: Basic Multilingual Plane
    result = list()
    while len(result) < strlen:
        v = random.randrange(0xD7FF)
        if v not in restricted_codes:
            result.append(polymorphic_unichr(v))
    return ''.join(result)


def generate_random_unicode_table(max_num_rows, max_num_cols, restricted_chars):
    num_rows = natural_random(1, max_num_rows)
    num_cols = natural_random(1, max_num_cols)
    good_keys = ['Привет!', 'Бабушка', ' ??????', '128', '3q295 fa#(@*$*)', ' abc defg ', 'NR', 'a1', 'a2']
    result = list()
    good_column = random.randint(0, num_cols - 1)
    for r in xrange6(num_rows):
        result.append(list())
        for c in xrange6(num_cols):
            if c == good_column:
                result[-1].append(random.choice(good_keys))
            else:
                result[-1].append(make_random_unicode_entry(0, 20, restricted_chars))
    return result


def make_random_csv_fields_naive(num_fields, max_field_len):
    available = [',', '"', 'a', 'b', 'c', 'd']
    result = list()
    for fn in range(num_fields):
        flen = natural_random(0, max_field_len)
        chosen = list()
        for i in range(flen):
            chosen.append(random.choice(available))
        result.append(''.join(chosen))
    return result



def make_random_csv_records_naive():
    result = list()
    for num_test in xrange6(1000):
        num_fields = random.randint(1, 11)
        max_field_len = 25
        fields = make_random_csv_fields_naive(num_fields, max_field_len)
        csv_line = random_smart_join(fields, ',', 'quoted')
        defective_escaping = random.randint(0, 1)
        if defective_escaping:
            defect_pos = random.randint(0, len(csv_line))
            csv_line = csv_line[:defect_pos] + '"' + csv_line[defect_pos:]
        result.append((fields, csv_line, defective_escaping))
    return result


def normalize_newlines_in_fields(table):
    for row in table:
        for c in xrange6(len(row)):
            row[c] = row[c].replace('\r\n', '\n')
            row[c] = row[c].replace('\r', '\n')



class TestSplitMethods(unittest.TestCase):
    def test_split(self):
        self.assertEqual(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', True)[0], [' aaa', ' " aaa, bbb " ', ' ccc ', ' ddd '])
        self.assertEqual(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', False)[0], [' aaa', ' aaa, bbb ', ' ccc ', ' ddd '])


        test_cases = list()
        test_cases.append(('hello,world', (['hello', 'world'], False)))
        test_cases.append(('hello,"world"', (['hello', 'world'], False)))
        test_cases.append(('"abc"', (['abc'], False)))
        test_cases.append(('abc', (['abc'], False)))
        test_cases.append(('', ([''], False)))
        test_cases.append((',', (['', ''], False)))
        test_cases.append((',,,', (['', '', '', ''], False)))
        test_cases.append((',"",,,', (['', '', '', '', ''], False)))
        test_cases.append(('"","",,,""', (['', '', '', '', ''], False)))
        test_cases.append(('"aaa,bbb",', (['aaa,bbb', ''], False)))
        test_cases.append(('"aaa,bbb",ccc', (['aaa,bbb', 'ccc'], False)))
        test_cases.append(('"aaa,bbb","ccc"', (['aaa,bbb', 'ccc'], False)))
        test_cases.append(('"aaa,bbb","ccc,ddd"', (['aaa,bbb', 'ccc,ddd'], False)))
        test_cases.append((' "aaa,bbb" ,  "ccc,ddd" ', (['aaa,bbb', 'ccc,ddd'], False)))
        test_cases.append(('"aaa,bbb",ccc,ddd', (['aaa,bbb', 'ccc', 'ddd'], False)))
        test_cases.append(('"a"aa" a,bbb",ccc,ddd', (['"a"aa" a', 'bbb"', 'ccc', 'ddd'], True)))
        test_cases.append(('"aa, bb, cc",ccc",ddd', (['aa, bb, cc', 'ccc"', 'ddd'], True)))
        test_cases.append(('hello,world,"', (['hello', 'world', '"'], True)))
        test_cases.append((' aaa, " aaa, bbb " , ccc , ddd ', ([' aaa', ' aaa, bbb ', ' ccc ', ' ddd '], False)))
        test_cases.append((' aaa ,bbb ,ccc , ddd ', ([' aaa ', 'bbb ', 'ccc ', ' ddd '], False)))

        for tc in test_cases:
            src = tc[0]
            expected_dst = tc[1]
            warning_expected = expected_dst[1]
            test_dst = csv_utils.split_quoted_str(tc[0], ',')
            self.assertEqual(expected_dst, test_dst, msg='\nsrc: {}\ntest_dst: {}\nexpected_dst: {}\n'.format(src, test_dst, expected_dst))

            test_dst_preserved = csv_utils.split_quoted_str(tc[0], ',', True)
            self.assertEqual(test_dst[1], test_dst_preserved[1])
            self.assertEqual(','.join(test_dst_preserved[0]), tc[0], 'preserved split failure')
            if not warning_expected:
                self.assertEqual(test_dst[0], csv_utils.unquote_fields(test_dst_preserved[0]))


    def test_unquote(self):
        test_cases = list()
        test_cases.append(('  "hello, ""world"" aa""  " ', 'hello, "world" aa"  '))
        for tc in test_cases:
            src, expected = tc
            test_dst = csv_utils.unquote_field(src)
            self.assertEqual(expected, test_dst)


    def test_split_whitespaces(self):
        test_cases = list()
        test_cases.append(('hello world', (['hello', 'world'], False)))
        test_cases.append(('hello   world', (['hello', 'world'], False)))
        test_cases.append(('   hello   world   ', (['hello', 'world'], False)))
        test_cases.append(('     ', ([], False)))
        test_cases.append(('', ([], False)))
        test_cases.append(('   a   b  c d ', (['a', 'b', 'c', 'd'], False)))

        test_cases.append(('hello world', (['hello', 'world'], True)))
        test_cases.append(('hello   world', (['hello  ', 'world'], True)))
        test_cases.append(('   hello   world   ', (['   hello  ', 'world   '], True)))
        test_cases.append(('     ', ([], True)))
        test_cases.append(('', ([], True)))
        test_cases.append(('   a   b  c d ', (['   a  ', 'b ', 'c', 'd '], True)))

        for tc in test_cases:
            src = tc[0]
            expected_dst, preserve_whitespaces = tc[1]
            test_dst = csv_utils.split_whitespace_separated_str(src, preserve_whitespaces)
            self.assertEqual(test_dst, expected_dst)


    def test_random(self):
        random_records = make_random_csv_records_naive()
        for ir, rec in enumerate(random_records):
            expected_fields = rec[0]
            escaped_entry = rec[1]
            expected_warning = rec[2]
            test_fields, test_warning = csv_utils.split_quoted_str(escaped_entry, ',')
            test_fields_preserved, test_warning_preserved = csv_utils.split_quoted_str(escaped_entry, ',', True)
            self.assertEqual(','.join(test_fields_preserved), escaped_entry)
            self.assertEqual(expected_warning, test_warning)
            self.assertEqual(test_warning_preserved, test_warning)
            self.assertEqual(test_fields, csv_utils.unquote_fields(test_fields_preserved))
            if not expected_warning:
                self.assertEqual(expected_fields, test_fields)



class TestLineSplit(unittest.TestCase):
    def test_split_lines_custom(self):
        test_cases = list()
        test_cases.append(('', []))
        test_cases.append(('hello', ['hello']))
        test_cases.append(('hello\nworld', ['hello', 'world']))
        test_cases.append(('hello\rworld\n', ['hello', 'world']))
        test_cases.append(('hello\r\nworld\rhello world\nhello\n', ['hello', 'world', 'hello world', 'hello']))
        for tc in test_cases:
            src, expected_res = tc
            stream, encoding = string_to_randomly_encoded_stream(src)
            line_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=None, policy=None, chunk_size=6, line_mode=True)
            test_res = line_iterator._get_all_rows()
            self.assertEqual(expected_res, test_res)

    def test_split_chunk_sizes(self):
        source_tokens = ['', 'defghIJKLMN', 'a', 'bc'] + ['\n', '\r\n', '\r']
        for test_case in xrange6(1000):
            num_tokens = random.randint(0, 12)
            chunk_size = random.randint(1, 5) if random.randint(0, 1) else random.randint(1, 100)
            src = ''
            for tnum in xrange6(num_tokens):
                token = random.choice(source_tokens)
                src += token
            stream, encoding = string_to_randomly_encoded_stream(src)
            line_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=None, policy=None, chunk_size=chunk_size, line_mode=True)
            test_res = line_iterator._get_all_rows()
            expected_res = src.splitlines()
            self.assertEqual(expected_res, test_res)


class TestRecordIterator(unittest.TestCase):
    def test_iterator(self):
        for _test_num in xrange6(100):
            table = generate_random_decoded_binary_table(10, 10, ['\r', '\n'])
            delims = ['\t', ',', ';', '|']
            delim = random.choice(delims)
            table_has_delim = find_in_table(table, delim)
            policy = 'quoted' if table_has_delim else random.choice(['quoted', 'simple'])
            csv_data = table_to_csv_string_random(table, delim, policy)
            stream, encoding = string_to_randomly_encoded_stream(csv_data)

            record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=delim, policy=policy)
            parsed_table = record_iterator._get_all_records()
            self.assertEqual(table, parsed_table)

            parsed_table = write_and_parse_back(table, encoding, delim, policy)
            self.assertEqual(table, parsed_table)


    def test_iterator_unicode(self):
        for _test_num in xrange6(100):
            table = generate_random_unicode_table(10, 10, ['\r', '\n'])
            delims = ['\t', ',', ';', '|', 'Д', 'Ф', '\u2063']
            delim = random.choice(delims)
            table_has_delim = find_in_table(table, delim)
            policy = 'quoted' if table_has_delim else random.choice(['quoted', 'simple'])
            csv_data = table_to_csv_string_random(table, delim, policy)
            encoding = 'utf-8'
            stream = io.BytesIO(csv_data.encode(encoding))

            record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=delim, policy=policy)
            parsed_table = record_iterator._get_all_records()
            self.assertEqual(table, parsed_table)

            parsed_table = write_and_parse_back(table, encoding, delim, policy)
            self.assertEqual(table, parsed_table)


    def test_iterator_rfc(self):
        for _test_num in xrange6(100):
            table = generate_random_decoded_binary_table(10, 10, None)
            delims = ['\t', ',', ';', '|']
            delim = random.choice(delims)
            policy = 'quoted_rfc'
            csv_data = table_to_csv_string_random(table, delim, policy)
            normalize_newlines_in_fields(table) # XXX normalizing '\r' -> '\n' because record iterator doesn't preserve original separators
            stream, encoding = string_to_randomly_encoded_stream(csv_data)

            record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=delim, policy=policy)
            parsed_table = record_iterator._get_all_records()
            self.assertEqual(table, parsed_table)

            parsed_table = write_and_parse_back(table, encoding, delim, policy)
            self.assertEqual(table, parsed_table)


    def test_multiline_fields(self):
        data_lines = []
        data_lines.append('foo, bar,aaa')
        data_lines.append('test,"hello, bar", "aaa ')
        data_lines.append('test","hello, bar", "bbb ')
        data_lines.append('foo, bar,aaa')
        data_lines.append('foo, ""bar"",aaa')
        data_lines.append('foo, test","hello, bar", "bbb "')
        data_lines.append('foo, bar,aaa')
        csv_data = '\n'.join(data_lines)
        stream, encoding = string_to_randomly_encoded_stream(csv_data)
        table = [['foo', ' bar', 'aaa'], ['test', 'hello, bar', 'aaa \ntest', 'hello, bar', 'bbb \nfoo, bar,aaa\nfoo, "bar",aaa\nfoo, test', "hello, bar", 'bbb '], ['foo', ' bar', 'aaa']]
        delim = ','
        policy = 'quoted_rfc'

        record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim=delim, policy=policy)
        parsed_table = record_iterator._get_all_records()
        self.assertEqual(table, parsed_table)
        parsed_table = write_and_parse_back(table, encoding, delim, policy)
        self.assertEqual(table, parsed_table)


    def test_multicharacter_separator_parsing(self):
        data_lines = []
        data_lines.append('aaa:=)bbb:=)ccc')
        data_lines.append('aaa :=) bbb :=)ccc ')
        expected_table = [['aaa', 'bbb', 'ccc'], ['aaa ', ' bbb ', 'ccc ']]
        csv_data = '\n'.join(data_lines)
        stream = io.StringIO(csv_data)
        delim = ':=)'
        policy = 'simple'
        encoding = None
        record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim, policy)
        parsed_table = record_iterator._get_all_records()
        self.assertEqual(expected_table, parsed_table)

        parsed_table = write_and_parse_back(expected_table, encoding, delim, policy)
        self.assertEqual(expected_table, parsed_table)


    def test_whitespace_separated_parsing(self):
        data_lines = []
        data_lines.append('hello world')
        data_lines.append('   hello   world  ')
        data_lines.append('hello   world  ')
        data_lines.append('  hello   ')
        data_lines.append('  hello   world')
        expected_table = [['hello', 'world'], ['hello', 'world'], ['hello', 'world'], ['hello'], ['hello', 'world']]
        csv_data = '\n'.join(data_lines)
        stream = io.StringIO(csv_data)
        delim = ' '
        policy = 'whitespace'
        encoding = None
        record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim, policy)
        parsed_table = record_iterator._get_all_records()
        self.assertEqual(expected_table, parsed_table)

        parsed_table = write_and_parse_back(expected_table, encoding, delim, policy)
        self.assertEqual(expected_table, parsed_table)


    def test_monocolumn_separated_parsing(self):
        for i in xrange6(10):
            self.maxDiff = None
            table = list()
            num_rows = random.randint(1, 30)
            for irow in xrange6(num_rows):
                min_len = 0 if irow + 1 < num_rows else 1
                table.append([make_random_decoded_binary_csv_entry(min_len, 20, restricted_chars=['\r', '\n'])])
            csv_data = table_to_csv_string_random(table, None, 'monocolumn')
            stream = io.StringIO(csv_data)
            delim = None
            policy = 'monocolumn'
            encoding = None
            record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim, policy)
            parsed_table = record_iterator._get_all_records()
            self.assertEqual(table, parsed_table)

            parsed_table = write_and_parse_back(table, encoding, delim, policy)
            self.assertEqual(table, parsed_table)


    def test_monocolumn_write_failure(self):
        encoding = None
        writer_stream =  io.StringIO()
        delim = None
        policy = 'monocolumn'
        table = [["this will not", "work"], ["as monocolumn", "table"]]
        writer = rbql_csv.CSVWriter(writer_stream, True, encoding, delim, policy, '\n')
        with self.assertRaises(Exception) as cm:
            writer._write_all(table)
        e = cm.exception
        self.assertTrue(str(e).find('some records have more than one field') != -1)


    def test_output_warnings(self):
        encoding = None
        writer_stream = io.StringIO()
        delim = ','
        policy = 'simple'
        table = [["hello,world", None], ["hello", "world"]]
        writer = rbql_csv.CSVWriter(writer_stream, False, encoding, delim, policy, '\n')
        writer._write_all(table)
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = 'hello,world,\nhello,world\n'
        self.assertEqual(expected_data, actual_data)
        actual_warnings = writer.get_warnings()
        expected_warnings = ['None values in output were replaced by empty strings', 'Some output fields contain separator']
        self.assertEqual(expected_warnings, actual_warnings)


    def test_utf_decoding_errors(self):
        table = [['hello', u'\x80\x81\xffThis unicode string encoded as latin-1 is not a valid utf-8\xaa\xbb\xcc'], ['hello', 'world']]
        delim = ','
        policy = 'simple'
        encoding = 'latin-1'
        csv_data = table_to_csv_string_random(table, delim, policy)
        stream = io.BytesIO(csv_data.encode('latin-1'))
        record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim, policy)
        parsed_table = record_iterator._get_all_records()
        self.assertEqual(table, parsed_table)

        parsed_table = write_and_parse_back(table, encoding, delim, policy)
        self.assertEqual(table, parsed_table)

        stream = io.BytesIO(csv_data.encode('latin-1'))
        with self.assertRaises(Exception) as cm:
            record_iterator = rbql_csv.CSVRecordIterator(stream, True, 'utf-8', delim=delim, policy=policy)
            parsed_table = record_iterator._get_all_records()
        e = cm.exception
        self.assertTrue(str(e).find('Unable to decode input table as UTF-8') != -1)


    def test_bom_warning(self):
        table = list()
        table.append([u'\xef\xbb\xbfcde', '1234'])
        table.append(['abc', '1234'])
        table.append(['abc', '1234'])
        table.append(['efg', '100'])
        table.append(['abc', '100'])
        table.append(['cde', '12999'])
        table.append(['aaa', '2000'])
        table.append(['abc', '100'])
        delim = ','
        policy = 'simple'
        encoding = 'latin-1'
        csv_data = table_to_csv_string_random(table, delim, policy)
        stream = io.BytesIO(csv_data.encode('latin-1'))
        record_iterator = rbql_csv.CSVRecordIterator(stream, True, encoding, delim, policy)
        parsed_table = record_iterator._get_all_records()
        expected_warnings = ['UTF-8 Byte Order Mark (BOM) was found and skipped in input table']
        actual_warnings = record_iterator.get_warnings()
        self.assertEqual(expected_warnings, actual_warnings)
        expected_table = copy.deepcopy(table)
        expected_table[0][0] = 'cde'
        self.assertEqual(expected_table, parsed_table)



class TestRBQLWithCSV(unittest.TestCase):

    def process_test_case(self, tmp_tests_dir, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            return
        input_table_path = test_case['input_table_path']
        query = query.replace('###UT_TESTS_DIR###', script_dir)
        input_table_path = os.path.join(script_dir, input_table_path)
        expected_output_table_path = test_case.get('expected_output_table_path', None)
        if expected_output_table_path is not None:
            expected_output_table_path = os.path.join(script_dir, expected_output_table_path)
            expected_md5 = calc_file_md5(expected_output_table_path)
            output_file_name = os.path.basename(expected_output_table_path)
            actual_output_table_path = os.path.join(tmp_tests_dir, output_file_name) 
        else:
            actual_output_table_path = os.path.join(tmp_tests_dir, 'expected_empty_file') 

        expected_error = test_case.get('expected_error', None)
        expected_warnings = test_case.get('expected_warnings', [])
        delim = test_case['csv_separator']
        policy = test_case['csv_policy']
        encoding = test_case['csv_encoding']
        output_format = test_case.get('output_format', 'input')

        out_delim, out_policy = (delim, policy) if output_format == 'input' else rbql_csv.interpret_named_csv_format(output_format)
        error_info, warnings = rbql_csv.csv_run(query, input_table_path, delim, policy, actual_output_table_path, out_delim, out_policy, encoding)

        self.assertTrue((expected_error is not None) == (error_info is not None), 'Inside json test: {}. Expected error: {}, error_info: {}'.format(test_name, expected_error, error_info))
        if expected_error is not None:
            self.assertTrue(error_info['message'].find(expected_error) != -1, 'Inside json test: {}'.format(test_name))
        else:
            actual_md5 = calc_file_md5(actual_output_table_path)
            self.assertTrue(expected_md5 == actual_md5, 'md5 missmatch. Expected table: {}, Actual table: {}'.format(expected_output_table_path, actual_output_table_path))

        warnings = sorted(normalize_warnings(warnings))
        expected_warnings = sorted(expected_warnings)
        self.assertEqual(expected_warnings, warnings, 'Inside json test: {}'.format(test_name))



    def test_json_scenarios(self):
        tests_file = os.path.join(script_dir, 'csv_unit_tests.json')
        tmp_dir = tempfile.gettempdir()
        tmp_tests_dir = 'rbql_csv_unit_tests_dir_{}_{}'.format(time.time(), random.randint(1, 100000000)).replace('.', '_')
        tmp_tests_dir = os.path.join(tmp_dir, tmp_tests_dir)
        os.mkdir(tmp_tests_dir)
        with open(tests_file) as f:
            tests = json.loads(f.read())
            for test in tests:
                self.process_test_case(tmp_tests_dir, test)
        shutil.rmtree(tmp_tests_dir)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--create_random_csv_table', metavar='FILE', help='create random csv table')
    parser.add_argument('--create_big_csv_table', metavar='FILE', help='create random csv table for speed test')
    args = parser.parse_args()

    if args.create_random_csv_table is not None:
        dst_path = args.create_random_csv_table
        random_records = make_random_csv_records_naive()
        with open(dst_path, 'w') as dst:
            for rec in random_records:
                canonic_fields = rec[0]
                escaped_entry = rec[1]
                canonic_warning = rec[2]
                dst.write('{}\t{}\t{}\n'.format(escaped_entry, canonic_warning, ';'.join(canonic_fields)))
        return


    if args.create_big_csv_table is not None:
        dst_path = args.create_big_csv_table
        num_rows = 300 * 1000
        with open(dst_path, 'w') as dst:
            for nr in xrange6(num_rows):
                price = str(random.randint(10, 20))
                item = random.choice(['parsley', 'sage', 'rosemary', 'thyme'])
                csv_line = random_smart_join([price, item], ',', 'quoted')
                dst.write(csv_line + '\n')



if __name__ == '__main__':
    main()

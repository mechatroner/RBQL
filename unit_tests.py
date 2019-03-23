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

import rbql
from rbql import csv_utils

#This module must be both python2 and python3 compatible

PY3 = sys.version_info[0] == 3


########################################################################################################
# Below are some generic functions
########################################################################################################

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
    if src.find('"') != -1 or src.find(delim) != -1 or random.randint(0, 1) == 1:
        spaces_before = ' ' * random.randint(0, 2) if delim != ' ' else ''
        spaces_after = ' ' * random.randint(0, 2) if delim != ' ' else ''
        escaped = src.replace('"', '""')
        escaped = '{}"{}"{}'.format(spaces_before, escaped, spaces_after)
        return escaped
    return src


def simple_join(fields, delim):
    result = delim.join(fields)
    if result.count(delim) + 1 != len(fields):
        raise RuntimeError('Unable to use simple policy')
    return result


def whitespace_join(fields):
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
        return whitespace_join(fields)
    elif policy == 'quoted':
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
    line_separators = ['\n', '\r\n', '\r']
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


########################################################################################################
# Below are some ad-hoc functions:
########################################################################################################


def make_random_decoded_binary_csv_entry(min_len, max_len, restricted_chars):
    strlen = random.randint(min_len, max_len)
    char_codes = list(range(256))
    restricted_chars = [ord(c) for c in restricted_chars]
    char_codes = [i for i in char_codes if i not in restricted_chars]
    data_bytes = list()
    for i in xrange6(strlen):
        data_bytes.append(random.choice(char_codes))
    binary_data = bytes(bytearray(data_bytes))
    decoded_binary = binary_data.decode('latin-1')
    assert decoded_binary.encode('latin-1') == binary_data
    return decoded_binary


def generate_random_decoded_binary_table(max_num_rows, max_num_cols):
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
                result[-1].append(make_random_decoded_binary_csv_entry(0, 20, restricted_chars=['\r', '\n']))
    return result


class TestSplitMethods(unittest.TestCase):

    def test_split(self):
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
        for tc in test_cases:
            src = tc[0]
            canonic_dst = tc[1]
            warning_expected = canonic_dst[1]
            test_dst = csv_utils.split_quoted_str(tc[0], ',')
            self.assertEqual(canonic_dst, test_dst, msg='\nsrc: {}\ntest_dst: {}\ncanonic_dst: {}\n'.format(src, test_dst, canonic_dst))

            test_dst_preserved = csv_utils.split_quoted_str(tc[0], ',', True)
            self.assertEqual(test_dst[1], test_dst_preserved[1])
            self.assertEqual(','.join(test_dst_preserved[0]), tc[0], 'preserved split failure')
            if not warning_expected:
                self.assertEqual(test_dst[0], csv_utils.unquote_fields(test_dst_preserved[0]))


    def test_unquote(self):
        test_cases = list()
        test_cases.append(('  "hello, ""world"" aa""  " ', 'hello, "world" aa"  '))
        for tc in test_cases:
            src, canonic = tc
            test_dst = csv_utils.unquote_field(src)
            self.assertEqual(canonic, test_dst)


    def test_split_whitespaces(self):
        test_cases = list()
        test_cases.append(('hello world', (['hello', 'world'], False)))
        test_cases.append(('hello   world', (['hello', 'world'], False)))
        test_cases.append(('   hello   world   ', (['hello', 'world'], False)))
        test_cases.append(('     ', ([], False)))
        test_cases.append(('', ([], False)))
        test_cases.append(('   a   b  c d ', (['a', 'b', 'c', 'd'], False)))

        test_cases.append(('hello world', (['hello ', 'world'], True)))
        test_cases.append(('hello   world', (['hello   ', 'world'], True)))
        test_cases.append(('   hello   world   ', (['   hello   ', 'world   '], True)))
        test_cases.append(('     ', ([], True)))
        test_cases.append(('', ([], True)))
        test_cases.append(('   a   b  c d ', (['   a   ', 'b  ', 'c ', 'd '], True)))

        for tc in test_cases:
            src = tc[0]
            canonic_dst, preserve_whitespaces = tc[1]
            test_dst = csv_utils.split_whitespace_separated_str(src, preserve_whitespaces)
            self.assertEqual(test_dst, canonic_dst)


    def make_random_csv_fields_naive(self, num_fields, max_field_len):
        available = [',', '"', 'a', 'b', 'c', 'd']
        result = list()
        for fn in range(num_fields):
            flen = natural_random(0, max_field_len)
            chosen = list()
            for i in range(flen):
                chosen.append(random.choice(available))
            result.append(''.join(chosen))
        return result



    def make_random_csv_records_naive(self):
        result = list()
        for num_test in xrange6(1000):
            num_fields = random.randint(1, 11)
            max_field_len = 25
            fields = self.make_random_csv_fields_naive(num_fields, max_field_len)
            csv_line = random_smart_join(fields, ',', 'quoted')
            defective_escaping = random.randint(0, 1)
            if defective_escaping:
                defect_pos = random.randint(0, len(csv_line))
                csv_line = csv_line[:defect_pos] + '"' + csv_line[defect_pos:]
            result.append((fields, csv_line, defective_escaping))
        return result



    def test_random(self):
        random_records = self.make_random_csv_records_naive()
        for ir, rec in enumerate(random_records):
            canonic_fields = rec[0]
            escaped_entry = rec[1]
            canonic_warning = rec[2]
            test_fields, test_warning = csv_utils.split_quoted_str(escaped_entry, ',')
            test_fields_preserved, test_warning_preserved = csv_utils.split_quoted_str(escaped_entry, ',', True)
            self.assertEqual(','.join(test_fields_preserved), escaped_entry)
            self.assertEqual(canonic_warning, test_warning)
            self.assertEqual(test_warning_preserved, test_warning)
            self.assertEqual(test_fields, csv_utils.unquote_fields(test_fields_preserved))
            if not canonic_warning:
                self.assertEqual(canonic_fields, test_fields)



class TestLineSplit(unittest.TestCase):

    def test_split_custom(self):
        test_cases = list()
        test_cases.append(('', []))
        test_cases.append(('hello', ['hello']))
        test_cases.append(('hello\nworld', ['hello', 'world']))
        test_cases.append(('hello\rworld\n', ['hello', 'world']))
        test_cases.append(('hello\r\nworld\r', ['hello', 'world']))
        for tc in test_cases:
            src, canonic_res = tc
            stream, encoding = string_to_randomly_encoded_stream(src)
            line_iterator = csv_utils.CSVRecordIterator(stream, encoding, delim=None, policy=None, chunk_size=6)
            test_res = line_iterator._get_all_rows()
            self.assertEqual(canonic_res, test_res)

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
            line_iterator = csv_utils.CSVRecordIterator(stream, encoding, delim=None, policy=None, chunk_size=chunk_size)
            test_res = line_iterator._get_all_rows()
            canonic_res = src.splitlines()
            self.assertEqual(canonic_res, test_res)


class TestRecordIterator(unittest.TestCase):
    def test_iterator(self):
        for _test_num in xrange6(100):
            table = generate_random_decoded_binary_table(10, 10)
            delims = ['\t', ',', ';', '|']
            delim = random.choice(delims)
            table_has_delim = find_in_table(table, delim)
            policy = 'quoted' if table_has_delim else random.choice(['quoted', 'simple'])
            csv_string = table_to_csv_string_random(table, delim, policy)
            stream, encoding = string_to_randomly_encoded_stream(csv_string)
            record_iterator = csv_utils.CSVRecordIterator(stream, encoding, delim=delim, policy=policy)
            parsed_table = record_iterator._get_all_records()
            self.assertEqual(table, parsed_table)



class TestRBQLQueryParsing(unittest.TestCase):

    def test_comment_strip(self):
        a = ''' # a comment  '''
        a_strp = rbql.strip_py_comments(a)
        self.assertEqual(a_strp, '')


    def test_literals_replacement(self):
        #TODO generate some random examples: Generate some strings randomly and then parse them
        test_cases = list()
        test_cases.append((r'Select 100 order by a1', []))
        test_cases.append((r'Select "hello" order by a1', ['"hello"']))
        test_cases.append((r"Select 'hello', 100 order by a1 desc", ["'hello'"]))
        test_cases.append((r'Select "hello", *, "world" 100 order by a1 desc', ['"hello"', '"world"']))
        test_cases.append((r'Select "hello", "world", "hello \" world", "hello \\\" world", "hello \\\\\\\" world" order by "world"', ['"hello"', '"world"', r'"hello \" world"', r'"hello \\\" world"', r'"hello \\\\\\\" world"', '"world"']))

        for tc in test_cases:
            format_expression, string_literals = rbql.separate_string_literals_py(tc[0])
            canonic_literals = tc[1]
            self.assertEqual(canonic_literals, string_literals)
            self.assertEqual(tc[0], rbql.combine_string_literals(format_expression, string_literals))


    def test_separate_actions(self):
        query = 'select top   100 *, a2, a3 inner  join /path/to/the/file.tsv on a1 == b3 where a4 == "hello" and int(b3) == 100 order by int(a7) desc '
        canonic_res = {'JOIN': {'text': '/path/to/the/file.tsv on a1 == b3', 'join_subtype': rbql.INNER_JOIN}, 'SELECT': {'text': '*, a2, a3', 'top': 100}, 'WHERE': {'text': 'a4 == "hello" and int(b3) == 100'}, 'ORDER BY': {'text': 'int(a7)', 'reverse': True}}
        test_res = rbql.separate_actions(query)
        assert test_res == canonic_res


    def test_except_parsing(self):
        except_part = '  a1,a2,a3, a4,a5, a6 ,   a7  ,a8'
        self.assertEqual('select_except(afields, [0,1,2,3,4,5,6,7])', rbql.translate_except_expression(except_part))

        except_part = 'a1 ,  a2,a3, a4,a5, a6 ,   a7  , a8  '
        self.assertEqual('select_except(afields, [0,1,2,3,4,5,6,7])', rbql.translate_except_expression(except_part))

        except_part = 'a1'
        self.assertEqual('select_except(afields, [0])', rbql.translate_except_expression(except_part))


    def test_join_parsing(self):
        join_part = '/path/to/the/file.tsv on a1 == b3'
        self.assertEqual(('/path/to/the/file.tsv', 'safe_join_get(afields, 1)', 2), rbql.parse_join_expression(join_part))

        join_part = ' file.tsv on b20== a12  '
        self.assertEqual(('file.tsv', 'safe_join_get(afields, 12)', 19), rbql.parse_join_expression(join_part))

        join_part = '/path/to/the/file.tsv on a1==a12  '
        with self.assertRaises(Exception) as cm:
            rbql.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        join_part = ' Bon b1 == a12 '
        with self.assertRaises(Exception) as cm:
            rbql.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)


    def test_update_translation(self):
        rbql_src = '  a1 =  a2  + b3, a2=a4  if b3 == a2 else a8, a8=   100, a30  =200/3 + 1  '
        test_dst = rbql.translate_update_expression(rbql_src, '    ')
        canonic_dst = list()
        canonic_dst.append('safe_set(up_fields, 1,  a2  + b3)')
        canonic_dst.append('    safe_set(up_fields, 2,a4  if b3 == a2 else a8)')
        canonic_dst.append('    safe_set(up_fields, 8,   100)')
        canonic_dst.append('    safe_set(up_fields, 30,200/3 + 1)')
        canonic_dst = '\n'.join(canonic_dst)
        self.assertEqual(canonic_dst, test_dst)


    def test_select_translation(self):
        rbql_src = ' *, a1,  a2,a1,*,*,b1, * ,   * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

        rbql_src = ' *, a1,  a2,a1,*,*,*,b1, * ,   * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

        rbql_src = ' * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

        rbql_src = ' *,* '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + [] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

        rbql_src = ' *,*, * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

        rbql_src = ' *,*, * , *'
        test_dst = rbql.translate_select_expression_py(rbql_src)
        canonic_dst = '[] + star_fields + [] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(canonic_dst, test_dst)

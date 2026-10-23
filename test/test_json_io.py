#!/usr/bin/env python

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

script_dir = os.path.dirname(os.path.abspath(__file__))
# Use insert instead of append to make sure that we are using local rbql here.
sys.path.insert(0, os.path.join(os.path.dirname(script_dir), 'rbql-py'))

import rbql
from rbql import rbql_json


def normalize_warnings(warnings):
    result = []
    for warning in warnings:
        if warning == 'UTF-8 Byte Order Mark (BOM) was found and skipped in input table':
            result.append('BOM removed from input')
        else:
            result.append(warning)
    return result


def calc_file_md5(fname):
    import hashlib
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


class TestBalancingIterator(unittest.TestCase):
    def test_simple(self):
        data = '{"hello": "world"}'
        expected = json.loads(data)
        it = rbql_json.BalancingIterator()
        it.add_fragment(data)
        self.assertFalse(it.empty())
        json_obj = it.get_current_object()
        self.assertEqual(expected, json_obj)
        # Test double read:
        json_obj = it.get_current_object()
        self.assertEqual(expected, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_multi_add(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment('{"hello": ')
        self.assertTrue(it.empty())
        self.assertTrue(it.has_trailing_data())
        it.add_fragment('"world"}')
        self.assertFalse(it.empty())
        self.assertFalse(it.has_trailing_data())
        json_obj = it.get_current_object()
        self.assertEqual({"hello": "world"}, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_trailing_spaces(self):
        data = '{"hello": "world"} \n  \t '
        it = rbql_json.BalancingIterator()
        it.add_fragment(data)
        self.assertFalse(it.empty())
        json_obj = it.get_current_object()
        self.assertEqual({"hello": "world"}, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_trailing_incomplete_object(self):
        data = '{"hello": "world"} \n  \t '
        it = rbql_json.BalancingIterator()
        it.add_fragment(data)
        self.assertFalse(it.empty())
        json_obj = it.get_current_object()
        self.assertEqual({"hello": "world"}, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_trailing_incomplete_object(self):
        data = '{"hello": "world"} \n  \t {"hello": '
        it = rbql_json.BalancingIterator()
        it.add_fragment(data)
        self.assertFalse(it.empty())
        json_obj = it.get_current_object()
        self.assertEqual({"hello": "world"}, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertTrue(it.has_trailing_data())

    def test_trailing_incomplete_string(self):
        data = '{"hello": "world"} \n  \t "hello'
        it = rbql_json.BalancingIterator()
        it.add_fragment(data)
        self.assertFalse(it.empty())
        json_obj = it.get_current_object()
        self.assertEqual({"hello": "world"}, json_obj)
        it.advance()
        self.assertTrue(it.empty())
        self.assertTrue(it.has_trailing_data())

    def test_unbalanced_curly(self):
        data = '{"hello": "world"} }'
        it = rbql_json.BalancingIterator()
        with self.assertRaises(Exception) as cm:
            it.add_fragment(data)
        e = cm.exception
        self.assertEqual(str(e), 'Found an unbalanced closing curly bracket `}` after parsing the first 1 records')

    def test_unbalanced_square(self):
        data = '{"hello": "world"} ]'
        it = rbql_json.BalancingIterator()
        with self.assertRaises(Exception) as cm:
            it.add_fragment(data)
        e = cm.exception
        self.assertEqual(str(e), 'Found an unbalanced closing square bracket `]` after parsing the first 1 records')

    def test_integers_with_explicity_finish_without_trailing_space(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment('1 2 3 4 5')
        it.finish()
        for i in range(5):
            self.assertEqual(it.get_current_object() - 1, i)
            it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_string_backslash(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment('"hello\\')
        self.assertTrue(it.empty())
        self.assertTrue(it.has_trailing_data())
        it.add_fragment('"world"')
        self.assertFalse(it.empty())
        self.assertFalse(it.has_trailing_data())
        self.assertEqual(r'hello"world', it.get_current_object())
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_double_backslash_double_quote(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment('"hello\\')
        self.assertTrue(it.empty())
        self.assertTrue(it.has_trailing_data())
        it.add_fragment('\\"')
        self.assertFalse(it.empty())
        self.assertFalse(it.has_trailing_data())
        self.assertEqual('hello\\', it.get_current_object())
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_strings(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment('"hello""world"')
        self.assertEqual(it.get_current_object(), 'hello')
        it.advance()
        self.assertEqual(it.get_current_object(), 'world')
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_garbage_data(self):
        it = rbql_json.BalancingIterator()
        with self.assertRaises(Exception) as cm:
            it.add_fragment('some garbage data')
        e = cm.exception
        self.assertEqual(str(e), 'Failed to deserialize a record after the first 0: Expecting value: line 1 column 1 (char 0)')

    def test_two_backslashes(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment(r'"hello world\\"')
        self.assertEqual(it.get_current_object(), 'hello world\\')
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def test_three_backslashes(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment(r'"hello world\\\" bye!"')
        self.assertEqual(it.get_current_object(), r'hello world\" bye!')
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())

    def bad_backslash_still_works(self):
        it = rbql_json.BalancingIterator()
        it.add_fragment(r'"hello\ \"world"')
        self.assertEqual(it.get_current_object(), r'"hello\ \"world"')
        it.advance()
        self.assertTrue(it.empty())
        self.assertFalse(it.has_trailing_data())


class TestDeduplicateHeadersKeys(unittest.TestCase):
    def test_deduplication_simple(self):
        input_keys = ['a', 'b']
        actual_output_keys, actual_deduplicated_keys = rbql_json.deduplicate_header_keys(input_keys)
        expected_output_keys = ['a', 'b']
        expected_deduplicated_keys = []
        self.assertEqual(expected_output_keys, actual_output_keys)
        self.assertEqual(expected_deduplicated_keys, actual_deduplicated_keys)

        input_keys = ['a', 'a']
        actual_output_keys, actual_deduplicated_keys = rbql_json.deduplicate_header_keys(input_keys)
        expected_output_keys = ['a', 'a_2']
        expected_deduplicated_keys = ['a']
        self.assertEqual(expected_output_keys, actual_output_keys)
        self.assertEqual(expected_deduplicated_keys, actual_deduplicated_keys)

        input_keys = ['a', 'a', 'a_2']
        actual_output_keys, actual_deduplicated_keys = rbql_json.deduplicate_header_keys(input_keys)
        expected_output_keys = ['a', 'a_2', 'a_2_2']
        expected_deduplicated_keys = ['a', 'a_2']
        self.assertEqual(expected_output_keys, actual_output_keys)
        self.assertEqual(expected_deduplicated_keys, actual_deduplicated_keys)

        input_keys = []
        actual_output_keys, actual_deduplicated_keys = rbql_json.deduplicate_header_keys(input_keys)
        expected_output_keys = []
        expected_deduplicated_keys = []
        self.assertEqual(expected_output_keys, actual_output_keys)
        self.assertEqual(expected_deduplicated_keys, actual_deduplicated_keys)

class TestJsonObjectByHeader(unittest.TestCase):
    def test_simple(self):
        header = ['bar']
        fields = ['foo']
        actual_object = rbql_json.get_json_object_to_write(header, fields)
        expected_object = 'foo'
        self.assertEqual(expected_object, actual_object)

        header = ['bar']
        fields = ['foo', 'bar']
        with self.assertRaises(Exception) as cm:
            actual_object = rbql_json.get_json_object_to_write(header, fields)
        e = cm.exception
        self.assertEqual(str(e), 'Inconsistent number of columns in output header and the current record: 1 != 2')

        header = ['h1', 'h2']
        fields = ['foo', 'bar']
        actual_object = rbql_json.get_json_object_to_write(header, fields)
        expected_object = {'h1': 'foo', 'h2': 'bar'}
        self.assertEqual(expected_object, actual_object)

        header = []
        fields = ['foo', 'bar']
        actual_object = rbql_json.get_json_object_to_write(header, fields)
        expected_object = {'col1': 'foo', 'col2': 'bar'}
        self.assertEqual(expected_object, actual_object)
        


class TestGetDeduplicationWarnings(unittest.TestCase):
    def test_lines_writer(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, pretty_indent=None, wrap_in_json_array=False)
        writer.set_header(['a', 'a'])
        actual_warnings = writer.get_warnings()
        expected_warnings = ['Deduplicated output json keys to avoid data loss: "a"']
        self.assertEqual(expected_warnings, actual_warnings)

    def test_array_writer(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.set_header(['a', 'a'])
        actual_warnings = writer.get_warnings()
        expected_warnings = ['Deduplicated output json keys to avoid data loss: "a"']
        self.assertEqual(expected_warnings, actual_warnings)


class TestRegularStreaming(unittest.TestCase):
    def test_many_chunks(self):
        encoding = None
        vals = list(range(20))
        keys = ['key{}'.format(v) for v in vals]
        json_str = json.dumps(dict(zip(keys, vals)))
        data = '\n'.join([json_str] * 10)
        chunk_size = 8
        input_stream = io.StringIO(data)
        input_iterator = rbql_json.JsonIterator(input_stream, encoding=encoding, chunk_size=chunk_size)
        num_records = 0
        while True:
            r = input_iterator.get_record()
            if r is None:
                break
            num_records += 1
        self.assertEqual(num_records, 10)
        self.assertGreaterEqual(input_iterator.num_chunks_read_internal_stat, len(data) / chunk_size)


class TestUnwrapMode(unittest.TestCase):
    def test_one_record(self):
        input_stream = io.StringIO('["foo"]')
        encoding = None
        input_iterator = rbql_json.JsonIterator(input_stream, unwrap_mode=True, encoding=encoding)
        self.assertEqual(input_iterator.get_record(), ["foo"])
        self.assertEqual(input_iterator.get_record(), None)

    def test_empty_table(self):
        input_stream = io.StringIO('[]')
        encoding = None
        input_iterator = rbql_json.JsonIterator(input_stream, unwrap_mode=True, encoding=encoding)
        self.assertEqual(input_iterator.get_record(), None)
    
    def test_bad_non_json_input(self):
        input_stream = io.StringIO('')
        encoding = None
        input_iterator = rbql_json.JsonIterator(input_stream, unwrap_mode=True, encoding=encoding)
        with self.assertRaises(Exception) as cm:
            while True:
                record = input_iterator.get_record()
                if record is None:
                    break
        e = cm.exception
        self.assertEqual(str(e), 'Closing square bracket `]` at the end is required in unwrap mode')
    
    def test_non_array_object(self):
        input_stream = io.StringIO('{"foo": 2}')
        encoding = None
        input_iterator = rbql_json.JsonIterator(input_stream, unwrap_mode=True, encoding=encoding)
        with self.assertRaises(Exception) as cm:
            while True:
                record = input_iterator.get_record()
                if record is None:
                    break
        e = cm.exception
        self.assertEqual(str(e), 'Input data must be wrapped in JSON array in unwrap mode. Expected `[` got `{` char')


class TestJsonLinesWriter(unittest.TestCase):
    def test_empty_write(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, pretty_indent=None, wrap_in_json_array=False)
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = ''
        self.assertEqual(expected_data, actual_data)

    def test_two_records_single_element(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, pretty_indent=None, wrap_in_json_array=False)
        writer.write([{'foo': 1}])
        writer.write([{'foo': 2}])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '{"foo": 1}\n{"foo": 2}\n'
        self.assertEqual(expected_data, actual_data)


class TestArrayObjectReadWrite(unittest.TestCase):
    def test_empty_write(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '[\n]\n'
        self.assertEqual(expected_data, actual_data)

    def test_write_json_utf_8_stream_encode(self):
        writer_stream = io.BytesIO()
        close_stream_on_finish = False
        encoding = 'utf-8'
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = b'[\n]\n'
        self.assertEqual(expected_data, actual_data)

    def test_two_records_single_element(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.write([{'foo': 1}])
        writer.write([{'foo': 2}])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '[\n{"foo": 1},\n{"foo": 2}\n]\n'
        self.assertEqual(expected_data, actual_data)

    def test_no_array_wrap(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=False)
        writer.write([{'foo': 1}])
        writer.write([{'foo': 2}])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '{"foo": 1}\n{"foo": 2}\n'
        self.assertEqual(expected_data, actual_data)

    def test_pretty_indent(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, pretty_indent=2, wrap_in_json_array=True)
        writer.write([{'foo': 1}])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '[\n{\n  "foo": 1\n}\n]\n'
        self.assertEqual(expected_data, actual_data)

    def test_two_records_two_elements(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.write(['foo', 1])
        writer.write(['bar', 2])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '[\n{"col1": "foo", "col2": 1},\n{"col1": "bar", "col2": 2}\n]\n'
        self.assertEqual(expected_data, actual_data)

    def test_set_header(self):
        writer_stream = io.StringIO()
        close_stream_on_finish = False
        encoding = None
        writer = rbql_json.JsonWriter(writer_stream, close_stream_on_finish, encoding, wrap_in_json_array=True)
        writer.set_header(['name', 'count'])
        writer.write(['foo', 1])
        writer.finish()
        writer_stream.seek(0)
        actual_data = writer_stream.getvalue()
        expected_data = '[\n{"name": "foo", "count": 1}\n]\n'
        self.assertEqual(expected_data, actual_data)


class TestRBQLWithJSON(unittest.TestCase):
    def process_test_case(self, tmp_tests_dir, test_case):
        debug_mode = test_case.get('debug_mode', False)
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            return
        unwrap_array = test_case.get('unwrap_array', False)
        pretty_indent = test_case.get('pretty_indent', None)
        wrap_into_array = test_case.get('wrap_into_array', unwrap_array)
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
        absolute_output_table_path = test_case.get('absolute_output_table_path', None)
        if absolute_output_table_path is not None:
            actual_output_table_path = absolute_output_table_path

        expected_error = test_case.get('expected_error', None) or test_case.get('expected_error_py', None)
        expected_warnings = test_case.get('expected_warnings', [])

        if debug_mode:
            rbql_json.set_debug_mode()
        warnings = []
        error_type, error_msg = None, None
        try:
            rbql_json.query_json(query, input_table_path, actual_output_table_path, warnings, unwrap_array=unwrap_array, wrap_into_array=wrap_into_array, pretty_indent=pretty_indent)
        except Exception as e:
            if debug_mode:
                raise
            error_type, error_msg = rbql.exception_to_error_info(e)

        self.assertTrue((expected_error is not None) == (error_type is not None), 'Inside json test: "{}". Expected error: {}, error_type, error_msg: {}'.format(test_name, expected_error, error_type, error_msg))
        if expected_error is not None:
            self.assertTrue(error_msg.find(expected_error) != -1, 'Inside json test: "{}", Expected error: "{}", Actual error: "{}"'.format(test_name, expected_error, error_msg))
        else:
            actual_md5 = calc_file_md5(actual_output_table_path)
            self.assertTrue(expected_md5 == actual_md5, 'md5 missmatch in test "{}". Compare expected and actual:\nvimdiff {} {}'.format(test_name, expected_output_table_path, actual_output_table_path))

        warnings = sorted(normalize_warnings(warnings))
        expected_warnings = sorted(expected_warnings)
        self.assertEqual(expected_warnings, warnings, 'Inside json test: "{}". Expected warnings: {}, Actual warnings: {}'.format(test_name, expected_warnings, warnings))



    def test_json_scenarios(self):
        tests_file = os.path.join(script_dir, 'json_files_unit_tests.json')
        tmp_dir = tempfile.gettempdir()
        tmp_tests_dir = 'rbql_csv_unit_tests_dir_{}_{}'.format(time.time(), random.randint(1, 100000000)).replace('.', '_')
        tmp_tests_dir = os.path.join(tmp_dir, tmp_tests_dir)
        os.mkdir(tmp_tests_dir)
        with open(tests_file) as f:
            tests = json.loads(f.read())
            filtered_tests = [t for t in tests if t.get('skip_others', False)]
            if len(filtered_tests):
                tests = filtered_tests
            for test in tests:
                self.process_test_case(tmp_tests_dir, test)
        shutil.rmtree(tmp_tests_dir)



if __name__ == '__main__':
    main()

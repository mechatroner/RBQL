# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import os
import sys
import json
import random
import tempfile
import time
import shutil
import pandas
from pandas.testing import assert_frame_equal

import rbql
from rbql import rbql_engine
from rbql import rbql_pandas


python_version = float('{}.{}'.format(sys.version_info[0], sys.version_info[1]))

script_dir = os.path.dirname(os.path.abspath(__file__))


def normalize_warnings(warnings):
    # TODO move into a common test lib module e.g. "tests_common.py"
    result = []
    for warning in warnings:
        if warning.find('Number of fields in "input" table is not consistent') != -1:
            result.append('inconsistent input records')
        else:
            assert False, 'unknown warning'
    return result


def do_randomly_split_replace(query, old_name, new_name):
    # TODO put common test functionality into a lib script.
    query_parts = query.split(old_name)
    result = query_parts[0]
    for i in range(1, len(query_parts)):
        result += old_name if random.choice([True, False]) else new_name
        result += query_parts[i]
    return result


def randomly_replace_column_variable_style(query):
    for i in reversed(range(10)):
        query = do_randomly_split_replace(query, 'a{}'.format(i), 'a[{}]'.format(i))
        query = do_randomly_split_replace(query, 'b{}'.format(i), 'b[{}]'.format(i))
    return query


class TestDataframeBasic(unittest.TestCase):
    def test_basic(self):
        input_df = pandas.DataFrame([['foo', 2], ['bar', 7], ['zorb', 20]], columns=['kind', 'speed'])
        output_warnings = []
        output_df = rbql_pandas.query_dataframe('select * where a1.find("o") != -1 order by a.speed', input_df, output_warnings)
        self.assertEqual(2, len(output_df))
        self.assertEqual([], output_warnings, 2)
        expected_output_df = pandas.DataFrame([['foo', 2], ['zorb', 20]], columns=['kind', 'speed'])
        assert_frame_equal(expected_output_df, output_df)


class TestJsonTables(unittest.TestCase):

    def assertDataframeEqual(self, a, b, msg):
        try:
            assert_frame_equal(a, b)
        except AssertionError as e:
            raise self.failureException(msg) from e

    def setUp(self):
        self.addTypeEqualityFunc(pandas.DataFrame, self.assertDataframeEqual)

    def process_test_case(self, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            if python_version >= 3:
                query = test_case.get('query_python_3', None)
            else:
                query = test_case.get('query_python_2', None)
        debug_mode = test_case.get('debug_mode', False)
        minimal_python_version = float(test_case.get('minimal_python_version', 2.7))
        if python_version < minimal_python_version:
            print('Skipping {}: python version must be at least {}. Interpreter version is {}'.format(test_name, minimal_python_version, python_version))
            return
        randomly_replace_var_names = test_case.get('randomly_replace_var_names', True)
        if query is None:
            self.assertTrue(test_case.get('query_js', None) is not None)
            return # Skip this test
        if randomly_replace_var_names:
            query = randomly_replace_column_variable_style(query)
        input_table = test_case['input_table']
        join_table = test_case.get('join_table', None)
        input_column_names = test_case.get('input_column_names', None)
        join_column_names = test_case.get('join_column_names', None)
        input_df = pandas.DataFrame(input_table, columns=input_column_names)
        join_df = None if join_table is None else pandas.DataFrame(join_table, columns=join_column_names)
        normalize_column_names = test_case.get('normalize_column_names', True)
        user_init_code = test_case.get('python_init_code', '')
        expected_output_header = test_case.get('expected_output_header', None)
        expected_output_table = test_case.get('expected_output_table', None)
        expected_output_df = None if expected_output_table is None else pandas.DataFrame(expected_output_table, columns=expected_output_header)
        expected_error_type = test_case.get('expected_error_type', None)
        expected_error = test_case.get('expected_error', None)
        if expected_error is None:
            expected_error = test_case.get('expected_error_py', None)
        if expected_error is None:
            if python_version >= 3:
                expected_error = test_case.get('expected_error_py_3', None)
            else:
                expected_error = test_case.get('expected_error_py_2', None)
        expected_error_exact = test_case.get('expected_error_exact', False)
        expected_warnings = test_case.get('expected_warnings', [])

        rbql_engine.set_debug_mode(debug_mode)
        warnings = []
        error_type, error_msg = None, None
        try:
            output_df = rbql_pandas.query_dataframe(query, input_df, warnings, join_table, normalize_column_names, user_init_code)
        except Exception as e:
            if debug_mode:
                raise
            error_type, error_msg = rbql.exception_to_error_info(e)

        self.assertTrue((expected_error is not None) == (error_type is not None), 'Inside json test: "{}". Expected error: {}, error_type: {}, error_msg: {}'.format(test_name, expected_error, error_type, error_msg))
        if expected_error_type is not None:
            self.assertTrue(error_type == expected_error_type, 'Inside json test: {}'.format(test_name))
        if expected_error is not None:
            if expected_error_exact:
                self.assertEqual(expected_error, error_msg, 'Inside json test: {}. Expected error: {}, Actual error: {}'.format(test_name, expected_error, error_msg))
            else:
                self.assertTrue(error_msg.find(expected_error) != -1, 'Inside json test: {}'.format(test_name))
        else:
            try:
                self.assertEqual(expected_output_df, output_df)
            except Exception:
                print('\nFailed inside json test: "{}"'.format(test_name))
                raise

            warnings = sorted(normalize_warnings(warnings))
            expected_warnings = sorted(expected_warnings)
            self.assertEqual(expected_warnings, warnings, 'Inside json test: {}. Expected warnings: {}; Actual warnings: {}'.format(test_name, ','.join(expected_warnings), ','.join(warnings)))


    def test_json_tables(self):
        tests_file = os.path.join(script_dir, 'rbql_unit_tests.json')
        with open(tests_file) as f:
            tests = json.loads(f.read())
            filtered_tests = [t for t in tests if t.get('skip_others', False)]
            if len(filtered_tests):
                tests = filtered_tests
            for test in tests:
                self.process_test_case(test)

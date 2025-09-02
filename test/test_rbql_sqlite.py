import unittest
import os
import sys
import json
import random
import tempfile
import time
import shutil
import sqlite3

script_dir = os.path.dirname(os.path.abspath(__file__))
# Use insert instead of append to make sure that we are using local rbql here.
sys.path.insert(0, os.path.join(os.path.dirname(script_dir), 'rbql-py'))

import rbql
from rbql import rbql_engine
from rbql import rbql_sqlite

def calc_file_md5(fname):
    # TODO put into a common test_common.py module
    import hashlib
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def normalize_warnings(warnings):
    # TODO can we get rid of this function? Why do we need to normalize warnings?
    # TODO move into a common test lib module e.g. "tests_common.py"
    result = []
    for warning in warnings:
        if warning.find('Number of fields in "input" table is not consistent') != -1:
            result.append('inconsistent input records')
        elif warning.find('Inconsistent double quote escaping') != -1:
            result.append('inconsistent double quote escaping')
        elif warning.find('None values in output were replaced by empty strings') != -1:
            result.append('null values in output were replaced')
        elif warning == 'UTF-8 Byte Order Mark (BOM) was found and skipped in input table':
            result.append('BOM removed from input')
        else:
            result.append(warning)
    return result


class TestSqliteJsonScenarios(unittest.TestCase):

    def process_test_case(self, tmp_tests_dir, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            query = test_case.get('query_python_3', None)
        debug_mode = test_case.get('debug_mode', False)
        if query is None:
            self.assertTrue(test_case.get('query_js', None) is not None)
            return # Skip this test
        input_db_path = os.path.join(script_dir, test_case['input_db_path'])
        input_table_name = test_case['input_table_name']
        join_table = test_case.get('join_table', None)
        user_init_code = test_case.get('python_init_code', '')
        out_delim = ',' # TODO read from the test_case
        out_policy = 'quoted_rfc' # TODO read from the test_case
        output_encoding = 'utf-8' # TODO read from the test_case
        warnings = []

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
        error_type, error_msg = None, None

        db_connection = None
        try:
            db_connection = sqlite3.connect(input_db_path)
            rbql_sqlite.query_sqlite_to_csv(query, db_connection, input_table_name, actual_output_table_path, out_delim, out_policy, output_encoding, warnings)
        except Exception as e:
            if debug_mode:
                raise
            error_type, error_msg = rbql.exception_to_error_info(e)
        finally:
            db_connection.close()

        self.assertTrue((expected_error is not None) == (error_type is not None), 'Inside json test: "{}". Expected error: {}, error_type, error_msg: {}'.format(test_name, expected_error, error_type, error_msg))
        if expected_error is not None:
            self.assertTrue(error_msg.find(expected_error) != -1, 'Inside json test: "{}", Expected error: "{}", Actual error: "{}"'.format(test_name, expected_error, error_msg))
        else:
            actual_md5 = calc_file_md5(actual_output_table_path)
            self.assertTrue(expected_md5 == actual_md5, 'md5 missmatch. Expected table: {}, Actual table: {}'.format(expected_output_table_path, actual_output_table_path))

        warnings = sorted(normalize_warnings(warnings))
        expected_warnings = sorted(expected_warnings)
        self.assertEqual(expected_warnings, warnings, 'Inside json test: "{}"'.format(test_name))


    def test_json_scenarios(self):
        tests_file = os.path.join(script_dir, 'sqlite_unit_tests.json')
        tmp_dir = tempfile.gettempdir()
        tmp_tests_dir = 'rbql_sqlite_unit_tests_dir_{}_{}'.format(time.time(), random.randint(1, 100000000)).replace('.', '_')
        tmp_tests_dir = os.path.join(tmp_dir, tmp_tests_dir)
        os.mkdir(tmp_tests_dir)
        with open(tests_file) as f:
            tests = json.loads(f.read())
            for test in tests:
                self.process_test_case(tmp_tests_dir, test)
        shutil.rmtree(tmp_tests_dir)

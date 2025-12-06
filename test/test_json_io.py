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



class TestRBQLWithJSON(unittest.TestCase):
    def process_test_case(self, tmp_tests_dir, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            return
        debug_mode = test_case.get('debug_mode', False)
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
            rbql_json.query_json(query, input_table_path, actual_output_table_path, warnings)
        except Exception as e:
            if debug_mode:
                raise
            error_type, error_msg = rbql.exception_to_error_info(e)

        self.assertTrue((expected_error is not None) == (error_type is not None), 'Inside json test: "{}". Expected error: {}, error_type, error_msg: {}'.format(test_name, expected_error, error_type, error_msg))
        if expected_error is not None:
            self.assertTrue(error_msg.find(expected_error) != -1, 'Inside json test: "{}", Expected error: "{}", Actual error: "{}"'.format(test_name, expected_error, error_msg))
        else:
            actual_md5 = calc_file_md5(actual_output_table_path)
            self.assertTrue(expected_md5 == actual_md5, 'md5 missmatch in test "{}". Expected table: {}, Actual table: {}'.format(test_name, expected_output_table_path, actual_output_table_path))

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

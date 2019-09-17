# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import os
import json
import random

import rbql

#This module must be both python2 and python3 compatible


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



class TestRBQLQueryParsing(unittest.TestCase):

    def test_comment_strip(self):
        a = ''' # a comment  '''
        a_strp = rbql.strip_comments(a)
        self.assertEqual(a_strp, '')


    def test_string_literals_separation(self):
        #TODO generate some random examples: Generate some strings randomly and then parse them
        test_cases = list()
        test_cases.append((r'Select 100 order by a1', []))
        test_cases.append((r'Select "hello" order by a1', ['"hello"']))
        test_cases.append((r"Select 'hello', 100 order by a1 desc", ["'hello'"]))
        test_cases.append((r'Select "hello", *, "world" 100 order by a1 desc', ['"hello"', '"world"']))
        test_cases.append((r'Select "hello", "world", "hello \" world", "hello \\\" world", "hello \\\\\\\" world" order by "world"', ['"hello"', '"world"', r'"hello \" world"', r'"hello \\\" world"', r'"hello \\\\\\\" world"', '"world"']))

        for tc in test_cases:
            format_expression, string_literals = rbql.separate_string_literals_py(tc[0])
            expected_literals = tc[1]
            self.assertEqual(expected_literals, string_literals)
            self.assertEqual(tc[0], rbql.combine_string_literals(format_expression, string_literals))


    def test_separate_actions(self):
        query = 'select top   100 *, a2, a3 inner  join /path/to/the/file.tsv on a1 == b3 where a4 == "hello" and int(b3) == 100 order by int(a7) desc '
        expected_res = {'JOIN': {'text': '/path/to/the/file.tsv on a1 == b3', 'join_subtype': rbql.INNER_JOIN}, 'SELECT': {'text': '*, a2, a3', 'top': 100}, 'WHERE': {'text': 'a4 == "hello" and int(b3) == 100'}, 'ORDER BY': {'text': 'int(a7)', 'reverse': True}}
        test_res = rbql.separate_actions(query)
        assert test_res == expected_res


    def test_except_parsing(self):
        except_part = '  a1,a2,a3, a4,a5, a6 ,   a7  ,a8'
        self.assertEqual('select_except(record_a, [0,1,2,3,4,5,6,7])', rbql.translate_except_expression(except_part, {'a1': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a6': 5, 'a7': 6, 'a8': 7}))

        except_part = 'a1 ,  a2,a3, a4,a5, a6 ,   a7  , a8  '
        self.assertEqual('select_except(record_a, [0,1,2,3,4,5,6,7])', rbql.translate_except_expression(except_part, {'a1': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a6': 5, 'a7': 6, 'a8': 7}))

        except_part = 'a1'
        self.assertEqual('select_except(record_a, [0])', rbql.translate_except_expression(except_part, {'a1': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a6': 5, 'a7': 6, 'a8': 7}))


    def test_join_parsing(self):
        join_part = '/path/to/the/file.tsv on a1 == b3'
        self.assertEqual(('/path/to/the/file.tsv', 'a1', 'b3'), rbql.parse_join_expression(join_part))

        join_part = ' file.tsv on b[20]== a.name  '
        self.assertEqual(('file.tsv', 'b[20]', 'a.name'), rbql.parse_join_expression(join_part))

        join_part = ' Bon b1 == a.age '
        with self.assertRaises(Exception) as cm:
            rbql.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        with self.assertRaises(Exception) as cm:
            rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'a1', 'a2')
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        with self.assertRaises(Exception) as cm:
            rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'a1', 'b10')
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        with self.assertRaises(Exception) as cm:
            rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'b1', 'b2')
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)



    def test_update_translation(self):
        rbql_src = '  a1 =  a2  + b3, a2=a4  if b3 == a2 else a8, a8=   100, a30  =200/3 + 1  '
        test_dst = rbql.translate_update_expression(rbql_src, {'a1': 0, 'a2': 1, 'a4': 3, 'a8': 7, 'a30': 29}, '    ')
        expected_dst = list()
        expected_dst.append('safe_set(up_fields, 0,  a2  + b3)')
        expected_dst.append('    safe_set(up_fields, 1,a4  if b3 == a2 else a8)')
        expected_dst.append('    safe_set(up_fields, 7,   100)')
        expected_dst.append('    safe_set(up_fields, 29,200/3 + 1)')
        expected_dst = '\n'.join(expected_dst)
        self.assertEqual(expected_dst, test_dst)


    def test_select_translation(self):
        rbql_src = ' *, a1,  a2,a1,*,*,b1, * ,   * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *, a1,  a2,a1,*,*,*,b1, * ,   * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,* '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,*, * '
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,*, * , *'
        test_dst = rbql.translate_select_expression_py(rbql_src)
        expected_dst = '[] + star_fields + [] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)



def round_floats(src_table):
    for row in src_table:
        for c in range(len(row)):
            if isinstance(row[c], float):
                row[c] = round(row[c], 3)


def do_randomly_split_replace(query, old_name, new_name):
    query_parts = query.split(old_name)
    result = query_parts[0]
    for i in range(1, len(query_parts)):
        if random.choice([True, False]):
            result += old_name
        else:
            result += new_name
        result += query_parts[i]
    return result




def randomly_replace_column_variable_style(query):
    for i in xrange(10):
        query = do_randomly_split_replace(query, 'a{}'.format(i), 'a[{}]'.format(i))
        query = do_randomly_split_replace(query, 'b{}'.format(i), 'b[{}]'.format(i))
    return query



class TestTableRun(unittest.TestCase):
    def test_table_run_simple(self):
        input_table = [('Roosevelt', 1858), ('Napoleon', 1769), ('Confucius', -551)]
        query = 'select a2 // 10, "name " + a1 order by a2'
        expected_output_table = [[-56, 'name Confucius'], [176, 'name Napoleon'], [185, 'name Roosevelt']]
        output_table = []
        error_info, warnings = rbql.table_run(query, input_table, output_table)
        self.assertEqual(error_info, None)
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)


    def test_table_run_simple_join(self):
        input_table = [('Roosevelt', 1858, 'USA'), ('Napoleon', 1769, 'France'), ('Confucius', -551, 'China')]
        join_table = [('China', 1386), ('France', 67), ('USA', 327), ('Russia', 140)]
        query = 'select a2 // 10, b2, "name " + a1 order by a2 JOIN B on a3 == b1'
        expected_output_table = [[-56, 1386, 'name Confucius'], [176, 67, 'name Napoleon'], [185, 327, 'name Roosevelt']]
        output_table = []
        error_info, warnings = rbql.table_run(query, input_table, output_table, join_table)
        self.assertEqual(error_info, None)
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)



class TestJsonTables(unittest.TestCase):

    def process_test_case(self, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        randomly_replace_var_names = test_case.get('randomly_replace_var_names', True)
        if query is None:
            self.assertTrue(test_case.get('query_js', None) is not None)
            return # Skip this test
        if randomly_replace_var_names:
            query = randomly_replace_column_variable_style(query)
        input_table = test_case['input_table']
        join_table = test_case.get('join_table', None)
        user_init_code = test_case.get('python_init_code', '')
        expected_output_table = test_case.get('expected_output_table', None)
        expected_error_type = test_case.get('expected_error_type', None)
        expected_error = test_case.get('expected_error', None)
        if expected_error is None:
            expected_error = test_case.get('expected_error_py', None)
        expected_warnings = test_case.get('expected_warnings', [])
        output_table = []

        error_info, warnings = rbql.table_run(query, input_table, output_table, join_table, user_init_code=user_init_code)

        warnings = sorted(normalize_warnings(warnings))
        expected_warnings = sorted(expected_warnings)
        self.assertEqual(expected_warnings, warnings, 'Inside json test: {}. Expected warnings: {}; Actual warnings: {}'.format(test_name, ','.join(expected_warnings), ','.join(warnings)))
        self.assertTrue((expected_error is not None) == (error_info is not None), 'Inside json test: {}. expected_error: {}, error_info: {}'.format(test_name, expected_error, error_info))
        if expected_error_type is not None:
            self.assertTrue(error_info['type'] == expected_error_type, 'Inside json test: {}'.format(test_name))
        if expected_error is not None:
            self.assertTrue(error_info['message'].find(expected_error) != -1, 'Inside json test: {}'.format(test_name))
        else:
            round_floats(expected_output_table)
            round_floats(output_table)
            self.assertEqual(expected_output_table, output_table, 'Inside json test: {}'.format(test_name))


    def test_json_tables(self):
        tests_file = os.path.join(script_dir, 'rbql_unit_tests.json')
        with open(tests_file) as f:
            tests = json.loads(f.read())
            for test in tests:
                self.process_test_case(test)

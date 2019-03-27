#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import os
import json

import rbql

#This module must be both python2 and python3 compatible


script_dir = os.path.dirname(os.path.abspath(__file__))


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
        expected_dst = list()
        expected_dst.append('safe_set(up_fields, 1,  a2  + b3)')
        expected_dst.append('    safe_set(up_fields, 2,a4  if b3 == a2 else a8)')
        expected_dst.append('    safe_set(up_fields, 8,   100)')
        expected_dst.append('    safe_set(up_fields, 30,200/3 + 1)')
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



class TestJsonTables(unittest.TestCase):
    def process_test_case(self, test_case_path):
        test_case = json.loads(open(test_case_path).read())
        query = test_case['query_python']
        input_table = test_case['input_table']
        expected_output_table = test_case['expected_output_table']
        expected_error = test_case.get('expected_error', None)
        expected_warnings = test_case.get('expected_warnings', [])
        # FIXME compare warnings text
        input_iterator = rbql.TableIterator(input_table)
        output_writer = rbql.TableWriter()
        error_info, warnings = rbql.generic_run(query, input_iterator, output_writer)
        self.assertEqual(len(expected_warnings), len(warnings))
        self.assertTrue((expected_error is not None) == (error_info is not None))
        if expected_error is not None:
            self.assertTrue(error_info['message'].find(expected_error) != -1)
        else:
            output_table = output_writer.table
            for row in output_table:
                for c in range(len(row)):
                    row[c] = str(row[c])
            self.assertEqual(output_table, expected_output_table)


    def test_json_tables(self):
        json_test_cases_dir = os.path.join(script_dir, 'json_test_cases')
        json_file_names = [f for f in os.listdir(json_test_cases_dir) if f.endswith('.json')]
        for name in json_file_names:
            test_case_path = os.path.join(json_test_cases_dir, name)
            self.process_test_case(test_case_path)

# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import os
import sys
import json
import random

script_dir = os.path.dirname(os.path.abspath(__file__))
# Use insert instead of append to make sure that we are using local rbql here.
sys.path.insert(0, os.path.join(os.path.dirname(script_dir), 'rbql-py'))

import rbql
from rbql import rbql_csv
from rbql import rbql_engine

#This module must be both python2 and python3 compatible



vinf = rbql_engine.VariableInfo

python_minor_version = int(sys.version_info[1])


def normalize_warnings(warnings):
    # TODO move into a common test lib module e.g. "tests_common.py"
    result = []
    for warning in warnings:
        if warning.find('Number of fields in "input" table is not consistent') != -1:
            result.append('inconsistent input records')
        else:
            assert False, 'unknown warning'
    return result


def prepare_and_parse_select_expression_to_column_infos(select_part):
    select_expression, string_literals = rbql_engine.separate_string_literals(select_part)
    select_expression_for_ast = rbql_engine.translate_select_expression(select_expression)[1]
    combined_select_expression_for_ast = rbql_engine.combine_string_literals(select_expression_for_ast, string_literals)
    column_infos = rbql_engine.ast_parse_select_expression_to_column_infos(combined_select_expression_for_ast)
    return column_infos


class TestRBQLQueryParsing(unittest.TestCase):

    def test_comment_strip(self):
        a = ''' # a comment  '''
        a_strp = rbql_engine.strip_comments(a)
        self.assertEqual(a_strp, '')


    def test_like_to_regex_conversion(self):
        a = '%hello_world.foo.*bar%'
        b = rbql_engine.like_to_regex(a)
        self.assertEqual(r'^.*hello.world\.foo\.\*bar.*$', b)


    def test_replace_star_count(self):
        src = 'a1, COUNT(*), a2'
        expected_dst = 'a1, COUNT(1), a2'
        dst = rbql_engine.replace_star_count(src)
        self.assertEqual(expected_dst, dst)

        src = 'a1, COUNT(*) as cnt , a2'
        expected_dst = 'a1, COUNT(1) as cnt , a2'
        dst = rbql_engine.replace_star_count(src)
        self.assertEqual(expected_dst, dst)



    def test_column_name_parsing_from_file(self):
        tests_file = os.path.join(script_dir, 'other_test_files', 'select_parts.txt')
        output_path_to_compare_with_js = os.path.join(script_dir, 'python_column_infos.txt')
        with open(tests_file) as src, open(output_path_to_compare_with_js, 'w') as dst:
            for line in src:
                expected_num_columns, select_part = line.rstrip().split('\t')
                expected_num_columns = int(expected_num_columns)
                column_infos = prepare_and_parse_select_expression_to_column_infos(select_part)
                self.assertEqual(expected_num_columns, len(column_infos))
                # Using `separators` param here to get rid of extra spaces and make serialization look just like js version, so we can compare file md5s
                column_infos_str = json.dumps([v._asdict() if v is not None else None for v in column_infos], separators=(',', ':'))
                dst.write(column_infos_str + '\n')


    def test_column_name_parsing(self):
        select_part = 'a1, a[2], a.hello, a["world"], NR, NF, something, foo(something, \'bar\'), "test", 3, 3 + 3, a.hello  as  my_hello , *, NR  AS MY_NR, a.*, b.*, b2'
        column_infos = prepare_and_parse_select_expression_to_column_infos(select_part)
        expected = [rbql_engine.QueryColumnInfo('a', 0, None, False, None), # a1
                    rbql_engine.QueryColumnInfo('a', 1, None, False, None), # a[2]
                    rbql_engine.QueryColumnInfo(None, None, 'hello', False, None), # a.hello
                    rbql_engine.QueryColumnInfo(None, None, 'world', False, None), # a["world"]
                    rbql_engine.QueryColumnInfo(None, None, 'NR', False, None), # NR
                    rbql_engine.QueryColumnInfo(None, None, 'NF', False, None), # NF
                    rbql_engine.QueryColumnInfo(None, None, 'something', False, None), # something 
                    None, # foo(something, 'bar')
                    None, # "test"
                    None, # 3
                    None, # 3 + 3
                    rbql_engine.QueryColumnInfo(None, None, None, False, 'my_hello'), # as my_hello
                    rbql_engine.QueryColumnInfo(None, None, None, True, None), # *
                    rbql_engine.QueryColumnInfo(None, None, None, False, 'MY_NR'), # as MY_NR
                    rbql_engine.QueryColumnInfo('a', None, None, True, None), # a.*
                    rbql_engine.QueryColumnInfo('b', None, None, True, None), # b.*
                    rbql_engine.QueryColumnInfo('b', 1, None, False, None), # b2
                   ]
        self.assertEqual(expected, column_infos) 

        output_header = rbql_engine.select_output_header(['a_foo_1', 'a_foo_2'], ['b_foo_1'], expected);
        expected_header = ['a_foo_1', 'a_foo_2', 'hello', 'world', 'NR', 'NF', 'something', 'col8', 'col9', 'col10', 'col11', 'my_hello', 'a_foo_1', 'a_foo_2', 'b_foo_1', 'MY_NR', 'a_foo_1', 'a_foo_2', 'b_foo_1', 'col20'];
        self.assertEqual(expected_header, output_header);

        select_part = 'a1, a[2], a.hello, a["world"], NR, NF, something, foo(something, \'bar\')), "test", 3, 3 + 3, *, a.*, b.*, b2'
        with self.assertRaises(SyntaxError) as cm:
            prepare_and_parse_select_expression_to_column_infos(select_part)

        select_part = 'a1, a[2], a.hello, a["world"], NR, NF, something, foo(something, \'bar\'), "test", {3, 3 + 3, *, a.*, b.*, b2'
        with self.assertRaises(SyntaxError) as cm:
            prepare_and_parse_select_expression_to_column_infos(select_part)

        select_part = 'a1, a[2], a.hello, a["world"], NR, NF, something, foo(something, \'bar\'), "test", 3, 3 + 3, *, a.*, b.*, b2, a.hello  aas  my_hello , NR  AS MY_NR'
        with self.assertRaises(SyntaxError) as cm:
            # Explanation: "aas" should not parse.
            prepare_and_parse_select_expression_to_column_infos(select_part)

    def test_string_literals_separation(self):
        #TODO generate some random examples: Generate some strings randomly and then parse them
        test_cases = list()
        test_cases.append((r'Select 100 order by a1', []))
        test_cases.append((r'Select "hello" order by a1', ['"hello"']))
        test_cases.append((r"Select 'hello', 100 order by a1 desc", ["'hello'"]))
        test_cases.append((r'Select "hello", *, "world" 100 order by a1 desc', ['"hello"', '"world"']))
        test_cases.append((r'Select "hello", "world", "hello \" world", "hello \\\" world", "hello \\\\\\\" world" order by "world"', ['"hello"', '"world"', r'"hello \" world"', r'"hello \\\" world"', r'"hello \\\\\\\" world"', '"world"']))

        for tc in test_cases:
            format_expression, string_literals = rbql_engine.separate_string_literals(tc[0])
            expected_literals = tc[1]
            self.assertEqual(expected_literals, string_literals)
            self.assertEqual(tc[0], rbql_engine.combine_string_literals(format_expression, string_literals))


    def test_separate_actions(self):
        query = 'select top   100 *, a2, a3 inner  join /path/to/the/file.tsv on a1 == b3 where a4 == "hello" and int(b3) == 100 order by int(a7) desc '
        expected_res = {'JOIN': {'text': '/path/to/the/file.tsv on a1 == b3', 'join_subtype': rbql_engine.INNER_JOIN}, 'SELECT': {'text': '*, a2, a3', 'top': 100}, 'WHERE': {'text': 'a4 == "hello" and int(b3) == 100'}, 'ORDER BY': {'text': 'int(a7)', 'reverse': True}}
        test_res = rbql_engine.separate_actions(rbql_engine.default_statement_groups, query)
        assert test_res == expected_res


    def test_except_parsing(self):
        except_part = '  a1,a2,a3, a4,a5, a[6] ,   a7  ,a8'
        column_infos = {'a1': vinf(True, 0), 'a2': vinf(True, 1), 'a3': vinf(True, 2), 'a4': vinf(True, 3), 'a5': vinf(True, 4), 'a[6]': vinf(True, 5), 'a7': vinf(True, 6), 'a8': vinf(True, 7)}
        input_header = None
        self.assertEqual((None, 'select_except(record_a, [0,1,2,3,4,5,6,7])'), rbql_engine.translate_except_expression(except_part, column_infos, [], input_header))

        except_part = '  a1,a2,a3, a4,a5, a[6]'
        input_header = ['nm1', 'nm2', 'nm3', 'nm4', 'nm5', 'nm6', 'nm7', 'nm8']
        self.assertEqual((['nm7', 'nm8'], 'select_except(record_a, [0,1,2,3,4,5])'), rbql_engine.translate_except_expression(except_part, column_infos, [], input_header))

        except_part = '  a1,a2,a3, a4,a5, a[6]'
        input_header = None
        self.assertEqual((None, 'select_except(record_a, [0,1,2,3,4,5])'), rbql_engine.translate_except_expression(except_part, column_infos, [], input_header))

        except_part = 'a1'
        input_header = ['nm1', 'nm2', 'nm3', 'nm4', 'nm5', 'nm6', 'nm7', 'nm8']
        expected_output_header = ['nm2', 'nm3', 'nm4', 'nm5', 'nm6', 'nm7', 'nm8']
        self.assertEqual((expected_output_header, 'select_except(record_a, [0])'), rbql_engine.translate_except_expression(except_part, column_infos, [], input_header))

        except_part = 'a[1] ,  a2,a3, a4,a5, a6 ,   a[7]  , a8  '
        column_infos = {'a[1]': vinf(True, 0), 'a2': vinf(True, 1), 'a3': vinf(True, 2), 'a4': vinf(True, 3), 'a5': vinf(True, 4), 'a6': vinf(True, 5), 'a[7]': vinf(True, 6), 'a8': vinf(True, 7)}
        input_header = None
        self.assertEqual((None, 'select_except(record_a, [0,1,2,3,4,5,6,7])'), rbql_engine.translate_except_expression(except_part, column_infos, [], input_header))


    def test_join_parsing(self):
        join_part = '/path/to/the/file.tsv on a1 == b3'
        self.assertEqual(('/path/to/the/file.tsv', [('a1', 'b3')]), rbql_engine.parse_join_expression(join_part))

        join_part = ' file.tsv on b[20]== a.name  '
        self.assertEqual(('file.tsv', [('b[20]', 'a.name')]), rbql_engine.parse_join_expression(join_part))

        join_part = ' file.tsv on b[20]== a.name and   a1  ==b3 '
        self.assertEqual(('file.tsv', [('b[20]', 'a.name'), ('a1', 'b3')]), rbql_engine.parse_join_expression(join_part))

        join_part = ' file.tsv on b[20]== a.name and   a1  ==b3 and '
        with self.assertRaises(Exception) as cm:
            rbql_engine.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        join_part = ' file.tsv on b[20]== a.name and   a1  ==b3 + "foo" '
        with self.assertRaises(Exception) as cm:
            rbql_engine.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        join_part = ' Bon b1 == a.age '
        with self.assertRaises(Exception) as cm:
            rbql_engine.parse_join_expression(join_part)
        e = cm.exception
        self.assertTrue(str(e).find('Invalid join syntax') != -1)

        self.assertEqual((['safe_join_get(record_a, 0)'], [1]), rbql_engine.resolve_join_variables({'a1': vinf(True, 0), 'a2': vinf(True, 1)}, {'b1': vinf(True, 0), 'b2': vinf(True, 1)}, [('a1', 'b2')], []))

        with self.assertRaises(Exception) as cm:
            rbql_engine.resolve_join_variables({'a1': vinf(True, 0), 'a2': vinf(True, 1)}, {'b1': vinf(True, 0), 'b2': vinf(True, 1)}, [('a1', 'b.name')], [])
        e = cm.exception
        self.assertTrue(str(e).find('Unable to parse JOIN expression: Join table does not have field "b.name"') != -1)

        with self.assertRaises(Exception) as cm:
            rbql_engine.resolve_join_variables({'a1': vinf(True, 0), 'a2': vinf(True, 1)}, {'b1': vinf(True, 0), 'b2': vinf(True, 1)}, [('a1', 'b["foo bar"]')], [])
        e = cm.exception
        self.assertTrue(str(e).find('Unable to parse JOIN expression: Join table does not have field "b["foo bar"]"') != -1)

        with self.assertRaises(Exception) as cm:
            rbql_engine.resolve_join_variables({'a1': vinf(True, 0), 'a2': vinf(True, 1)}, {'b1': vinf(True, 0), 'b2': vinf(True, 1)}, [('b1', 'b2')], [])
        e = cm.exception
        self.assertTrue(str(e).find('Unable to parse JOIN expression: Input table does not have field "b1"') != -1)



    def test_update_translation(self):
        rbql_src = '  a[1] =  a2  + b3, a2=a4  if b3 == a2 else a8, a8=   ___RBQL_STRING_LITERAL0___, a30  =200/3 + 1  '
        test_dst = rbql_engine.translate_update_expression(rbql_src, {'a[1]': vinf(1, 0), 'a2': vinf(1, 1), 'a4': vinf(1, 3), 'a8': vinf(1, 7), 'a30': vinf(1, 29)}, ['"100 200"'])
        expected_dst = list()
        expected_dst.append('safe_set(up_fields, 0, a2  + b3)')
        expected_dst.append('safe_set(up_fields, 1, a4  if b3 == a2 else a8)')
        expected_dst.append('safe_set(up_fields, 7, "100 200")')
        expected_dst.append('safe_set(up_fields, 29, 200/3 + 1)')
        expected_dst = '\n'.join(expected_dst)
        self.assertEqual(expected_dst, test_dst)


        rbql_src = '  a.name =  a2  + b3, a2=a4  if b3 == a2 else a8, a8=   ___RBQL_STRING_LITERAL0___, a[___RBQL_STRING_LITERAL1___]  =200/3 + 1  '
        test_dst = rbql_engine.translate_update_expression(rbql_src, {'a.name': vinf(1, 0), 'a2': vinf(1, 1), 'a4': vinf(1, 3), 'a8': vinf(1, 7), 'a["foo bar"]': vinf(1, 29), 'a["not used = should not fail"]': vinf(0, 32)}, ['"100 200"', '"foo bar"'])
        expected_dst = list()
        expected_dst.append('safe_set(up_fields, 0, a2  + b3)')
        expected_dst.append('safe_set(up_fields, 1, a4  if b3 == a2 else a8)')
        expected_dst.append('safe_set(up_fields, 7, "100 200")')
        expected_dst.append('safe_set(up_fields, 29, 200/3 + 1)')
        self.assertEqual(expected_dst, test_dst.split('\n'))


        rbql_src = '  a.name =  a2  + b3, a[___RBQL_STRING_LITERAL1___]=a4  if b3 == a2 else a8, a8=   ___RBQL_STRING_LITERAL0___, a[___RBQL_STRING_LITERAL2___]  =200/3 + 1  '
        test_dst = rbql_engine.translate_update_expression(rbql_src, {'a.name': vinf(1, 0), 'a[\'a.foo = 100, a2 = a3, a["foobar"] = 10 \']': vinf(0, 1), 'a4': vinf(1, 3), 'a8': vinf(1, 7), 'a["foo bar"]': vinf(1, 29), 'a["not used = should not fail"]': vinf(0, 32)}, ['"100 200"', '\'a.foo = 100, a2 = a3, a["foobar"] = 10 \'', '"foo bar"'])
        expected_dst = list()
        expected_dst.append('safe_set(up_fields, 0, a2  + b3)')
        expected_dst.append('safe_set(up_fields, 1, a4  if b3 == a2 else a8)')
        expected_dst.append('safe_set(up_fields, 7, "100 200")')
        expected_dst.append('safe_set(up_fields, 29, 200/3 + 1)')
        expected_dst = '\n'.join(expected_dst)
        self.assertEqual(expected_dst, test_dst)


        rbql_src = '  "this will fail", a2=a4  if b3 == a2 else a8, a8=   ___RBQL_STRING_LITERAL0___, a[___RBQL_STRING_LITERAL1___]  =200/3 + 1  '
        with self.assertRaises(Exception) as cm:
            test_dst = rbql_engine.translate_update_expression(rbql_src, {'a.name': vinf(1, 0), 'a2': vinf(1, 1), 'a4': vinf(1, 3), 'a8': vinf(1, 7), 'a["foo bar"]': vinf(1, 29)}, ['"100 200"', '"foo bar"'])
        e = cm.exception
        self.assertEqual(str(e), '''Unable to parse "UPDATE" expression: the expression must start with assignment, but ""this will fail", a2" does not look like an assignable field name''')

        rbql_src = 'a.mysterious_field=a4  if b3 == a2 else a8, a8=   ___RBQL_STRING_LITERAL0___, a[___RBQL_STRING_LITERAL1___]  =200/3 + 1  '
        with self.assertRaises(Exception) as cm:
            test_dst = rbql_engine.translate_update_expression(rbql_src, {'a.name': vinf(1, 0), 'a2': vinf(1, 1), 'a4': vinf(1, 3), 'a8': vinf(1, 7), 'a["foo bar"]': vinf(1, 29)}, ['"100 200"', '"foo bar"'])
        e = cm.exception
        self.assertEqual(str(e), '''Unable to parse "UPDATE" expression: Unknown field name: "a.mysterious_field"''')


    def test_select_translation(self):
        rbql_src = ' *, a1,  a2,a1,*,*,b1, * ,   * '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *, a1,  a2,a1,*,*,*,b1, * ,   * '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *, a1,  a2,a1,*,a.* ,b.* , a.*  , *,*,b1, * ,   * '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [ a1,  a2,a1] + star_fields + [] + record_a + [] + record_b + [] + record_a + [] + star_fields + [] + star_fields + [b1] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' * '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,* '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,*, * '
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = ' *,*, * , *'
        test_dst = rbql_engine.translate_select_expression(rbql_src)[0]
        expected_dst = '[] + star_fields + [] + star_fields + [] + star_fields + [] + star_fields + []'
        self.assertEqual(expected_dst, test_dst)

        rbql_src = '   '
        with self.assertRaises(Exception) as cm:
            rbql_engine.translate_select_expression(rbql_src)
        e = cm.exception
        self.assertEqual(str(e), '''"SELECT" expression is empty''')



def round_floats(src_table):
    for row in src_table:
        for c in range(len(row)):
            if isinstance(row[c], float):
                row[c] = round(row[c], 3)


def do_randomly_split_replace(query, old_name, new_name):
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


class TestFromQueries(unittest.TestCase):
    # Basic tests for queries with FROM statements.
    def test_table_run_simple(self):
        input_table = [('Roosevelt', 1858), ('Napoleon', 1769), ('Confucius', -551)]
        query_text = 'select a2 // 10, "name " + a1 from input_table order by a2'
        expected_output_table = [[-56, 'name Confucius'], [176, 'name Napoleon'], [185, 'name Roosevelt']]
        output_table = []
        warnings = []

        input_iterator = None
        output_writer = rbql_engine.TableWriter(output_table)
        tables_registry = rbql_engine.ListTableRegistry([rbql_engine.ListTableInfo('input_table', input_table, None), rbql_engine.ListTableInfo('unused_table', [], None)], normalize_column_names=True)

        rbql.query(query_text, input_iterator, output_writer, warnings, tables_registry, user_init_code='')
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)


    def test_table_run_missing_from(self):
        input_table = [('Roosevelt', 1858), ('Napoleon', 1769), ('Confucius', -551)]
        query_text = 'select a2 // 10, "name " + a1 order by a2'
        expected_output_table = [[-56, 'name Confucius'], [176, 'name Napoleon'], [185, 'name Roosevelt']]
        output_table = []
        warnings = []
    
        input_iterator = None
        output_writer = rbql_engine.TableWriter(output_table)
        tables_registry = rbql_engine.ListTableRegistry([rbql_engine.ListTableInfo('input_table', input_table, None)], normalize_column_names=True)
        with self.assertRaises(Exception) as cm:
            rbql.query(query_text, input_iterator, output_writer, warnings, tables_registry, user_init_code='')
        e = cm.exception
        self.assertEqual('Queries without context-based input table must contain "FROM" statement', str(e))

    def test_table_run_wrong_from(self):
        input_table = [('Roosevelt', 1858), ('Napoleon', 1769), ('Confucius', -551)]
        query_text = 'select a2 // 10, "name " + a1 from input_table_nonexistent order by a2'
        expected_output_table = [[-56, 'name Confucius'], [176, 'name Napoleon'], [185, 'name Roosevelt']]
        output_table = []
        warnings = []

        input_iterator = None
        output_writer = rbql_engine.TableWriter(output_table)
        tables_registry = rbql_engine.ListTableRegistry([rbql_engine.ListTableInfo('input_table', input_table, None)], normalize_column_names=True)
        with self.assertRaises(Exception) as cm:
            rbql.query(query_text, input_iterator, output_writer, warnings, tables_registry, user_init_code='')
        e = cm.exception
        self.assertEqual('Unable to find input table: "input_table_nonexistent"', str(e))

    def test_table_run_simple_join(self):
        input_table = [('Roosevelt', 1858, 'USA'), ('Napoleon', 1769, 'France'), ('Confucius', -551, 'China')]
        join_table = [('China', 1386), ('France', 67), ('USA', 327), ('Russia', 140)]
        query_text = 'select a2 // 10, b2, "name " + a1 FROM my_input_table order by a2 JOIN my_join_table on a3 == b1'
        expected_output_table = [[-56, 1386, 'name Confucius'], [176, 67, 'name Napoleon'], [185, 327, 'name Roosevelt']]
        output_table = []
        warnings = []

        input_iterator = None
        output_writer = rbql_engine.TableWriter(output_table)
        tables_registry = rbql_engine.ListTableRegistry([rbql_engine.ListTableInfo('my_input_table', input_table, None), rbql_engine.ListTableInfo('my_join_table', join_table, None), rbql_engine.ListTableInfo('unused_table', [], None)], normalize_column_names=True)

        rbql.query(query_text, input_iterator, output_writer, warnings, tables_registry, user_init_code='')
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)


class TestTableRun(unittest.TestCase):
    def test_table_run_simple(self):
        input_table = [('Roosevelt', 1858), ('Napoleon', 1769), ('Confucius', -551)]
        query = 'select a2 // 10, "name " + a1 order by a2'
        expected_output_table = [[-56, 'name Confucius'], [176, 'name Napoleon'], [185, 'name Roosevelt']]
        output_table = []
        warnings = []
        rbql.query_table(query, input_table, output_table, warnings)
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)


    def test_table_run_simple_join(self):
        input_table = [('Roosevelt', 1858, 'USA'), ('Napoleon', 1769, 'France'), ('Confucius', -551, 'China')]
        join_table = [('China', 1386), ('France', 67), ('USA', 327), ('Russia', 140)]
        query = 'select a2 // 10, b2, "name " + a1 order by a2 JOIN B on a3 == b1'
        expected_output_table = [[-56, 1386, 'name Confucius'], [176, 67, 'name Napoleon'], [185, 327, 'name Roosevelt']]
        output_table = []
        warnings = []
        rbql.query_table(query, input_table, output_table, warnings, join_table)
        self.assertEqual(warnings, [])
        self.assertEqual(expected_output_table, output_table)



class TestJsonTables(unittest.TestCase):

    def process_test_case(self, test_case):
        test_name = test_case['test_name']
        query = test_case.get('query_python', None)
        if query is None:
            query = test_case.get('query_python_3', None)
        debug_mode = test_case.get('debug_mode', False)
        minimal_minor_python_version = int(test_case.get('minimal_python_version', '3.0').split('.')[1])
        if python_minor_version < minimal_minor_python_version:
            print('Skipping {}: python version must be at least {}. Interpreter version is {}'.format(test_name, minimal_minor_python_version, python_minor_version))
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
        normalize_column_names = test_case.get('normalize_column_names', True)
        user_init_code = test_case.get('python_init_code', '')
        expected_output_header = test_case.get('expected_output_header', list())
        expected_output_table = test_case.get('expected_output_table', None)
        expected_error_type = test_case.get('expected_error_type', None)
        expected_error = test_case.get('expected_error', None)
        if expected_error is None:
            expected_error = test_case.get('expected_error_py', None)
        if expected_error is None:
            expected_error = test_case.get('expected_error_py_3', None)
        expected_error_exact = test_case.get('expected_error_exact', False)
        expected_warnings = test_case.get('expected_warnings', [])
        output_table = []

        rbql_engine.set_debug_mode(debug_mode)
        warnings = []
        output_column_names = []
        error_type, error_msg = None, None
        try:
            rbql.query_table(query, input_table, output_table, warnings, join_table, input_column_names, join_column_names, output_column_names, normalize_column_names, user_init_code)
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
            round_floats(expected_output_table)
            round_floats(output_table)
            self.assertEqual(expected_output_table, output_table, 'Inside json test: {}'.format(test_name))
            self.assertEqual(expected_output_header, output_column_names, 'Inside json test: {}'.format(test_name))

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

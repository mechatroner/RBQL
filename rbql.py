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


#This module must be both python2 and python3 compatible
#TODO add other languages for functions: java, node js, cpp, perl

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

def dynamic_import(module_name):
    try:
        importlib.invalidate_caches()
    except AttributeError:
        pass
    return importlib.import_module(module_name)


#TODO Description of features
# * varying number of column for where queries or when column is not in result
# * lnum feature


sp4 = '    '
sp8 = sp4 + sp4
sp12 = sp4 + sp4 + sp4

column_var_regex = re.compile(r'^c([1-9][0-9]*)$')

class RBParsingError(Exception):
    pass


class RbAction:
    def __init__(self, action_type):
        self.action_type = action_type
        self.meta_code = None


def xrange6(x):
    if sys.version_info[0] < 3:
        return xrange(x)
    return range(x)


def is_digit(c):
    v = ord(c)
    return v >= ord('0') and v <= ord('9')


def is_boundary(c):
    if c == '_':
        return False
    v = ord(c)
    if v >= ord('a') and v <= ord('z'):
        return False
    if v >= ord('A') and v <= ord('Z'):
        return False
    if v >= ord('0') and v <= ord('9'):
        return False
    return True


def is_escaped_quote(cline, i):
    if i == 0:
        return False
    if i == 1 and cline[i - 1] == '\\':
        return True
    #Don't fix for raw string literals: double backslash before quote is not allowed there
    if cline[i - 1] == '\\' and cline[i - 2] != '\\':
        return True
    return False


def strip_comments(cline):
    cline = cline.rstrip()
    cline = cline.replace('\t', ' ')
    cur_quote_mark = None
    for i in xrange6(len(cline)):
        c = cline[i]
        if cur_quote_mark is None and c == '#':
            return cline[:i].rstrip()
        if cur_quote_mark is None and (c == "'" or c == '"'):
            cur_quote_mark = c
            continue
        if cur_quote_mark is not None and c == cur_quote_mark and not is_escaped_quote(cline, i):
            cur_quote_mark = None
    return cline


class TokenType:
    RAW = 1
    STRING_LITERAL = 2
    WHITESPACE = 3
    ALPHANUM_RAW = 4
    SYMBOLS_RAW = 5


class Token:
    def __init__(self, ttype, content):
        self.ttype = ttype
        self.content = content

    def __str__(self):
        return '{}\t{}'.join(self.ttype, self.content)


def tokenize_string_literals(lines):
    result = list()
    for cline in lines:
        cur_quote_mark = None
        k = 0
        i = 0
        while i < len(cline):
            c = cline[i]
            if cur_quote_mark is None and (c == "'" or c == '"'):
                cur_quote_mark = c
                result.append(Token(TokenType.RAW, cline[k:i]))
                k = i
            elif cur_quote_mark is not None and c == cur_quote_mark and not is_escaped_quote(cline, i):
                cur_quote_mark = None
                result.append(Token(TokenType.STRING_LITERAL, cline[k:i + 1]))
                k = i + 1
            i += 1
        if k < i:
            result.append(Token(TokenType.RAW, cline[k:i]))
        result.append(Token(TokenType.WHITESPACE, ' '))
    return result


def tokenize_terms(tokens):
    result = list()
    for token in tokens:
        if token.ttype != TokenType.RAW:
            result.append(token)
            continue
        content = token.content
        read_type = None
        k = 0
        i = 0
        for i in xrange6(len(content)):
            c = content[i]
            if c == ' ':
                if k < i:
                    assert read_type in [TokenType.ALPHANUM_RAW, TokenType.SYMBOLS_RAW]
                    result.append(Token(read_type, content[k:i]))
                k = i + 1
                read_type = None
                result.append(Token(TokenType.WHITESPACE, ' '))
                continue
            new_read_type = TokenType.SYMBOLS_RAW if is_boundary(c) else TokenType.ALPHANUM_RAW
            if read_type == new_read_type:
                continue
            if k < i:
                assert read_type is not None
                result.append(Token(read_type, content[k:i]))
            k = i
            read_type = new_read_type
        i = len(content)
        if k < i:
            assert read_type is not None
            result.append(Token(read_type, content[k:i]))
    return result


def remove_consecutive_whitespaces(tokens):
    result = list()
    for i in xrange6(len(tokens)):
        if (tokens[i].ttype != TokenType.WHITESPACE) or (i == 0) or (tokens[i - 1].ttype != TokenType.WHITESPACE):
            result.append(tokens[i])
    if len(result) and result[0].ttype == TokenType.WHITESPACE:
        result = result[1:]
    if len(result) and result[-1].ttype == TokenType.WHITESPACE:
        result = result[:-1]
    return result


def replace_column_vars(tokens):
    for i in xrange6(len(tokens)):
        if tokens[i].ttype == TokenType.STRING_LITERAL:
            continue
        mtobj = column_var_regex.match(tokens[i].content)
        if mtobj is not None:
            column_number = int(mtobj.group(1))
            tokens[i].content = 'fields[{}]'.format(column_number - 1)
    return tokens


def join_tokens(tokens):
    return ''.join([t.content for t in tokens])


def consume_action(tokens, idx):
    if tokens[idx].ttype == TokenType.STRING_LITERAL:
        return (None, idx + 1)
    if tokens[idx].content.upper() == 'SELECT':
        action = RbAction('SELECT')
        if idx + 2 < len(tokens) and tokens[idx + 1].ttype == TokenType.WHITESPACE and tokens[idx + 2].content.upper() == 'DISTINCT':
            action.distinct = True
            return (action, idx + 3)
        else:
            action.distinct = False
            return (action, idx + 1)
    if idx + 2 < len(tokens) and tokens[idx].content.upper() == 'ORDER' and tokens[idx + 1].ttype == TokenType.WHITESPACE and tokens[idx + 2].content.upper() == 'BY':
        action = RbAction('ORDER BY')
        return (action, idx + 3)
    if tokens[idx].content.upper() == 'WHERE':
        action = RbAction('WHERE')
        return (action, idx + 1)
    return (None, idx + 1)


def separate_actions(tokens):
    result = dict()
    prev_action = None
    k = 0
    i = 0
    while i < len(tokens):
        action, i_next = consume_action(tokens, i) 
        if action is None:
            i = i_next
            continue
        if prev_action is not None:
            prev_action.meta_code = join_tokens(tokens[k:i])
            result[prev_action.action_type] = prev_action
        if action.action_type in result:
            raise RBParsingError('More than one "{}" statements found'.format(action.action_type))
        prev_action = action
        i = i_next
        k = i
    if prev_action is not None:
        prev_action.meta_code = join_tokens(tokens[k:i])
        result[prev_action.action_type] = prev_action
    return result




spart_0 = r'''#!/usr/bin/env python

import sys
import random #for random sort
import datetime #for date manipulations
import re #for regexes
'''

spart_1 = r'''
DLM = '{}'

class SimpleWriter:
    def __init__(self, dst):
        self.dst = dst

    def write(self, record):
        self.dst.write(record)
        self.dst.write('\n')


class UniqWriter:
    def __init__(self, dst):
        self.dst = dst
        self.seen = set()

    def write(self, record):
        if record in self.seen:
            return
        self.seen.add(record)
        self.dst.write(record)
        self.dst.write('\n')


def main():
    rb_transform(sys.stdin, sys.stdout)

def rb_transform(source, destination):
    unsorted_entries = list()
    writer = {}(destination)
    for lnum, line in enumerate(source, 1):
        line = line.rstrip('\n')
        fields = line.split(DLM)
        flen = len(fields)
'''

spart_2 = r'''
        out_fields = [
'''

spart_3 = r'''        ]
'''

spart_simple_print = r'''
        writer.write(DLM.join([str(f) for f in out_fields]))
'''

spart_sort_add= r'''
        sort_key_value = ({})
        unsorted_entries.append((sort_key_value, DLM.join([str(f) for f in out_fields])))
'''

spart_sort_print = r'''
    if len(unsorted_entries):
        unsorted_entries = sorted(unsorted_entries, reverse = {})
        for e in unsorted_entries:
            writer.write(e[1])

'''

spart_final = r'''
if __name__ == '__main__':
    main()
'''

def vim_sanitize(obj):
    return str(obj).replace("'", '"')

def set_vim_variable(vim, var_name, value):
    str_value = str(value).replace("'", '"')
    vim.command("let {} = '{}'".format(var_name, str_value))

def normalize_delim(delim):
    if delim == '\t':
        return r'\t'
    return delim

def parse_to_py(rbql_lines, py_dst, delim, import_modules=None):
    if not py_dst.endswith('.py'):
        raise RBParsingError('python module file must have ".py" extension')

    for il in xrange6(len(rbql_lines)):
        cline = rbql_lines[il]
        if cline.find("'''") != -1 or cline.find('"""') != -1: #TODO improve parsing to allow multiline strings/comments
            raise RBParsingError('In line {}. Multiline python comments and doc strings are not allowed in rbql'.format(il + 1))
        rbql_lines[il] = strip_comments(cline)

    rbql_lines = [l for l in rbql_lines if len(l)]

    tokens = tokenize_string_literals(rbql_lines)
    tokens = tokenize_terms(tokens)
    tokens = remove_consecutive_whitespaces(tokens)
    tokens = replace_column_vars(tokens)
    rb_actions = separate_actions(tokens)

    if 'SELECT' not in rb_actions:
        raise RBParsingError('"SELECT" statement not found')
    select_distinct = rb_actions['SELECT'].distinct
    select_items = rb_actions['SELECT'].meta_code.split(',')
    select_items = [l.strip() for l in select_items]
    select_items = [l for l in select_items if len(l)]
    if not len(select_items):
        raise RBParsingError('"SELECT" expression is empty')

    writer_name = 'UniqWriter' if select_distinct else 'SimpleWriter'

    with open(py_dst, 'w') as dst:
        dst.write(spart_0)
        if import_modules is not None:
            for mdl in import_modules:
                dst.write('import {}\n'.format(mdl))
        dst.write(spart_1.format(normalize_delim(delim), writer_name))
        if 'WHERE' in rb_actions:
            dst.write('{}if not ({}):\n'.format(sp8, rb_actions['WHERE'].meta_code))
            dst.write('{}continue\n'.format(sp12))
        dst.write(spart_2)
        for l in select_items:
            if l == '*':
                dst.write('{}line,\n'.format(sp12, l))
            else:
                dst.write('{}{},\n'.format(sp12, l))
        dst.write(spart_3)
        reverse_sort = 'False'
        if 'ORDER BY' in rb_actions:
            order_expression = rb_actions['ORDER BY'].meta_code
            direction_marker = ' DESC'
            if order_expression.upper().endswith(direction_marker):
                order_expression = order_expression[:-len(direction_marker)].rstrip()
                reverse_sort = 'True'
            direction_marker = ' ASC'
            if order_expression.upper().endswith(direction_marker):
                order_expression = order_expression[:-len(direction_marker)].rstrip()
            dst.write(spart_sort_add.format(order_expression))
        else:
            dst.write(spart_simple_print)

        dst.write(spart_sort_print.format(reverse_sort))
        dst.write(spart_final)


def vim_execute(src_table_path, rb_script_path, py_script_path, dst_table_path, delim):
    if os.path.exists(py_script_path):
        os.remove(py_script_path)
    import vim
    try:
        src_lines = open(rb_script_path).readlines()
        parse_to_py(src_lines, py_script_path, delim)
    except RBParsingError as e:
        set_vim_variable(vim, 'query_status', 'Parsing Error')
        set_vim_variable(vim, 'report', e)
        return

    module_name = os.path.basename(py_script_path)
    assert module_name.endswith('.py')
    module_name = module_name[:-3]
    module_dir = os.path.dirname(py_script_path) 
    sys.path.insert(0, module_dir)
    try:
        rbconvert = dynamic_import(module_name)
        src = open(src_table_path, 'r')
        with open(dst_table_path, 'w') as dst:
            rbconvert.rb_transform(src, dst)
        src.close()
    except Exception as e:
        error_msg = 'Error: Unable to use generated python module.\n'
        error_msg += 'Original python exception:\n{}\n'.format(str(e))
        set_vim_variable(vim, 'query_status', 'Execution Error')
        set_vim_variable(vim, 'report', error_msg)
        return
    set_vim_variable(vim, 'query_status', 'OK')



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--delim', help='Delimiter', default=r'\t')
    parser.add_argument('--query', help='Query string in rbql')
    parser.add_argument('--query_path', metavar='FILE', help='Read rbql query from FILE')
    parser.add_argument('--input_table_path', metavar='FILE', help='Read input table from FILE instead of stdin')
    parser.add_argument('--output_table_path', metavar='FILE', help='Write output table to FILE instead of stdout')
    parser.add_argument('--convert_only', action='store_true', help='Only generate python script do not run query on input table')
    parser.add_argument('-I', dest='libs', action='append', help='Import module to use in the result conversion script. Can be used multiple times')
    args = parser.parse_args()

    delim = args.delim
    query = args.query
    query_path = args.query_path
    convert_only = args.convert_only
    input_path = args.input_table_path
    output_path = args.output_table_path
    import_modules = args.libs

    rbql_lines = None
    if query is None and query_path is None:
        print >> sys.stderr, 'Error: provide either "--query" or "--query_path" option'
        sys.exit(1)
    if query is not None and query_path is not None:
        print >> sys.stderr, 'Error: unable to use both "--query" and "--query_path" options'
        sys.exit(1)
    if query_path is not None:
        assert query is None
        rbql_lines = open(query_path).readlines()
    else:
        assert query_path is None
        rbql_lines = [query]

    tmp_dir = tempfile.gettempdir()

    module_name = 'rbconvert_{}'.format(time.time()).replace('.', '_')
    module_filename = '{}.py'.format(module_name)

    tmp_path = os.path.join(tmp_dir, module_filename)

    try:
        parse_to_py(rbql_lines, tmp_path, delim, import_modules)
    except RBParsingError as e:
        print('RBQL Parsing Error: \t{}'.format(e))
        sys.exit(1)
    if not os.path.isfile(tmp_path) or not os.access(tmp_path, os.R_OK):
        print >> sys.stderr, 'Error: Unable to find generated python module at {}.'.format(tmp_path)
        sys.exit(1)
    sys.path.insert(0, tmp_dir)
    try:
        rbconvert = dynamic_import(module_name)
        input_src = open(input_path) if input_path else sys.stdin
        if output_path:
            with open(output_path, 'w') as dst:
                rbconvert.rb_transform(input_src, dst)
        else:
            rbconvert.rb_transform(input_src, sys.stdout)
    except Exception as e:
        error_msg = 'Error: Unable to use generated python module.\n'
        error_msg += 'Location of the generated module: {}\n\n'.format(tmp_path)
        error_msg += 'Original python exception:\n{}\n'.format(str(e))
        print >> sys.stderr, error_msg
        sys.exit(1)



def table_to_string(array2d):
    return '\n'.join(['\t'.join(ln) for ln in array2d])


def table_to_stream(array2d):
    return StringIO(table_to_string(array2d))


def run_conversion_test(query, input_table, testname, import_modules=None):
    tmp_dir = tempfile.gettempdir()
    if not len(sys.path) or sys.path[0] != tmp_dir:
        sys.path.insert(0, tmp_dir)
    module_name = 'rbconvert_{}_{}'.format(time.time(), testname).replace('.', '_')
    module_filename = '{}.py'.format(module_name)
    tmp_path = os.path.join(tmp_dir, module_filename)
    src = table_to_stream(input_table)
    dst = StringIO()
    parse_to_py([query], tmp_path, '\t', import_modules)
    assert os.path.isfile(tmp_path) and os.access(tmp_path, os.R_OK)
    rbconvert = dynamic_import(module_name)
    rbconvert.rb_transform(src, dst)
    out_data = dst.getvalue()
    out_lines = out_data[:-1].split('\n')
    out_table = [ln.split('\t') for ln in out_lines]
    return out_table


class TestEverything(unittest.TestCase):
    def compare_tables(self, canonic_table, test_table):
        self.assertEqual(len(canonic_table), len(test_table))
        for i in xrange6(len(canonic_table)):
            self.assertEqual(len(canonic_table[i]), len(test_table[i]))
            self.assertEqual(canonic_table[i], test_table[i])
        self.assertEqual(canonic_table, test_table)

    def test_run1(self):
        query = 'select lnum, c1, len(c3) where int(c1) > 5'

        input_table = list()
        input_table.append(['5', 'haha', 'hoho'])
        input_table.append(['-20', 'haha', 'hioho'])
        input_table.append(['50', 'haha', 'dfdf'])
        input_table.append(['20', 'haha', ''])

        canonic_table = list()
        canonic_table.append(['3', '50', '4'])
        canonic_table.append(['4', '20', '0'])

        test_table = run_conversion_test(query, input_table, 'test1')
        self.compare_tables(canonic_table, test_table)

    def test_run2(self):
        query = 'select distinct c2 where int(c1) > 10'

        input_table = list()
        input_table.append(['5', 'haha', 'hoho'])
        input_table.append(['-20', 'haha', 'hioho'])
        input_table.append(['50', 'haha', 'dfdf'])
        input_table.append(['20', 'haha', ''])
        input_table.append(['8'])
        input_table.append(['3', '4', '1000', 'asdfasf', 'asdfsaf', 'asdfa'])
        input_table.append(['11', 'hoho', ''])
        input_table.append(['10', 'hihi', ''])
        input_table.append(['13', 'haha', ''])

        canonic_table = list()
        canonic_table.append(['haha'])
        canonic_table.append(['hoho'])

        test_table = run_conversion_test(query, input_table, 'test2')
        self.compare_tables(canonic_table, test_table)

    def test_run3(self):
        query = 'select * order by int(c1) desc'
        input_table = list()
        input_table.append(['5', 'haha', 'hoho'])
        input_table.append(['-20', 'haha', 'hioho'])
        input_table.append(['50', 'haha', 'dfdf'])
        input_table.append(['20', 'haha', ''])
        input_table.append(['11', 'hoho', ''])
        input_table.append(['10', 'hihi', ''])
        input_table.append(['13', 'haha', ''])

        canonic_table = list()
        canonic_table.append(['50', 'haha', 'dfdf'])
        canonic_table.append(['20', 'haha', ''])
        canonic_table.append(['13', 'haha', ''])
        canonic_table.append(['11', 'hoho', ''])
        canonic_table.append(['10', 'hihi', ''])
        canonic_table.append(['5', 'haha', 'hoho'])
        canonic_table.append(['-20', 'haha', 'hioho'])


        test_table = run_conversion_test(query, input_table, 'test3')
        self.compare_tables(canonic_table, test_table)

    def test_run4(self):
        query = 'select int(math.sqrt(int(c1)))'
        input_table = list()
        input_table.append(['0', 'haha', 'hoho'])
        input_table.append(['9'])
        input_table.append(['81', 'haha', 'dfdf'])
        input_table.append(['4', 'haha', 'dfdf', 'asdfa', '111'])

        canonic_table = list()
        canonic_table.append(['0'])
        canonic_table.append(['3'])
        canonic_table.append(['9'])
        canonic_table.append(['2'])

        test_table = run_conversion_test(query, input_table, 'test4', ['math', 'os'])
        self.compare_tables(canonic_table, test_table)


    def test_run5(self):
        query = 'select c2'
        input_table = list()
        input_table.append(['0', 'haha', 'hoho'])
        input_table.append(['9'])
        input_table.append(['81', 'haha', 'dfdf'])
        input_table.append(['4', 'haha', 'dfdf', 'asdfa', '111'])

        with self.assertRaises(IndexError):
            run_conversion_test(query, input_table, 'test5', ['math', 'os'])


class TestStringMethods(unittest.TestCase):

    def test_strip(self):
        a = 'v = "hello" #world  '
        a_strp = strip_comments(a)
        self.assertEqual(a_strp, 'v = "hello"')

    def test_strip2(self):
        a = r'''v = "hel\"lo" #w'or"ld  '''
        a_strp = strip_comments(a)
        self.assertEqual(a_strp, r'''v = "hel\"lo"''')

    def test_strip3(self):
        a = r'''v = "hello\\" #w'or"ld  '''
        a_strp = strip_comments(a)
        self.assertEqual(a_strp, r'''v = "hello\\"''')

    def test_strip4(self):
        a = ''' # a comment'''
        a_strp = strip_comments(a)
        self.assertEqual(a_strp, '')


if __name__ == '__main__':
    main()



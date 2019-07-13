#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function

import sys
import argparse

from . import csv_utils
from . import rbql_csv
from . import _version


PY3 = sys.version_info[0] == 3


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


policy_names = ['quoted', 'simple', 'whitespace', 'monocolumn']
out_format_names = ['csv', 'tsv', 'input']


def xrange6(x):
    if PY3:
        return range(x)
    return xrange(x)


def get_default_policy(delim):
    if delim in [';', ',']:
        return 'quoted'
    elif delim == ' ':
        return 'whitespace'
    else:
        return 'simple'


def show_error(error_type, error_msg, is_interactive):
    if is_interactive:
        full_msg = '{}Error [{}]:{} {}'.format(u'\u001b[31;1m', error_type, u'\u001b[0m', error_msg)
        print(full_msg)
    else:
        eprint('Error [{}]: {}'.format(error_type, error_msg))


def show_warning(msg, is_interactive):
    if is_interactive:
        full_msg = '{}Warning:{} {}'.format(u'\u001b[33;1m', u'\u001b[0m', msg)
        print(full_msg)
    else:
        eprint('Warning: ' + msg)


def run_with_python(args, is_interactive):
    delim = rbql_csv.normalize_delim(args.delim)
    policy = args.policy if args.policy is not None else get_default_policy(delim)
    query = args.query
    input_path = args.input
    output_path = args.output
    init_source_file = args.init_source_file
    csv_encoding = args.encoding
    args.output_delim, args.output_policy = (delim, policy) if args.out_format == 'input' else rbql_csv.interpret_named_csv_format(args.out_format)
    out_delim, out_policy = args.output_delim, args.output_policy

    user_init_code = ''
    if init_source_file is not None:
        user_init_code = rbql_csv.read_user_init_code(init_source_file)
    error_info, warnings = rbql_csv.csv_run(query, input_path, delim, policy, output_path, out_delim, out_policy, csv_encoding, user_init_code)

    if error_info is None:
        success = True
        for warning in warnings:
            show_warning(warning, is_interactive)
    else:
        success = False
        error_type = error_info['type']
        error_msg = error_info['message']
        show_error(error_type, error_msg, is_interactive)

    return success



def is_delimited_table(sampled_lines, delim, policy):
    if len(sampled_lines) < 2:
        return False
    num_fields = None
    for sl in sampled_lines:
        fields, warning = csv_utils.smart_split(sl, delim, policy, True)
        if warning or len(fields) < 2:
            return False
        if num_fields is None:
            num_fields = len(fields)
        if num_fields != len(fields):
            return False
    return True


def sample_lines(src_path, encoding):
    result = []
    source = open(src_path, 'rb')
    line_iterator = rbql_csv.CSVRecordIterator(source, True, encoding, delim=None, policy=None)
    for _i in xrange6(10):
        line = line_iterator.get_row()
        if line is None:
            break
        result.append(line)
    return result


def autodetect_delim_policy(input_path, encoding):
    sampled_lines = sample_lines(input_path, encoding)
    autodetection_dialects = [('\t', 'simple'), (',', 'quoted'), (';', 'quoted')]
    for delim, policy in autodetection_dialects:
        if is_delimited_table(sampled_lines, delim, policy):
            return (delim, policy)
    if input_path.endswith('.csv'):
        return (',', 'quoted')
    if input_path.endswith('.tsv'):
        return ('\t', 'simple')
    return (None, None)


def sample_records(input_path, delim, policy, encoding):
    sampled_lines = sample_lines(input_path, encoding)
    bad_lines = []
    result = []
    for il, line in enumerate(sampled_lines):
        fields, warning = csv_utils.smart_split(line, delim, policy, True)
        if warning:
            bad_lines.append(il + 1)
        result.append(fields)
    return (bad_lines, result)


def print_colorized(records, delim, encoding, show_column_names):
    # TODO consider colorizing a1,a2,... in different default color
    reset_color_code = u'\u001b[0m'
    color_codes = [u'\u001b[0m', u'\u001b[31m', u'\u001b[32m', u'\u001b[33m', u'\u001b[34m', u'\u001b[35m', u'\u001b[36m', u'\u001b[31;1m', u'\u001b[32;1m', u'\u001b[33;1m']
    for record in records:
        out_fields = []
        for i, field in enumerate(record):
            color_code = color_codes[i % len(color_codes)]
            if show_column_names:
                colored_field = '{}a{}:{}'.format(color_code, i + 1, field)
            else:
                colored_field = '{}{}'.format(color_code, field)
            out_fields.append(colored_field)
        out_line = delim.join(out_fields) + reset_color_code
        if PY3:
            sys.stdout.buffer.write(out_line.encode(encoding))
        else:
            sys.stdout.write(out_line.encode(encoding))
        sys.stdout.write('\n')
        sys.stdout.flush()


def get_default_output_path(input_path, delim):
    well_known_extensions = {',': '.csv', '\t': '.tsv'}
    if delim in well_known_extensions:
        return input_path + well_known_extensions[delim]
    return input_path + '.txt'


def run_interactive_loop(args):
    while True:
        print('\nInput SQL-like RBQL query and press Enter:')
        sys.stdout.write('> ')
        sys.stdout.flush()
        query = sys.stdin.readline()
        if not len(query):
            print()
            break # Ctrl-D
        query = query.strip()
        args.query = query
        success = run_with_python(args, is_interactive=True)
        if success:
            print('\nOutput table preview:')
            print('====================================')
            _bad_lines, records = sample_records(args.output, args.output_delim, args.output_policy, args.encoding)
            print_colorized(records, args.output_delim, args.encoding, show_column_names=False)
            print('====================================')
            print('Success! Result table was saved to: ' + args.output)
            break


def start_preview_mode(args):
    input_path = args.input
    if not input_path:
        show_error('generic', 'Input file must be provided in interactive mode. You can use stdin input only in non-interactive mode', is_interactive=True)
        return
    if args.delim is not None:
        delim = rbql_csv.normalize_delim(args.delim)
        policy = args.policy if args.policy is not None else get_default_policy(delim)
    else:
        delim, policy = autodetect_delim_policy(input_path, args.encoding)
        if delim is None:
            show_error('generic', 'Unable to autodetect table delimiter. Provide column separator explicitly with "--delim" option', is_interactive=True)
            return
        args.delim = delim
        args.policy = policy
    bad_lines, records = sample_records(input_path, delim, policy, args.encoding)
    print('Input table preview:')
    print('====================================')
    print_colorized(records, delim, args.encoding, show_column_names=True)
    print('====================================\n')
    if len(bad_lines):
        show_warning('Some input lines have quoting errors. Line numbers: ' + ', '.join([str(v) for v in bad_lines]), is_interactive=True)
    if args.output is None:
        args.output = get_default_output_path(input_path, delim)
        show_warning('Output path was not provided. Result set will be saved as: ' + args.output, is_interactive=True)
    try:
        run_interactive_loop(args)
    except KeyboardInterrupt:
        print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--delim', help='Delimiter')
    parser.add_argument('--policy', help='csv split policy', choices=policy_names)
    parser.add_argument('--out-format', help='output format', default='input', choices=out_format_names)
    parser.add_argument('--query', help='Query string in rbql. Run in interactive mode if not provided')
    parser.add_argument('--input', metavar='FILE', help='Read csv table from FILE instead of stdin. Must always be provided in interactive mode')
    parser.add_argument('--output', metavar='FILE', help='Write output table to FILE instead of stdout. Must always be provided in interactive mode')
    parser.add_argument('--version', action='store_true', help='Print RBQL version and exit')
    parser.add_argument('--encoding', help='Manually set csv table encoding', default=rbql_csv.default_csv_encoding, choices=['latin-1', 'utf-8'])
    parser.add_argument('--init-source-file', metavar='FILE', help='path to init source file to use instead of ~/.rbql_init_source.py')
    args = parser.parse_args()

    if args.version:
        print(_version.__version__)
        return

    if args.query:
        if args.delim is None:
            show_error('generic', 'Separator must be provided with "--delim" option in non-interactive mode', is_interactive=False)
            sys.exit(1)
        success = run_with_python(args, is_interactive=False)
        if not success:
            sys.exit(1)
    else:
        start_preview_mode(args)



if __name__ == '__main__':
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import sys
import os
import argparse
import random
import codecs
import json

def write_json_line(dst, indent, line, add_newline=True):
    out_line = '    ' * indent + line 
    if add_newline:
        out_line += '\n'
    dst.write(out_line)


def main():
    parser = argparse.ArgumentParser()
    #parser.add_argument('--verbose', action='store_true', help='Run in verbose mode')
    #parser.add_argument('--num_iter', type=int, help='number of iterations option')
    parser.add_argument('file_name', help='example of positional argument')
    parser.add_argument('dst_file', help='example of positional argument')
    args = parser.parse_args()

    #num_iter = args.num_iter
    file_name = args.file_name
    dst_file = args.dst_file
    lines = None
    with codecs.open(file_name, encoding='utf-8') as src:
        lines = src.readlines()

    with codecs.open(dst_file, 'w', encoding='utf-8') as dst:
        dst.write('[\n')
        data_before = None
        for il, line in enumerate(lines):
            if il % 2 == 0:
                data_before = json.loads(line)
            else:
                data_cur = json.loads(line)
                assert data_cur['src_table'] == data_before['src_table']
                assert data_cur['canonic_table'] == data_before['canonic_table']
                assert data_cur['backend_language'] == 'js'
                dst.write('    {\n')
                indent = 2
                write_json_line(dst, indent, '"test_name": ' + json.dumps('test_{}'.format(il / 2 + 1), ensure_ascii=False) + ',')
                dst.write('    }')
                if il + 1 < len(lines):
                    dst.write(',')
                dst.write('\n')
                data_before = None
        dst.write(']\n')

if __name__ == '__main__':
    main()

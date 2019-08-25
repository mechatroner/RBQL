# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import sys
import datetime
import os

PY3 = sys.version_info[0] == 3

#This module must be both python2 and python3 compatible


class MaxMinSumToken:
    def __init__(self, value):
        self.mad_value = value


def MAX(value):
    return MaxMinSumToken(value)


def MIN(value):
    return MaxMinSumToken(value)


def SUM(value):
    return MaxMinSumToken(value)


# <<<< COPYPASTE FROM "mad_max.py"
#####################################
#####################################
# This is to ensure that "mad_max.py" file has exactly the same content as this fragment. This condition will be ensured by test_mad_max.py
# To edit this code you need to simultaneously edit this fragment and content of mad_max.py, otherwise test_mad_max.py will fail.

builtin_max = max
builtin_min = min
builtin_sum = sum


def max(*args, **kwargs):
    single_arg = len(args) == 1 and not kwargs
    if single_arg:
        if PY3 and isinstance(args[0], str):
            return MAX(args[0])
        if not PY3 and isinstance(args[0], basestring):
            return MAX(args[0])
        if isinstance(args[0], int) or isinstance(args[0], float):
            return MAX(args[0])
    try:
        return builtin_max(*args, **kwargs)
    except TypeError:
        if single_arg:
            return MAX(args[0])
        raise


def min(*args, **kwargs):
    single_arg = len(args) == 1 and not kwargs
    if single_arg:
        if PY3 and isinstance(args[0], str):
            return MIN(args[0])
        if not PY3 and isinstance(args[0], basestring):
            return MIN(args[0])
        if isinstance(args[0], int) or isinstance(args[0], float):
            return MIN(args[0])
    try:
        return builtin_min(*args, **kwargs)
    except TypeError:
        if single_arg:
            return MIN(args[0])
        raise


def sum(*args):
    try:
        return builtin_sum(*args)
    except TypeError:
        if len(args) == 1:
            return SUM(args[0])
        raise

#####################################
#####################################
# >>>> COPYPASTE END



def read_file(file_path):
    with open(file_path) as f:
        return f.read()


class TestMadMax(unittest.TestCase):

    def test_mad_max(self):
        now = datetime.datetime.now()
        self.assertTrue(max(7).mad_value == 7)
        self.assertTrue(max(None).mad_value == None)
        self.assertTrue(max(now).mad_value == now)
        self.assertTrue(max('hello').mad_value == 'hello')
        self.assertTrue(max(0.6).mad_value == 0.6)
        self.assertTrue(max(4, 6) == 6)
        self.assertTrue(max(4, 8, 6) == 8)
        self.assertTrue(max(4, 8, 6, key=lambda v: -v) == 4)
        if PY3:
            self.assertTrue(max([], default=7) == 7)
            self.assertTrue(max(['b', 'x', 'a'], default='m') == 'x')
        with self.assertRaises(TypeError) as cm:
            max(7, key=lambda v: v)
        e = cm.exception
        self.assertTrue(str(e).find('object is not iterable') != -1)


    def test_mad_min(self):
        now = datetime.datetime.now()
        self.assertTrue(min(7).mad_value == 7)
        self.assertTrue(min(None).mad_value == None)
        self.assertTrue(min(now).mad_value == now)
        self.assertTrue(min('hello').mad_value == 'hello')
        self.assertTrue(min(0.6).mad_value == 0.6)
        self.assertTrue(min(4, 6) == 4)
        self.assertTrue(min(4, 8, 6) == 4)
        self.assertTrue(min(4, 8, 6, key=lambda v: -v) == 8)
        if PY3:
            self.assertTrue(min([], default=7) == 7)
            self.assertTrue(min(['b', 'x', 'a'], default='m') == 'a')
        with self.assertRaises(TypeError) as cm:
            min(7, key=lambda v: v)
        e = cm.exception
        self.assertTrue(str(e).find('object is not iterable') != -1)


    def test_mad_sum(self):
        now = datetime.datetime.now()
        self.assertTrue(sum(7).mad_value == 7)
        self.assertTrue(sum(None).mad_value == None)
        self.assertTrue(sum(now).mad_value == now)
        self.assertTrue(sum([1, 2, 3]) == 6)
        self.assertTrue(sum([1, 2, 3], 2) == 8)
        self.assertTrue(sum('hello').mad_value == 'hello')
        with self.assertRaises(TypeError) as cm:
            sum(7, 8)


    def test_mad_source(self):
        this_file_path = os.path.realpath(__file__.rstrip('c'))
        test_dir_path = os.path.dirname(this_file_path)
        rbql_dir_path = os.path.dirname(test_dir_path)
        mad_max_path = os.path.join(test_dir_path, 'mad_max.py')
        template_path = os.path.join(rbql_dir_path, 'rbql', 'engine', 'template.py')
        original_data = read_file(mad_max_path)
        this_data = read_file(this_file_path)
        template_data = read_file(template_path)
        assert original_data.find('COPYPASTE') != -1
        assert this_data.find(original_data) != -1
        assert template_data.find(original_data) != -1

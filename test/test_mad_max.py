# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import sys

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
    try:
        return builtin_max(*args, **kwargs)
    except TypeError:
        if len(args) == 1 and not kwargs:
            return MAX(args[0])
        raise


def min(*args, **kwargs):
    try:
        return builtin_min(*args, **kwargs)
    except TypeError:
        if len(args) == 1 and not kwargs:
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


class TestMadMax(unittest.TestCase):

    def test_mad_max(self):
        self.assertTrue(max(7).mad_value == 7)
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
        pass #FIXME


    def test_mad_sum(self):
        pass #FIXME


    def test_mad_source(self):
        pass #FIXME check both this file and template.py

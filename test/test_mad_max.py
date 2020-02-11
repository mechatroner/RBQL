# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import sys
import datetime
import os

PY3 = sys.version_info[0] == 3

#This module must be both python2 and python3 compatible

import rbql
from rbql.rbql_engine import *

rbql.rbql_engine.query_context = RBQLContext(None, None, None)


class TestMadMax(unittest.TestCase):

    def test_mad_max(self):
        now = datetime.datetime.now()
        self.assertTrue(max(7).value == 7)
        self.assertTrue(max(None).value == None)
        self.assertTrue(max(now).value == now)
        self.assertTrue(max('hello').value == 'hello')
        self.assertTrue(max(0.6).value == 0.6)
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
        self.assertTrue(min(7).value == 7)
        self.assertTrue(min(None).value == None)
        self.assertTrue(min(now).value == now)
        self.assertTrue(min('hello').value == 'hello')
        self.assertTrue(min(0.6).value == 0.6)
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
        self.assertTrue(sum(7).value == 7)
        self.assertTrue(sum(None).value == None)
        self.assertTrue(sum(now).value == now)
        self.assertTrue(sum([1, 2, 3]) == 6)
        self.assertTrue(sum([1, 2, 3], 2) == 8)
        self.assertTrue(sum('hello').value == 'hello')
        with self.assertRaises(TypeError) as cm:
            sum(7, 8)


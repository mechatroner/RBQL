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
from rbql import rbql_engine


def get_mad_max():
    return rbql_engine.compile_and_run(rbql_engine.RBQLContext(None, None, None), unit_test_mode=True)[0]

def get_mad_min():
    return rbql_engine.compile_and_run(rbql_engine.RBQLContext(None, None, None), unit_test_mode=True)[1]

def get_mad_sum():
    return rbql_engine.compile_and_run(rbql_engine.RBQLContext(None, None, None), unit_test_mode=True)[2]


class TestMadMax(unittest.TestCase):

    def test_mad_max(self):
        now = datetime.datetime.now()
        self.assertEqual(7, get_mad_max()(7).value)
        self.assertTrue(get_mad_max()(None).value == None)
        self.assertTrue(get_mad_max()(now).value == now)
        self.assertTrue(get_mad_max()('hello').value == 'hello')
        self.assertTrue(get_mad_max()(0.6).value == 0.6)
        self.assertTrue(get_mad_max()(4, 6) == 6)
        self.assertTrue(get_mad_max()(4, 8, 6) == 8)
        self.assertTrue(get_mad_max()(4, 8, 6, key=lambda v: -v) == 4)
        if PY3:
            self.assertTrue(get_mad_max()([], default=7) == 7)
            self.assertTrue(get_mad_max()(['b', 'x', 'a'], default='m') == 'x')
        with self.assertRaises(TypeError) as cm:
            get_mad_max()(7, key=lambda v: v)
        e = cm.exception
        self.assertTrue(str(e).find('object is not iterable') != -1)

    
    def test_mad_min(self):
        now = datetime.datetime.now()
        self.assertTrue(get_mad_min()(7).value == 7)
        self.assertTrue(get_mad_min()(None).value == None)
        self.assertTrue(get_mad_min()(now).value == now)
        self.assertTrue(get_mad_min()('hello').value == 'hello')
        self.assertTrue(get_mad_min()(0.6).value == 0.6)
        self.assertTrue(get_mad_min()(4, 6) == 4)
        self.assertTrue(get_mad_min()(4, 8, 6) == 4)
        self.assertTrue(get_mad_min()(4, 8, 6, key=lambda v: -v) == 8)
        if PY3:
            self.assertTrue(get_mad_min()([], default=7) == 7)
            self.assertTrue(get_mad_min()(['b', 'x', 'a'], default='m') == 'a')
        with self.assertRaises(TypeError) as cm:
            get_mad_min()(7, key=lambda v: v)
        e = cm.exception
        self.assertTrue(str(e).find('object is not iterable') != -1)
    
    
    def test_mad_sum(self):
        now = datetime.datetime.now()
        self.assertTrue(get_mad_sum()(7).value == 7)
        self.assertTrue(get_mad_sum()(None).value == None)
        self.assertTrue(get_mad_sum()(now).value == now)
        self.assertTrue(get_mad_sum()([1, 2, 3]) == 6)
        self.assertTrue(get_mad_sum()([1, 2, 3], 2) == 8)
        self.assertTrue(get_mad_sum()('hello').value == 'hello')
        with self.assertRaises(TypeError) as cm:
            get_mad_sum()(7, 8)
    

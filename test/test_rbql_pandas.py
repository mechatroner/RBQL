# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

import unittest
import os
import sys
import json
import random
import tempfile
import time
import shutil
import pandas
from pandas.testing import assert_frame_equal

import rbql
from rbql import rbql_engine
from rbql import rbql_pandas


class TestDataframeBasic(unittest.TestCase):
    def test_basic(self):
        input_df = pandas.DataFrame([['foo', 2], ['bar', 7], ['zorb', 20]], columns=['kind', 'speed'])
        output_warnings = []
        output_df = rbql_pandas.query_dataframe('select * where a1.find("o") != -1 order by a.speed', input_df, output_warnings)
        self.assertEqual(2, len(output_df))
        self.assertEqual([], output_warnings, 2)
        expected_output_df = pandas.DataFrame([['foo', 2], ['zorb', 20]], columns=['kind', 'speed'])
        assert_frame_equal(expected_output_df, output_df)
import argparse
import os
import sys
import tempfile
import unittest

script_dir = os.path.dirname(os.path.abspath(__file__))
# Use insert instead of append to make sure that we are using local rbql here.
sys.path.insert(0, os.path.join(os.path.dirname(script_dir), 'rbql-py'))

from rbql import rbql_main


def make_csv_args(input_path, init_source_file):
    return argparse.Namespace(
        debug_mode=False,
        delim=',',
        policy='quoted',
        query='select a1',
        with_headers=False,
        input=input_path,
        output=None,
        encoding='utf-8',
        out_format='input',
        output_delim=',',
        output_policy='quoted',
        init_source_file=init_source_file,
        comment_prefix=None,
        comment_regex=None,
        color=False,
        strip_spaces=False,
    )


class TestMissingInitSourceFile(unittest.TestCase):
    def test_missing_init_source_file_returns_error_not_exception(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = os.path.join(tmp_dir, 'input.csv')
            with open(input_path, 'w') as f:
                f.write('a,b\n1,2\n')
            missing_init = os.path.join(tmp_dir, 'does-not-exist-init.py')
            args = make_csv_args(input_path, missing_init)
            # A missing --init-source-file must be reported as a regular
            # RBQL error (return False), not escape as a raw traceback.
            self.assertFalse(rbql_main.run_with_python_csv(args, is_interactive=False))


if __name__ == '__main__':
    unittest.main()

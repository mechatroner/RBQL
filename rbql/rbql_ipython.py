# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

from . import rbql_engine
from . import rbql_pandas

# TODO figure out how to implement at least basic autocomplete for the magic command.


class IPythonDataframeRegistry(rbql_engine.RBQLTableRegistry):
    def __init__(self, all_ns_refs):
        self.all_ns_refs = all_ns_refs

    def get_iterator_by_table_id(self, table_id, single_char_alias):
        # It seems to be the first namespace is "user" namespace, at least according to this code: 
        # https://github.com/google/picatrix/blob/a2f39766ad4b007b125dc8f84916e18fb3dc5478/picatrix/lib/utils.py
        for ns in self.all_ns_refs:
            if table_id in ns:
                return rbql_pandas.DataframeIterator(ns[table_id], normalize_column_names=True, variable_prefix=single_char_alias)
        return None


def eprint(*args, **kwargs):
    import sys
    print(*args, file=sys.stderr, **kwargs)


def load_ipython_extension(ipython):
    from IPython.core.magic import register_line_magic
    from IPython.core.getipython import get_ipython

    ipython = ipython or get_ipython() # The pattern taken from here: https://github.com/pydoit/doit/blob/9efe141a5dc96d4912143561695af7fc4a076490/doit/tools.py
    # ipython is interactiveshell. Docs: https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.interactiveshell.html

    # The difference between line and cell magic is described here: https://jakevdp.github.io/PythonDataScienceHandbook/01.03-magic-commands.html.
    # In short: line magic only accepts one line of input whereas cell magic supports multiline input as magic command argument.
    # Both line and cell magic would make sense for RBQL queries but for MVP it should be enough to implement just the cell magic.
    @register_line_magic("rbql")
    def run_rbql_query(query_text):
        # Unfortunately globals() and locals() called from here won't contain user variables defined in the notebook.

        tables_registry = IPythonDataframeRegistry(ipython.all_ns_refs)
        output_writer = rbql_pandas.DataframeWriter()
        # Ignore warnings because pandas dataframes can't cause them.
        output_warnings = []
        # TODO make it possible to specify user_init_code in code cells.
        error_type, error_msg = None, None
        try:
            rbql_engine.query(query_text, input_iterator=None, output_writer=output_writer, output_warnings=output_warnings, join_tables_registry=tables_registry, user_init_code='')
        except Exception as e:
            error_type, error_msg = rbql_engine.exception_to_error_info(e)
        if error_type is None:
            return output_writer.result
        else:
            # TODO use IPython.display to print error in red color, see https://stackoverflow.com/questions/16816013/is-it-possible-to-print-using-different-colors-in-ipythons-notebook
            eprint('Error [{}]: {}'.format(error_type, error_msg))

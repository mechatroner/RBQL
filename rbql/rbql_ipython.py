# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function

from . import rbql_engine
from . import rbql_pandas


class IPythonDataframeRegistry(rbql_engine.RBQLTableRegistry):
    def __init__(self, all_ns_refs):
        self.all_ns_refs = all_ns_refs

    def get_iterator_by_table_id(self, table_id):
        # It seems to be the first namespace is "user" namespace, at least according to this code: 
        # https://github.com/google/picatrix/blob/a2f39766ad4b007b125dc8f84916e18fb3dc5478/picatrix/lib/utils.py
        for ns in self.all_ns_refs
            if table_id in ns:
                return ns[table_id]
        return None


def load_ipython_extension(ipython):
    # The `ipython` argument is the currently active `InteractiveShell`
    # instance, which can be used in any way. This allows you to register
    # new magics or aliases, for example.

    # The difference between line and cell magic is described here: https://jakevdp.github.io/PythonDataScienceHandbook/01.03-magic-commands.html.
    # In short: line magic only accepts one line of input whereas cell magic supports multiline input as magic command argument.
    # Both line and cell magic would make sense for RBQL queries but for MVP it should be enough to implement just the cell magic.

    from IPython.core.magic import register_line_magic
    from IPython.core.getipython import get_ipython

    ipython = ipython or get_ipython() # The pattern taken from here: https://github.com/pydoit/doit/blob/9efe141a5dc96d4912143561695af7fc4a076490/doit/tools.py
    # ipython is interactiveshell. Docs: https://ipython.readthedocs.io/en/stable/api/generated/IPython.core.interactiveshell.html

    @register_line_magic("rbql")
    def run_rbql_query(query_text):
        # Unfortunately globals() and locals() called from here won't contain user variables defined in the notebook.

        # FIXME add proper error handling
        tables_registry = IPythonDataframeRegistry(ipython.all_ns_refs)
        output_writer = rbql_pandas.DataframeWriter()
        # TODO make it possible to specify user_init_code in code cells
        rbql_engine.query(query_text, input_iterator=None, output_writer=output_writer, output_warnings=None, join_tables_registry=tables_registry, user_init_code='')
        return output_writer.result

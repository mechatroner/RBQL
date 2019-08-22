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

# PROCESS_SELECT_COMMON
__RBQLMP__init_vars_select_expression
if __RBQLMP__where_expression:
    out_fields = __RBQLMP__select_expression
    if aggregation_stage > 0:
        key = __RBQLMP__aggregation_key_expression
        select_aggregated(key, out_fields)
    else:
        sort_key = (__RBQLMP__sort_key_expression)
        if query_context.unnest_list is not None:
            if not select_unnested(sort_key, out_fields):
                stop_flag = True
        else:
            if not select_simple(sort_key, out_fields):
                stop_flag = True



# PROCESS_SELECT_SIMPLE
star_fields = record_a
__COMMON_SELECT_EXPRESSION_PLACEHOLDER__



# PROCESS_SELECT_JOIN
join_matches = join_map.get_rhs(query_context.lhs_join_var)
for join_match in join_matches:
    bNR, bNF, record_b = join_match
    star_fields = record_a + record_b
    __COMMON_SELECT_EXPRESSION_PLACEHOLDER__
    if stop_flag:
        break



# PROCESS_UPDATE_JOIN:
join_matches = join_map.get_rhs(query_context.lhs_join_var)
if len(join_matches) > 1:
    raise RbqlRuntimeError('More than one record in UPDATE query matched a key from the input table in the join table') # UT JSON # TODO output the failed key
if len(join_matches) == 1:
    bNR, bNF, record_b = join_matches[0]
else:
    bNR, bNF, record_b = None, None, None
up_fields = record_a[:]
__RBQLMP__init_vars_update_expression
if len(join_matches) == 1 and (__RBQLMP__where_expression):
    NU += 1
    __RBQLMP__update_expressions
if not writer.write(up_fields):
    stop_flag = True


# PROCESS_UPDATE_SIMPLE
up_fields = record_a[:]
__RBQLMP__init_vars_update_expression
if __RBQLMP__where_expression:
    NU += 1
    __RBQLMP__update_expressions
if not writer.write(up_fields)
    stop_flag = True


# MAIN_LOOP_BODY:
NR = 0
NU = 0
stop_flag = False
while not stop_flag:
    record_a = input_iterator.get_record()
    if record_a is None:
        break
    NR += 1
    NF = len(record_a)
    query_context.unnest_list = None # TODO optimize, don't need to set this every iteration
    try:
        __EXPRESSION_PLACEHOLDER__
    except InternalBadKeyError as e:
        raise RbqlRuntimeError('No "{}" field at record {}'.format(e.bad_key, NR)) # UT JSON
    except InternalBadFieldError as e:
        raise RbqlRuntimeError('No "a{}" field at record {}'.format(e.bad_idx + 1, NR)) # UT JSON
    except RbqlParsingError:
        raise
    except Exception as e:
        if debug_mode:
            raise
        if str(e).find('RBQLAggregationToken') != -1:
            raise RbqlParsingError(wrong_aggregation_usage_error) # UT JSON
        raise RbqlRuntimeError('At record ' + str(NR) + ', Details: ' + str(e)) # UT JSON
writer.finish()

try {
__RBQLMP__user_init_code
} catch (e) {
    throw new Error('Exception while executing user-provided init code: ' + e);
}


class RbqlRuntimeError extends Error {}


function InternalBadFieldError(idx) {
    this.idx = idx;
    this.name = 'InternalBadFieldError';
}



var unfold_list = null;

var module_was_used_failsafe = false;

// Aggregators:
var aggregation_stage = 0;
var aggr_init_counter = 0;
var functional_aggregators = [];

var writer = null;

var NU = 0; // NU - Num Updated. Alternative variables: NW (Num Where) - Not Practical. NW (Num Written) - Impossible to implement.
var NR = 0;

var finished_with_error = false;

var external_success_handler = null;
var external_error_handler = null;

var external_input_iterator = null;
var external_writer = null;
var external_join_map_impl = null;

var process_function = null;
var join_map = null;
var node_debug_mode_flag = false;


function finish_processing_error(error_type, error_msg) {
    if (finished_with_error)
        return;
    finished_with_error = true;
    // Stopping input_iterator to trigger exit procedure.
    external_input_iterator.finish();
    external_error_handler(error_type, error_msg);
}


function finish_processing_success() {
    if (finished_with_error)
        return;
    try {
        writer.finish(() => {
            var join_warnings = external_join_map_impl ? external_join_map_impl.get_warnings() : [];
            var warnings = join_warnings.concat(external_writer.get_warnings()).concat(external_input_iterator.get_warnings());
            external_success_handler(warnings);
        });
    } catch (e) {
        if (e instanceof RbqlRuntimeError) {
            finish_processing_error('query execution', e.message);
        } else {
            if (node_debug_mode_flag) {
                console.log('Unexpected exception, dumping stack trace:');
                console.log(e.stack);
            }
            finish_processing_error('unexpected', String(e));
        }
        return;
    }
}


function assert(condition, message) {
    if (!condition) {
        finish_processing_error('unexpected', message);
    }
}


function stable_compare(a, b) {
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i])
            return a[i] < b[i] ? -1 : 1;
    }
}


function safe_get(record, idx) {
    return idx < record.length ? record[idx] : null;
}


function safe_join_get(record, idx) {
    if (idx < record.length) {
        return record[idx];
    }
    throw new InternalBadFieldError(idx);
}


function safe_set(record, idx, value) {
    if (idx - 1 < record.length) {
        record[idx - 1] = value;
    } else {
        throw new InternalBadFieldError(idx - 1);
    }
}


function Marker(marker_id, value) {
    this.marker_id = marker_id;
    this.value = value;
    this.toString = function() {
        throw new RbqlRuntimeError('Unsupported aggregate expression');
    }
}


function UnfoldMarker() {}


function UNFOLD(vals) {
    if (unfold_list !== null) {
        // Technically we can support multiple UNFOLD's but the implementation/algorithm is more complex and just doesn't worth it
        throw new RbqlRuntimeError('Only one UNFOLD is allowed per query');
    }
    unfold_list = vals;
    return new UnfoldMarker();
}



function MinAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        // JS version doesn't need "NumHandler" hack like in Python impl because it has only one "number" type, no ints/floats
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, val);
        } else {
            this.stats.set(key, Math.min(cur_aggr, val));
        }
    }

    this.get_final = function(key) {
        return this.stats.get(key);
    }
}



function MaxAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, val);
        } else {
            this.stats.set(key, Math.max(cur_aggr, val));
        }
    }

    this.get_final = function(key) {
        return this.stats.get(key);
    }
}


function CountAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, 1);
        } else {
            this.stats.set(key, cur_aggr + 1);
        }
    }

    this.get_final = function(key) {
        return this.stats.get(key);
    }
}


function SumAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, val);
        } else {
            this.stats.set(key, cur_aggr + val);
        }
    }

    this.get_final = function(key) {
        return this.stats.get(key);
    }
}


function AvgAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, [val, 1]);
        } else {
            var cur_sum = cur_aggr[0];
            var cur_cnt = cur_aggr[1];
            this.stats.set(key, [cur_sum + val, cur_cnt + 1]);
        }
    }

    this.get_final = function(key) {
        var cur_aggr = this.stats.get(key);
        var cur_sum = cur_aggr[0];
        var cur_cnt = cur_aggr[1];
        var avg = cur_sum / cur_cnt;
        return avg;
    }
}


function VarianceAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, [val, val * val, 1]);
        } else {
            var cur_sum = cur_aggr[0];
            var cur_sum_sq = cur_aggr[1];
            var cur_cnt = cur_aggr[2];
            this.stats.set(key, [cur_sum + val, cur_sum_sq + val * val, cur_cnt + 1]);
        }
    }

    this.get_final = function(key) {
        var cur_aggr = this.stats.get(key);
        var cur_sum = cur_aggr[0];
        var cur_sum_sq = cur_aggr[1];
        var cur_cnt = cur_aggr[2];
        var avg_val = cur_sum / cur_cnt;
        var variance = cur_sum_sq / cur_cnt - avg_val * avg_val;
        return variance;
    }
}


function MedianAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parseFloat(val);
        var cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, [val]);
        } else {
            cur_aggr.push(val);
        }
    }

    this.get_final = function(key) {
        var cur_aggr = this.stats.get(key);
        cur_aggr.sort(function(a, b) { return a - b; });
        var m = Math.floor(cur_aggr.length / 2);
        if (cur_aggr.length % 2) {
            return cur_aggr[m];
        } else {
            return (cur_aggr[m - 1] + cur_aggr[m]) / 2.0;
        }
    }
}


function FoldAggregator(post_proc) {
    this.post_proc = post_proc;
    this.stats = new Map();

    this.increment = function(key, val) {
        let cur_aggr = this.stats.get(key);
        if (cur_aggr === undefined) {
            this.stats.set(key, [val]);
        } else {
            cur_aggr.push(val);
        }
    }

    this.get_final = function(key) {
        let cur_aggr = this.stats.get(key);
        return this.post_proc(cur_aggr);
    }
}


function SubkeyChecker() {
    this.subkeys = new Map();

    this.increment = function(key, subkey) {
        var old_subkey = this.subkeys.get(key);
        if (old_subkey === undefined) {
            this.subkeys.set(key, subkey);
        } else if (old_subkey != subkey) {
            throw 'Unable to group by "' + key + '", different values in output: "' + old_subkey + '" and "' + subkey + '"';
        }
    }

    this.get_final = function(key) {
        return this.subkeys.get(key);
    }
}


function init_aggregator(generator_name, val, post_proc=null) {
    aggregation_stage = 1;
    assert(aggr_init_counter == functional_aggregators.length, 'Unable to process aggregation expression');
    if (post_proc === null) {
        functional_aggregators.push(new generator_name());
    } else {
        functional_aggregators.push(new generator_name(post_proc));
    }
    var res = new Marker(aggr_init_counter, val);
    aggr_init_counter += 1;
    return res;
}


function MIN(val) {
    return aggregation_stage < 2 ? init_aggregator(MinAggregator, val) : val;
}


function MAX(val) {
    return aggregation_stage < 2 ? init_aggregator(MaxAggregator, val) : val;
}

function COUNT(val) {
    return aggregation_stage < 2 ? init_aggregator(CountAggregator, 1) : 1;
}

function SUM(val) {
    return aggregation_stage < 2 ? init_aggregator(SumAggregator, val) : val;
}

function AVG(val) {
    return aggregation_stage < 2 ? init_aggregator(AvgAggregator, val) : val;
}

function VARIANCE(val) {
    return aggregation_stage < 2 ? init_aggregator(VarianceAggregator, val) : val;
}

function MEDIAN(val) {
    return aggregation_stage < 2 ? init_aggregator(MedianAggregator, val) : val;
}

function FOLD(val, post_proc = v => v.join('|')) {
    return aggregation_stage < 2 ? init_aggregator(FoldAggregator, val, post_proc) : val;
}


function add_to_set(dst_set, value) {
    var len_before = dst_set.size;
    dst_set.add(value);
    return len_before != dst_set.size;
}


function TopWriter(subwriter) {
    this.subwriter = subwriter;
    this.NW = 0;

    this.write = function(record) {
        if (__RBQLMP__top_count !== null && this.NW >= __RBQLMP__top_count)
            return false;
        this.subwriter.write(record);
        this.NW += 1;
        return true;
    }

    this.finish = function(after_finish_callback) {
        this.subwriter.finish(after_finish_callback);
    }
}


function UniqWriter(subwriter) {
    this.subwriter = subwriter;
    this.seen = new Set();

    this.write = function(record) {
        if (!add_to_set(this.seen, JSON.stringify(record)))
            return true;
        if (!this.subwriter.write(record))
            return false;
        return true;
    }

    this.finish = function(after_finish_callback) {
        this.subwriter.finish(after_finish_callback);
    }
}


function UniqCountWriter(subwriter) {
    this.subwriter = subwriter;
    this.records = new Map();

    this.write = function(record) {
        var key = JSON.stringify(record);
        var old_val = this.records.get(key);
        if (old_val) {
            old_val[0] += 1;
        } else {
            this.records.set(key, [1, record]);
        }
        return true;
    }

    this.finish = function(after_finish_callback) {
        for (var [key, value] of this.records) {
            let [count, record] = value;
            record.unshift(count);
            if (!this.subwriter.write(record))
                break;
        }
        this.subwriter.finish(after_finish_callback);
    }
}


function SortedWriter(subwriter) {
    this.subwriter = subwriter;
    this.unsorted_entries = [];

    this.write = function(stable_entry) {
        this.unsorted_entries.push(stable_entry);
        return true;
    }

    this.finish = function(after_finish_callback) {
        var unsorted_entries = this.unsorted_entries;
        unsorted_entries.sort(stable_compare);
        if (__RBQLMP__reverse_flag)
            unsorted_entries.reverse();
        for (var i = 0; i < unsorted_entries.length; i++) {
            var entry = unsorted_entries[i];
            if (!this.subwriter.write(entry[entry.length - 1]))
                break;
        }
        this.subwriter.finish(after_finish_callback);
    }
}


function AggregateWriter(subwriter) {
    this.subwriter = subwriter;
    this.aggregators = [];
    this.aggregation_keys = new Set();

    this.finish = function(after_finish_callback) {
        var all_keys = Array.from(this.aggregation_keys);
        all_keys.sort();
        for (var i = 0; i < all_keys.length; i++) {
            var key = all_keys[i];
            var out_fields = [];
            for (var ag of this.aggregators) {
                out_fields.push(ag.get_final(key));
            }
            if (!this.subwriter.write(out_fields))
                break;
        }
        this.subwriter.finish(after_finish_callback);
    }
}



function FakeJoiner(join_map) {
    this.get_rhs = function(lhs_key) {
        return [null];
    }
}


function InnerJoiner(join_map) {
    this.join_map = join_map;

    this.get_rhs = function(lhs_key) {
        return this.join_map.get_join_records(lhs_key);
    }
}


function LeftJoiner(join_map) {
    this.join_map = join_map;
    this.null_record = [Array(join_map.max_record_len).fill(null)];

    this.get_rhs = function(lhs_key) {
        let result = this.join_map.get_join_records(lhs_key);
        if (result.length == 0) {
            return this.null_record;
        }
        return result;
    }
}


function StrictLeftJoiner(join_map) {
    this.join_map = join_map;

    this.get_rhs = function(lhs_key) {
        let result = this.join_map.get_join_records(lhs_key);
        if (result.length != 1) {
            throw new RbqlRuntimeError('In "STRICT LEFT JOIN" each key in A must have exactly one match in B. Bad A key: "' + lhs_key + '"');
        }
        return result;
    }
}


function select_except(src, except_fields) {
    let result = [];
    for (let i = 0; i < src.length; i++) {
        if (except_fields.indexOf(i) == -1)
            result.push(src[i]);
    }
    return result;
}


function process_update(NF, afields, rhs_records) {
    if (rhs_records.length > 1)
        throw new RbqlRuntimeError('More than one record in UPDATE query matched A-key in join table B');
    var bfields = null;
    if (rhs_records.length == 1)
        bfields = rhs_records[0];
    var up_fields = afields;
    __RBQLMP__init_column_vars_select
    if (rhs_records.length == 1 && (__RBQLMP__where_expression)) {
        NU += 1;
        __RBQLMP__update_statements
    }
    return writer.write(up_fields);
}


function select_simple(sort_key, out_fields) {
    if (__RBQLMP__sort_flag) {
        var sort_entry = sort_key.concat([NR, out_fields]);
        if (!writer.write(sort_entry))
            return false;
    } else {
        if (!writer.write(out_fields))
            return false;
    }
    return true;
}


function select_aggregated(key, transparent_values) {
    if (key !== null) {
        key = JSON.stringify(key);
    }
    if (aggregation_stage === 1) {
        if (!(writer instanceof TopWriter)) {
            throw new RbqlRuntimeError('Unable to use "ORDER BY" or "DISTINCT" keywords in aggregate query');
        }
        writer = new AggregateWriter(writer);
        for (var i = 0; i < transparent_values.length; i++) {
            var trans_value = transparent_values[i];
            if (trans_value instanceof Marker) {
                writer.aggregators.push(functional_aggregators[trans_value.marker_id]);
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value.value);
            } else {
                writer.aggregators.push(new SubkeyChecker());
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value);
            }
        }
        aggregation_stage = 2;
    } else {
        for (var i = 0; i < transparent_values.length; i++) {
            var trans_value = transparent_values[i];
            writer.aggregators[i].increment(key, trans_value);
        }
    }
    writer.aggregation_keys.add(key)
}


function select_unfolded(sort_key, folded_fields) {
    let out_fields = folded_fields.slice();
    let unfold_pos = folded_fields.findIndex(val => val instanceof UnfoldMarker);
    for (var i = 0; i < unfold_list.length; i++) {
        out_fields[unfold_pos] = unfold_list[i];
        if (!select_simple(sort_key, out_fields.slice()))
            return false;
    }
    return true;
}


function process_select(NF, afields, rhs_records) {
    for (var i = 0; i < rhs_records.length; i++) {
        unfold_list = null;
        var bfields = rhs_records[i];
        var star_fields = afields;
        if (bfields != null)
            star_fields = afields.concat(bfields);
        __RBQLMP__init_column_vars_update
        if (!(__RBQLMP__where_expression))
            continue;
        // TODO wrap all user expression in try/catch block to improve error reporting
        var out_fields = __RBQLMP__select_expression;
        if (aggregation_stage > 0) {
            var key = __RBQLMP__aggregation_key_expression;
            select_aggregated(key, out_fields);
        } else {
            var sort_key = [__RBQLMP__sort_key_expression];
            if (unfold_list !== null) {
                if (!select_unfolded(sort_key, out_fields))
                    return false;
            } else {
                if (!select_simple(sort_key, out_fields))
                    return false;
            }
        }
    }
    return true;
}


function process_record(record) {
    NR += 1;
    if (finished_with_error)
        return;
    try {
        do_process_record(record);
    } catch (e) {
        if (e instanceof InternalBadFieldError) {
            finish_processing_error('query execution', 'No "a' + (e.idx + 1) + '" column at record: ' + NR);
        } else if (e instanceof RbqlRuntimeError) {
            finish_processing_error('query execution', e.message);
        } else {
            if (node_debug_mode_flag) {
                console.log('Unexpected exception, dumping stack trace:');
                console.log(e.stack);
            }
            finish_processing_error('unexpected', `At record: ${NR}, Details: ${String(e)}`);
        }
    }
}


function do_process_record(afields) {
    let rhs_records = join_map.get_rhs(__RBQLMP__lhs_join_var);
    let NF = afields.length;
    if (!process_function(NF, afields, rhs_records)) {
        external_input_iterator.finish();
        return;
    }
}


function do_rb_transform(input_iterator, output_writer) {
    process_function = __RBQLMP__is_select_query ? process_select : process_update;
    var sql_join_type = {'VOID': FakeJoiner, 'JOIN': InnerJoiner, 'INNER JOIN': InnerJoiner, 'LEFT JOIN': LeftJoiner, 'STRICT LEFT JOIN': StrictLeftJoiner}['__RBQLMP__join_operation'];

    join_map = new sql_join_type(external_join_map_impl);

    writer = new TopWriter(output_writer);

    if ('__RBQLMP__writer_type' == 'uniq') {
        writer = new UniqWriter(writer);
    } else if ('__RBQLMP__writer_type' == 'uniq_count') {
        writer = new UniqCountWriter(writer);
    }

    if (__RBQLMP__sort_flag)
        writer = new SortedWriter(writer);

    input_iterator.set_record_callback(process_record);
    input_iterator.start();
}


function rb_transform(input_iterator, join_map_impl, output_writer, external_success_cb, external_error_cb, node_debug_mode=false) {
    node_debug_mode_flag = node_debug_mode;
    external_success_handler = external_success_cb;
    external_error_handler = external_error_cb;
    external_input_iterator = input_iterator;
    external_writer = output_writer;
    external_join_map_impl = join_map_impl;

    input_iterator.set_finish_callback(finish_processing_success);

    if (module_was_used_failsafe) {
        finish_processing_error('unexpected', 'Module can only be used once');
        return;
    }
    module_was_used_failsafe = true;

    try {
        if (external_join_map_impl !== null) {
            external_join_map_impl.build(function() { do_rb_transform(input_iterator, output_writer); }, finish_processing_error);
        } else {
            do_rb_transform(input_iterator, output_writer);
        }

    } catch (e) {
        if (e instanceof RbqlRuntimeError) {
            finish_processing_error('query execution', e.message);
        } else {
            if (node_debug_mode_flag) {
                console.log('Unexpected exception, dumping stack trace:');
                console.log(e.stack);
            }
            finish_processing_error('unexpected', String(e));
        }
    }
}


module.exports.rb_transform = rb_transform;

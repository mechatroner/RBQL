try {
__RBQLMP__user_init_code
} catch (e) {
    throw new Error('Exception while executing user-provided init code: ' + e);
}


var unfold_list = null;

var module_was_used_failsafe = false;

// Aggregators:
var aggregation_stage = 0;
var aggr_init_counter = 0;
var functional_aggregators = [];

var writer = null;

var NU = 0; // NU - Num Updated. Alternative variables: NW (Num Where) - Not Practical. NW (Num Written) - Impossible to implement.


function assert(condition, message) {
    if (!condition) {
        finish_processing_error(message);
    }
}


function stable_compare(a, b) {
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i])
            return a[i] < b[i] ? -1 : 1;
    }
}


function InternalBadFieldError(idx) {
    this.idx = idx;
    this.name = 'InternalBadFieldError';
}


function RbqlRuntimeError(error_msg) {
    this.error_msg = error_msg;
    this.name = 'RbqlError';
}


function safe_join_get(record, idx) {
    if (idx - 1 < record.length) {
        return record[idx - 1];
    }
    throw new InternalBadFieldError(idx - 1);
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
        throw new RbqlError('Unsupported aggregate expression');
    }
}


function UNFOLD(vals) {
    if (unfold_list !== null) {
        // Technically we can support multiple UNFOLD's but the implementation/algorithm is more complex and just doesn't worth it
        throw new RbqlError('Only one UNFOLD is allowed per query');
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
            this.stats.set(key, cur_aggr); // Do we really need to do this? mutable cur_aggr already holds a reference to the value
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
            this.stats.set(key, cur_aggr); // Do we really need to do this? mutable cur_aggr already holds a reference to the value
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
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MinAggregator, val) : val;
}


function MAX(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MaxAggregator, val) : val;
}

function COUNT(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.CountAggregator, 1) : 1;
}

function SUM(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.SumAggregator, val) : val;
}

function AVG(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.AvgAggregator, val) : val;
}

function VARIANCE(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.VarianceAggregator, val) : val;
}

function MEDIAN(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MedianAggregator, val) : val;
}

function FOLD(val, post_proc = v => v.join('|')) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.FoldAggregator, val, post_proc) : val;
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

    this.finish = function() {
        this.subwriter.finish();
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

    this.finish = function() {
        this.subwriter.finish();
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

    this.finish = function() {
        for (var [key, value] of this.records) {
            let [count, record] = value;
            record.unshift(count);
            if (!this.subwriter.write(record))
                break;
        }
        this.subwriter.finish();
    }
}


function SortedWriter(subwriter) {
    this.subwriter = subwriter;
    this.unsorted_entries = [];

    this.write = function(stable_entry) {
        this.unsorted_entries.push(stable_entry);
        return true;
    }

    this.finish = function() {
        var unsorted_entries = this.unsorted_entries;
        unsorted_entries.sort(stable_compare);
        if (__RBQLMP__reverse_flag)
            unsorted_entries.reverse();
        for (var i = 0; i < unsorted_entries.length; i++) {
            var entry = unsorted_entries[i];
            if (!this.subwriter.write(entry[entry.length - 1]))
                break;
        }
        this.subwriter.finish();
    }
}


function AggregateWriter(subwriter) {
    this.subwriter = subwriter;
    this.aggregators = [];
    this.aggregation_keys = new Set();

    this.finish = function() {
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
        this.subwriter.finish();
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
            throw new RbqlError('In "STRICT LEFT JOIN" each key in A must have exactly one match in B. Bad A key: "' + lhs_key + '"');
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
        throw new RbqlError('More than one record in UPDATE query matched A-key in join table B');
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
            throw new RbqlError('Unable to use "ORDER BY" or "DISTINCT" keywords in aggregate query');
        }
        writer = new AggregateWriter(writer);
        for (var i = 0; i < transparent_values.length; i++) {
            var trans_value = transparent_values[i];
            if (trans_value instanceof Marker) {
                writer.aggregators.push(functional_aggregators[trans_value.marker_id]);
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value.value);
            } else {
                writer.aggregators.push(new rbql_utils.SubkeyChecker());
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
        if (!select_simple(sort_key, out_fields))
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


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// OLD CODE:
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const fs = require('fs');
const readline = require('readline');
const rbql_utils = require('__RBQLMP__rbql_home_dir/rbql_utils.js');

try {
__RBQLMP__user_init_code
} catch (e) {
    throw new Error('Exception while executing user-provided init code: ' + e);
}


var csv_encoding = '__RBQLMP__csv_encoding';

var input_delim = '__RBQLMP__input_delim';
var join_delim = '__RBQLMP__join_delim';
var output_delim = '__RBQLMP__output_delim';

var join_table_path = __RBQLMP__rhs_table_path;

var module_was_used_failsafe = false;

var external_error_handler = null;
var external_success_handler = null;

var enable_monocolumn_csv_ux_optimization_hack = true;

var NR = 0;
var NU = 0; // NU - Num Updated. Alternative variables: NW (Num Where) - Not Practical. NW (Num Written) - Impossible to implement.

var line_reader = null;
var dst_stream = null;
var writer = null;

// For warnings:
var join_fields_info = new Object();
var input_fields_info = new Object();
var null_value_in_output = false;
var delim_in_simple_output = false;
var output_switch_to_csv = null; // Need this as warning because user could use TSV files and CSV would be unexpected. Also this is a marker for editor to use CSV syntax for output instead of  monocolumn.
var utf8_bom_removed = false;
var defective_csv_line_in_input = 0;
var defective_csv_line_in_join = 0;

// Aggregators:
var aggregation_stage = 0;
var aggr_init_counter = 0;
var functional_aggregators = [];

var process_function = __RBQLMP__is_select_query ? process_select : process_update;
var join_function = {'VOID': null_join, 'JOIN': inner_join, 'INNER JOIN': inner_join, 'LEFT JOIN': left_join, 'STRICT LEFT JOIN': strict_left_join}['__RBQLMP__join_operation'];

var output_join = null;
var input_split = null;
var join_split = null;

var join_map = null;
var max_join_fields = null;
var null_record = null;

var finished_with_error = false;

var unfold_list = null;

function finish_processing_error(error_msg) {
    finished_with_error = true;
    if (line_reader)
        line_reader.close();
    external_error_handler(error_msg);
}

// TODO utf8 (improvements) and tests

// TODO get rid of '_py' and '_js' suffixes in rbql.py/rbql.js (except for interface functions)


function assert(condition, message) {
    if (!condition) {
        finish_processing_error(message);
    }
}


function BadFieldError(idx) {
    this.idx = idx;
    this.name = 'BadFieldError';
}


function RbqlError(error_msg) {
    this.error_msg = error_msg;
    this.name = 'RbqlError';
}


function quote_field(src, delim) {
    if (src.indexOf('"') != -1 || src.indexOf(delim) != -1) {
        var escaped = src.replace(/"/g, '""');
        escaped = '"' + escaped + '"';
        return escaped;
    }
    return src;
}


function quoted_join(fields, delim) {
    var quoted_fields = fields.map(function(v) { return quote_field(String(v), delim); });
    return quoted_fields.join(delim);
}


function mono_join(fields, delim) {
    if (enable_monocolumn_csv_ux_optimization_hack && '__RBQLMP__input_policy' == 'monocolumn') {
        if (output_switch_to_csv === null) {
            output_switch_to_csv = fields.length > 1;
        }
        assert(output_switch_to_csv === fields.length > 1, 'Monocolumn optimization logic failure');
        if (output_switch_to_csv) {
            return quoted_join(fields, ',');
        } else {
            return fields[0];
        }
    }
    if (fields.length > 1) {
        throw new RbqlError('Unable to use "Monocolumn" output format: some records have more than one field');
    }
    return fields[0];
}


function simple_join(fields, delim) {
    var res = fields.join(delim);
    if (fields.join('').indexOf(delim) != -1) {
        delim_in_simple_output = true;
    }
    return res;
}


function Marker(marker_id, value) {
    this.marker_id = marker_id;
    this.value = value;
    this.toString = function() {
        throw new RbqlError('Unsupported aggregate expression');
    }
}

function UnfoldMarker() {}

function UNFOLD(vals) {
    if (unfold_list !== null) {
        // Technically we can support multiple UNFOLD's but the implementation/algorithm is more complex and just doesn't worth it
        throw new RbqlError('Only one UNFOLD is allowed per query');
    }
    unfold_list = vals;
    return new UnfoldMarker();
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
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MinAggregator, val) : val;
}


function MAX(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MaxAggregator, val) : val;
}

function COUNT(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.CountAggregator, 1) : 1;
}

function SUM(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.SumAggregator, val) : val;
}

function AVG(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.AvgAggregator, val) : val;
}

function VARIANCE(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.VarianceAggregator, val) : val;
}

function MEDIAN(val) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.MedianAggregator, val) : val;
}

function FOLD(val, post_proc = v => v.join('|')) {
    return aggregation_stage < 2 ? init_aggregator(rbql_utils.FoldAggregator, val, post_proc) : val;
}


function add_to_set(dst_set, value) {
    var len_before = dst_set.size;
    dst_set.add(value);
    return len_before != dst_set.size;
}


function stable_compare(a, b) {
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i])
            return a[i] < b[i] ? -1 : 1;
    }
}


function SimpleWriter(dst) {
    this.dst = dst;
    this.NW = 0;
    this.write = function(record) {
        if (__RBQLMP__top_count !== null && this.NW >= __RBQLMP__top_count)
            return false;
        this.dst.write(record);
        this.dst.write('\n');
        this.NW += 1;
        return true;
    }
    this.finish = function() {}
}


function UniqWriter(dst) {
    this.dst = dst;
    this.seen = new Set();
    this.NW = 0;
    this.write = function(record) {
        if (__RBQLMP__top_count !== null && this.NW >= __RBQLMP__top_count)
            return false;
        if (add_to_set(this.seen, record)) {
            this.dst.write(record);
            this.dst.write('\n');
            this.NW += 1;
        }
        return true;
    }
    this.finish = function() {}
}


function UniqCountWriter(dst) {
    this.dst = dst;
    this.records = new Map();
    this.write = function(record) {
        var old_val = this.records.get(record);
        if (old_val) {
            this.records.set(record, old_val + 1);
        } else {
            this.records.set(record, 1);
        }
        return true;
    }
    this.finish = function() {
        var NW = 0;
        for (var [record, count] of this.records) {
            if (__RBQLMP__top_count !== null && NW >= __RBQLMP__top_count)
                break;
            var new_record = count + output_delim + record + '\n';
            this.dst.write(new_record);
            NW += 1;
        }
    }
}


function SortedWriter(subwriter) {
    this.subwriter = subwriter;
    this.unsorted_entries = [];

    this.write = function(stable_entry) {
        this.unsorted_entries.push(stable_entry);
        return true;
    }

    this.finish = function() {
        var unsorted_entries = this.unsorted_entries;
        unsorted_entries.sort(stable_compare);
        if (__RBQLMP__reverse_flag)
            unsorted_entries.reverse();
        for (var i = 0; i < unsorted_entries.length; i++) {
            var entry = unsorted_entries[i];
            if (!this.subwriter.write(entry[entry.length - 1]))
                break;
        }
        this.subwriter.finish();
    }
}


function AggregateWriter(subwriter) {
    this.subwriter = subwriter;
    this.aggregators = [];
    this.aggregation_keys = new Set();

    this.finish = function() {
        var all_keys = Array.from(this.aggregation_keys);
        all_keys.sort();
        for (var i = 0; i < all_keys.length; i++) {
            var key = all_keys[i];
            var out_fields = [];
            for (var ag of this.aggregators) {
                out_fields.push(ag.get_final(key));
            }
            var out_record = output_join(out_fields, output_delim);
            if (!this.subwriter.write(out_record))
                break;
        }
        this.subwriter.finish();
    }
}


function safe_join_get(record, idx) {
    if (idx - 1 < record.length) {
        return record[idx - 1];
    }
    throw new BadFieldError(idx - 1);
}


function safe_set(record, idx, value) {
    if (idx - 1 < record.length) {
        record[idx - 1] = value;
    } else {
        throw new BadFieldError(idx - 1);
    }
}


function read_join_table(table_path) {
    var fields_max_len = 0;
    var content = fs.readFileSync(table_path, {encoding: csv_encoding});
    var lines = content.split(/(?:\r\n)|\r|\n/);
    var result = new Map();
    for (var i = 0; i < lines.length; i++) {
        var line = lines[i];
        if (i === 0) {
            var clean_line = remove_utf8_bom(line, csv_encoding);
            if (clean_line != line) {
                line = clean_line;
                utf8_bom_removed = true;
            }
        }
        if (i + 1 == lines.length && line.length == 0)
            break;
        var split_result = join_split(line);
        var bfields = split_result[0];
        var warning = split_result[1];
        if (warning && !defective_csv_line_in_join)
            defective_csv_line_in_join = i + 1;
        var num_fields = bfields.length;
        if (!join_fields_info.hasOwnProperty(num_fields))
            join_fields_info[num_fields] = i + 1;
        fields_max_len = Math.max(fields_max_len, num_fields);
        var key = null;
        try {
            key = __RBQLMP__rhs_join_var;
        } catch (e) {
            if (e instanceof BadFieldError) {
                throw new RbqlError('No "b' + (e.idx + 1) + '" column at line: ' + (i + 1) + ' in "B" table')
            }
        }
        var old_records = result.get(key);
        if (old_records == null) {
            result.set(key, [bfields]);
        } else {
            old_records.push(bfields);
        }
    }
    return [result, fields_max_len];
}


function select_except(src, except_fields) {
    let result = [];
    for (let i = 0; i < src.length; i++) {
        if (except_fields.indexOf(i) == -1)
            result.push(src[i]);
    }
    return result;
}


function generate_warnings() {
    var warnings = new Object();
    if (null_value_in_output)
        warnings['null_value_in_output'] = true;
    if (delim_in_simple_output)
        warnings['delim_in_simple_output'] = true;
    if (output_switch_to_csv)
        warnings['output_switch_to_csv'] = true;
    if (utf8_bom_removed)
        warnings['utf8_bom_removed'] = true;
    if (defective_csv_line_in_input)
        warnings['defective_csv_line_in_input'] = defective_csv_line_in_input;
    if (defective_csv_line_in_join)
        warnings['defective_csv_line_in_join'] = defective_csv_line_in_join;
    if (Object.keys(input_fields_info).length > 1)
        warnings['input_fields_info'] = input_fields_info;
    if (Object.keys(join_fields_info).length > 1)
        warnings['join_fields_info'] = join_fields_info;
    if (!Object.keys(warnings).length)
        return null;
    return warnings;
}


function null_join(lhs_key) {
    return null;
}

function inner_join(lhs_key) {
    var result = join_map.get(lhs_key);
    if (result == null)
        return [];
    return result;
}


function left_join(lhs_key) {
    var result = join_map.get(lhs_key);
    if (result == null)
        return null_record;
    return result;
}


function strict_left_join(lhs_key) {
    var result = join_map.get(lhs_key);
    if (result == null || result.length != 1) {
        throw new RbqlError('In "STRICT LEFT JOIN" each key in A must have exactly one match in B. Bad A key: "' + lhs_key + '"');
    }
    return result;
}


function replace_null_values(out_fields) {
    for (var i = 0; i < out_fields.length; i++) {
        if (out_fields[i] == null) {
            null_value_in_output = true;
            out_fields[i] = '';
        }
    }
}


function process_update(NF, afields, rhs_records) {
    if (rhs_records != null && rhs_records.length > 1)
        throw new RbqlError('More than one record in UPDATE query matched A-key in join table B at line: ' + NR);
    var bfields = null;
    if (rhs_records != null && rhs_records.length == 1)
        bfields = rhs_records[0];
    var up_fields = afields;
    __RBQLMP__init_column_vars_select
    if ((rhs_records == null || rhs_records.length == 1) && (__RBQLMP__where_expression)) {
        NU += 1;
        __RBQLMP__update_statements
    }
    replace_null_values(up_fields);
    var out_record = output_join(up_fields, output_delim);
    return writer.write(out_record);
}


function select_simple(sort_key, out_fields) {
    replace_null_values(out_fields);
    var out_record = output_join(out_fields, output_delim);
    if (__RBQLMP__sort_flag) {
        var sort_entry = sort_key.concat([NR, out_record]);
        if (!writer.write(sort_entry))
            return false;
    } else {
        if (!writer.write(out_record))
            return false;
    }
    return true;
}


function select_aggregated(key, transparent_values) {
    if (key !== null) {
        key = output_join(key, output_delim);
    }
    if (aggregation_stage === 1) {
        if (!(writer instanceof SimpleWriter)) {
            throw new RbqlError('Unable to use "ORDER BY" or "DISTINCT" keywords in aggregate query');
        }
        writer = new AggregateWriter(writer);
        for (var i = 0; i < transparent_values.length; i++) {
            var trans_value = transparent_values[i];
            if (trans_value instanceof Marker) {
                writer.aggregators.push(functional_aggregators[trans_value.marker_id]);
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value.value);
            } else {
                writer.aggregators.push(new rbql_utils.SubkeyChecker());
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
        if (!select_simple(sort_key, out_fields))
            return false;
    }
    return true;
}


function process_select(NF, afields, rhs_records) {
    if (rhs_records == null)
        rhs_records = [null];
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


function remove_utf8_bom(line, assumed_source_encoding) {
    if (assumed_source_encoding == 'binary' && line.length >= 3 && line.charCodeAt(0) === 0xEF && line.charCodeAt(1) === 0xBB && line.charCodeAt(2) === 0xBF) {
        return line.substring(3);
    }
    if (assumed_source_encoding == 'utf-8' && line.length >= 1 && line.charCodeAt(0) === 0xFEFF) {
        return line.substring(1);
    }
    return line;
}


function process_line(line) {
    if (finished_with_error)
        return; // Sometimes linereader still calls "line" callback even after "close()" method has been called.
    try {
        do_process_line(line);
    } catch (e) {
        if (e instanceof BadFieldError) {
            finish_processing_error('No "a' + (e.idx + 1) + '" column at line: ' + NR);
        } else if (e instanceof RbqlError) {
            finish_processing_error(e.error_msg);
        } else {
            finish_processing_error('Unexpected exception while processing line ' + NR + ': "' + e + '"');
        }
    }
}


function do_process_line(line) {
    if (NR === 0) {
        var clean_line = remove_utf8_bom(line, csv_encoding);
        if (clean_line != line) {
            line = clean_line;
            utf8_bom_removed = true;
        }
    }
    NR += 1;
    var split_result = input_split(line);
    var afields = split_result[0];
    var warning = split_result[1];
    if (warning && !defective_csv_line_in_input)
        defective_csv_line_in_input = NR;
    var NF = afields.length;
    if (!input_fields_info.hasOwnProperty(NF))
        input_fields_info[NF] = NR;
    var rhs_records = null;
    if (join_map != null)
        rhs_records = join_function(__RBQLMP__lhs_join_var);
    if (!process_function(NF, afields, rhs_records)) {
        line_reader.close();
        return;
    }
}


function finish_processing_success() {
    if (finished_with_error)
        return;
    try {
        writer.finish();
        var warnings = generate_warnings();
    } catch (e) {
        if (e instanceof RbqlError) {
            finish_processing_error(e.error_msg);
        } else {
            finish_processing_error('Unexpected exception: ' + e);
        }
        return;
    }
    external_success_handler(warnings);
}


function do_run_encapsulated(input_reader, output_stream) {
    // TODO add join_reader
    if (module_was_used_failsafe) {
        throw new RbqlError('Module can only be used once');
    }
    module_was_used_failsafe = true;

    line_reader = input_reader;
    dst_stream = output_stream;

    if ('__RBQLMP__output_policy' == 'simple') {
        output_join = simple_join;
    } else if ('__RBQLMP__output_policy' == 'quoted') {
        output_join = quoted_join;
    } else if ('__RBQLMP__output_policy' == 'monocolumn') {
        output_join = mono_join;
    } else if ('__RBQLMP__output_policy' == 'whitespace') {
        output_join = simple_join;
    } else {
        throw new RbqlError('unknown output csv policy');
    }


    if ('__RBQLMP__input_policy' == 'simple') {
        input_split = function(line) { return [line.split(input_delim), false]; }
    } else if ('__RBQLMP__input_policy' == 'quoted') {
        input_split = function(line) { return rbql_utils.split_quoted_str(line, input_delim); }
    } else if ('__RBQLMP__input_policy' == 'monocolumn') {
        input_split = function(line) { return [[line], false]; }
    } else if ('__RBQLMP__input_policy' == 'whitespace') {
        input_split = function(line) { return [rbql_utils.split_whitespace_separated_str(line), false]; }
    } else {
        throw new RbqlError('unknown input csv policy');
    }

    if ('__RBQLMP__join_policy' == '') {
        join_split = function(line) { return [null, null]; }
    } else if ('__RBQLMP__join_policy' == 'simple') {
        join_split = function(line) { return [line.split(join_delim), false]; }
    } else if ('__RBQLMP__join_policy' == 'quoted') {
        join_split = function(line) { return rbql_utils.split_quoted_str(line, join_delim); }
    } else if ('__RBQLMP__join_policy' == 'monocolumn') {
        join_split = function(line) { return [[line], false]; }
    } else if ('__RBQLMP__join_policy' == 'whitespace') {
        join_split = function(line) { return [rbql_utils.split_whitespace_separated_str(line), false]; }
    } else {
        throw new RbqlError('unknown join csv policy');
    }

    if (join_table_path !== null) {
        var join_params = read_join_table(join_table_path);
        join_map = join_params[0];
        max_join_fields = join_params[1];
        null_record = [Array(max_join_fields).fill(null)];
    }

    var writer_type = {'simple': SimpleWriter, 'uniq': UniqWriter, 'uniq_count': UniqCountWriter}['__RBQLMP__writer_type'];
    writer = new writer_type(dst_stream);
    if (__RBQLMP__sort_flag) {
        writer = new SortedWriter(writer);
    }

    line_reader.on('line', process_line);
    line_reader.on('close', finish_processing_success);
}


function run_encapsulated(input_reader, output_stream, external_success_cb, external_error_cb) {
    // This is the future worker interface (it is internal for now)
    external_success_handler = external_success_cb;
    external_error_handler = external_error_cb;
    try {
        do_run_encapsulated(input_reader, output_stream);
    } catch (e) {
        if (e instanceof RbqlError) {
            finish_processing_error(e.error_msg);
        } else {
            finish_processing_error('Unexpected exception: ' + e);
        }
    }
}


function run_on_node(external_success_cb, external_error_cb) {
    var input_reader = null;
    var src_table_path = __RBQLMP__src_table_path;
    if (src_table_path != null) {
        input_reader = readline.createInterface({ input: fs.createReadStream(src_table_path, {encoding: csv_encoding}) });
        // Should be the same as:
        // input_reader = readline.createInterface({ input: fs.createReadStream(src_table_path) });
        // input_reader.setEncoding(csv_encoding);
    } else {
        process.stdin.setEncoding(csv_encoding);
        input_reader = readline.createInterface({ input: process.stdin });
    }

    var output_stream = null;
    var dst_table_path = __RBQLMP__dst_table_path;
    if (dst_table_path != null) {
        output_stream = fs.createWriteStream(dst_table_path, {defaultEncoding: csv_encoding});
    } else {
        process.stdout.setDefaultEncoding(csv_encoding);
        output_stream = process.stdout;
    }
    run_encapsulated(input_reader, output_stream, external_success_cb, external_error_cb);
}


function cli_success_cb(warnings) {
    if (warnings !== null) {
        var warnings_report = JSON.stringify({'warnings': warnings});
        process.stderr.write(warnings_report);
    }
}


function cli_error_cb(error_msg) {
    var report = new Object();
    report.error = error_msg
    process.stderr.write(JSON.stringify(report));
    process.exit(1);
}


if (require.main === module) {
    run_on_node(cli_success_cb, cli_error_cb);
}


module.exports.run_on_node = run_on_node;

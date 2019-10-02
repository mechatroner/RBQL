__RBQLMP__user_init_code


class RbqlParsingError extends Error {}
class RbqlRuntimeError extends Error {}


function InternalBadFieldError(idx) {
    this.idx = idx;
    this.name = 'InternalBadFieldError';
}



var unnest_list = null;

var module_was_used_failsafe = false;

// Aggregators:
var aggregation_stage = 0;
var functional_aggregators = [];

var writer = null;

var NU = 0; // NU - Num Updated. Alternative variables: NW (Num Where) - Not Practical. NW (Num Written) - Impossible to implement.
var NR = 0;
var NF = 0;

var finished_with_error = false;
var input_finished = false;

var polymorphic_process = null;
var join_map = null;

const wrong_aggregation_usage_error = 'Usage of RBQL aggregation functions inside JavaScript expressions is not allowed, see the docs';


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
    if (idx < record.length) {
        record[idx] = value;
    } else {
        throw new InternalBadFieldError(idx);
    }
}


function RBQLAggregationToken(marker_id, value) {
    this.marker_id = marker_id;
    this.value = value;
    this.toString = function() {
        throw new RbqlParsingError(wrong_aggregation_usage_error);
    }
}


function UnnestMarker() {}


function UNNEST(vals) {
    if (unnest_list !== null) {
        // Technically we can support multiple UNNEST's but the implementation/algorithm is more complex and just doesn't worth it
        throw new RbqlParsingError('Only one UNNEST is allowed per query');
    }
    unnest_list = vals;
    return new UnnestMarker();
}
const unnest = UNNEST;
const Unnest = UNNEST;
const UNFOLD = UNNEST; // "UNFOLD" is deprecated, just for backward compatibility


function parse_number(val) {
    // We can do a more pedantic number test like `/^ *-{0,1}[0-9]+\.{0,1}[0-9]* *$/.test(val)`, but  user will probably use just Number(val) or parseInt/parseFloat
    let result = Number(val);
    if (isNaN(result)) {
        throw new RbqlRuntimeError(`Unable to convert value "${val}" to number. MIN, MAX, SUM, AVG, MEDIAN and VARIANCE aggregate functions convert their string arguments to numeric values`);
    }
    return result;
}


function MinAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parse_number(val);
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
        val = parse_number(val);
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


function SumAggregator() {
    this.stats = new Map();

    this.increment = function(key, val) {
        val = parse_number(val);
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
        val = parse_number(val);
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
        val = parse_number(val);
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
        val = parse_number(val);
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


function ArrayAggAggregator(post_proc=null) {
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
        if (this.post_proc === null)
            return cur_aggr;
        return this.post_proc(cur_aggr);
    }
}


function ConstGroupVerifier(output_index) {
    this.output_index = output_index;
    this.const_values = new Map();

    this.increment = function(key, value) {
        var old_value = this.const_values.get(key);
        if (old_value === undefined) {
            this.const_values.set(key, value);
        } else if (old_value != value) {
            throw new RbqlRuntimeError(`Invalid aggregate expression: non-constant values in output column ${this.output_index + 1}. E.g. "${old_value}" and "${value}"`);
        }
    }

    this.get_final = function(key) {
        return this.const_values.get(key);
    }
}


function init_aggregator(generator_name, val, post_proc=null) {
    aggregation_stage = 1;
    var res = new RBQLAggregationToken(functional_aggregators.length, val);
    if (post_proc === null) {
        functional_aggregators.push(new generator_name());
    } else {
        functional_aggregators.push(new generator_name(post_proc));
    }
    return res;
}


function MIN(val) {
    return aggregation_stage < 2 ? init_aggregator(MinAggregator, val) : val;
}
const min = MIN;
const Min = MIN;


function MAX(val) {
    return aggregation_stage < 2 ? init_aggregator(MaxAggregator, val) : val;
}
const max = MAX;
const Max = MAX;

function COUNT(val) {
    return aggregation_stage < 2 ? init_aggregator(CountAggregator, 1) : 1;
}
const count = COUNT;
const Count = COUNT;

function SUM(val) {
    return aggregation_stage < 2 ? init_aggregator(SumAggregator, val) : val;
}
const sum = SUM;
const Sum = SUM;

function AVG(val) {
    return aggregation_stage < 2 ? init_aggregator(AvgAggregator, val) : val;
}
const avg = AVG;
const Avg = AVG;

function VARIANCE(val) {
    return aggregation_stage < 2 ? init_aggregator(VarianceAggregator, val) : val;
}
const variance = VARIANCE;
const Variance = VARIANCE;

function MEDIAN(val) {
    return aggregation_stage < 2 ? init_aggregator(MedianAggregator, val) : val;
}
const median = MEDIAN;
const Median = MEDIAN;

function ARRAY_AGG(val, post_proc=null) {
    return aggregation_stage < 2 ? init_aggregator(ArrayAggAggregator, val, post_proc) : val;
}
const array_agg = ARRAY_AGG;
const FOLD = ARRAY_AGG; // "FOLD" is deprecated, just for backward compatibility


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

    this.finish = async function() {
        await this.subwriter.finish();
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

    this.finish = async function() {
        await this.subwriter.finish();
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

    this.finish = async function() {
        for (var [key, value] of this.records) {
            let [count, record] = value;
            record.unshift(count);
            if (!this.subwriter.write(record))
                break;
        }
        await this.subwriter.finish();
    }
}


function SortedWriter(subwriter) {
    this.subwriter = subwriter;
    this.unsorted_entries = [];

    this.write = function(stable_entry) {
        this.unsorted_entries.push(stable_entry);
        return true;
    }

    this.finish = async function() {
        var unsorted_entries = this.unsorted_entries;
        unsorted_entries.sort(stable_compare);
        if (__RBQLMP__reverse_flag)
            unsorted_entries.reverse();
        for (var i = 0; i < unsorted_entries.length; i++) {
            var entry = unsorted_entries[i];
            if (!this.subwriter.write(entry[entry.length - 1]))
                break;
        }
        await this.subwriter.finish();
    }
}


function AggregateWriter(subwriter) {
    this.subwriter = subwriter;
    this.aggregators = [];
    this.aggregation_keys = new Set();

    this.finish = async function() {
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
        await this.subwriter.finish();
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
        // FIXME nr, nf handling
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
        // FIXME nr, nf handling
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


function process_update(record_a, rhs_records) {
    if (rhs_records.length > 1)
        throw new RbqlRuntimeError('More than one record in UPDATE query matched A-key in join table B');
    var record_b = null;
    if (rhs_records.length == 1)
        record_b = rhs_records[0];
    var up_fields = record_a;
    __RBQLMP__init_column_vars_update
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
            throw new RbqlParsingError('Unable to use "ORDER BY" or "DISTINCT" keywords in aggregate query');
        }
        writer = new AggregateWriter(writer);
        let num_aggregators_found = 0;
        for (var i = 0; i < transparent_values.length; i++) {
            var trans_value = transparent_values[i];
            if (trans_value instanceof RBQLAggregationToken) {
                writer.aggregators.push(functional_aggregators[trans_value.marker_id]);
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value.value);
                num_aggregators_found += 1;
            } else {
                writer.aggregators.push(new ConstGroupVerifier(writer.aggregators.length));
                writer.aggregators[writer.aggregators.length - 1].increment(key, trans_value);
            }
        }
        if (num_aggregators_found != functional_aggregators.length) {
            throw new RbqlParsingError(wrong_aggregation_usage_error);
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


function select_unnested(sort_key, folded_fields) {
    let out_fields = folded_fields.slice();
    let unnest_pos = folded_fields.findIndex(val => val instanceof UnnestMarker);
    for (var i = 0; i < unnest_list.length; i++) {
        out_fields[unnest_pos] = unnest_list[i];
        if (!select_simple(sort_key, out_fields.slice()))
            return false;
    }
    return true;
}


function process_select(record_a, rhs_records) {
    for (var i = 0; i < rhs_records.length; i++) {
        unnest_list = null;
        var record_b = rhs_records[i];
        var star_fields = record_a;
        if (record_b != null)
            star_fields = record_a.concat(record_b);
        __RBQLMP__init_column_vars_select
        if (!(__RBQLMP__where_expression))
            continue;
        // TODO wrap all user expression in try/catch block to improve error reporting
        var out_fields = __RBQLMP__select_expression;
        if (aggregation_stage > 0) {
            var key = __RBQLMP__aggregation_key_expression;
            select_aggregated(key, out_fields);
        } else {
            var sort_key = [__RBQLMP__sort_key_expression];
            if (unnest_list !== null) {
                if (!select_unnested(sort_key, out_fields))
                    return false;
            } else {
                if (!select_simple(sort_key, out_fields))
                    return false;
            }
        }
    }
    return true;
}


async function do_rb_transform(input_iterator, join_map, output_writer) {
    polymorphic_process = __RBQLMP__is_select_query ? process_select : process_update;

    writer = new TopWriter(output_writer);

    if (__RBQLMP__writer_type == 'uniq') {
        writer = new UniqWriter(writer);
    } else if (__RBQLMP__writer_type == 'uniq_count') {
        writer = new UniqCountWriter(writer);
    }

    if (__RBQLMP__sort_flag)
        writer = new SortedWriter(writer);

    while (!finished_with_error) {
        let record_a = await input_iterator.get_record();
        if (record_a === null)
            break;
        NR += 1;
        let rhs_records = join_map.get_rhs(__RBQLMP__lhs_join_var);
        NF = record_a.length;
        if (!polymorphic_process(record_a, rhs_records)) {
            input_iterator.finish();
            break;
        }
    }
    if (output_writer.hasOwnProperty('finish'))
        await writer.finish();
    return input_iterator.get_warnings();
}


async function rb_transform(input_iterator, join_map_impl, output_writer) {
    if (module_was_used_failsafe) {
        throw new Error('Module can only be used once');
    }
    module_was_used_failsafe = true;
    if (join_map_impl !== null) {
        await join_map_impl.build();
    }
    let sql_join_type = {'VOID': FakeJoiner, 'JOIN': InnerJoiner, 'INNER JOIN': InnerJoiner, 'LEFT JOIN': LeftJoiner, 'STRICT LEFT JOIN': StrictLeftJoiner}[__RBQLMP__join_operation];
    join_map = new sql_join_type(join_map_impl);
    let warnings = await do_rb_transform(input_iterator, join_map, output_writer);
    return warnings;
}

module.exports.rb_transform = rb_transform;

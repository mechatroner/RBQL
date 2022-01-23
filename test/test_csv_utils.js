const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');
const crypto = require('crypto');
const stream = require('stream');

const csv_utils = require('../rbql-js/csv_utils.js');
const cli_parser = require('../rbql-js/cli_parser.js');
const test_common = require('./test_common.js');

var rbql_csv = null;
var rbql = null;


// TODO add iterator test with random unicode table / separator just like in Python version

// TODO implement random parse header unit tests similar to _do_test_random_headers in the Python test suite

const script_dir = __dirname;

var debug_mode = false;

const line_separators = ['\n', '\r\n', '\r'];

let num_csv_tests_executed = 0;


function rmtree(root_path) {
    if (fs.existsSync(root_path)) {
        fs.readdirSync(root_path).forEach(function(file_name, _index) {
            let child_path = path.join(root_path, file_name);
            if (fs.lstatSync(child_path).isDirectory()) {
                rmtree(child_path);
            } else {
                fs.unlinkSync(child_path);
            }
        });
        fs.rmdirSync(root_path);
    }
};


function random_choice(values) {
    return values[Math.floor(Math.random() * values.length)];
}


function random_int(min_val, max_val) {
    // Return value in [min_val, max_val] inclusive
    min_val = Math.ceil(min_val);
    max_val = Math.floor(max_val + 1);
    return Math.floor(Math.random() * (max_val - min_val)) + min_val;
}


function natural_random(min_val, max_val) {
    if (min_val <= 0 && max_val >= 0 && random_int(0, 2) == 0)
        return 0;
    let k = random_int(0, 8);
    if (k < 2)
        return min_val + k;
    if (k > 6)
        return max_val - 8 + k;
    return random_int(min_val, max_val);
}


function calc_str_md5(str) {
    return crypto.createHash('md5').update(str, 'utf-8').digest('hex');
}


function calc_file_md5(file_path) {
    let data = fs.readFileSync(file_path, 'utf-8');
    return calc_str_md5(data);
}


function die(error_msg) {
    console.error('Error: ' + error_msg);
    process.exit(1);
}


function assert(condition, message = null) {
    if (!condition) {
        if (debug_mode)
            console.trace();
        die(message || "Assertion failed");
    }
}


function replace_all(src, search, replacement) {
    return src.split(search).join(replacement);
}


function compare_splits(src, test_dst, expected_dst, test_warning, expected_warning) {
    if (test_warning != expected_warning || !test_common.assert_arrays_are_equal(test_dst, expected_dst, false, true)) {
        console.error('Error in csv split logic. Source line: ' + src);
        console.error('Test result: ' + test_dst.join(';'));
        console.error('Expected result: ' + expected_dst.join(';'));
        console.error('Expected warning: ' + expected_warning + ', Test warning: ' + test_warning);
        process.exit(1);
    }
}


function make_random_decoded_binary_csv_entry(min_len, max_len, restricted_chars) {
    let strlen = random_int(min_len, max_len);
    let bytes_set = [];
    if (restricted_chars == null)
        restricted_chars = [];
    for (let cc = 0; cc < 256; cc++) {
        let add_to_set = true;
        for (let i = 0; i < restricted_chars.length; i++) {
            if (cc == restricted_chars[i].charCodeAt(0))
                add_to_set = false;
        }
        if (add_to_set)
            bytes_set.push(cc);
    }
    let data_bytes = [];
    for (let i = 0; i < strlen; i++) {
        data_bytes.push(random_choice(bytes_set));
    }
    let data_buf = Buffer.from(data_bytes);
    let result = data_buf.toString('binary');
    let check_buf = Buffer.from(result, 'binary');
    assert(data_buf.equals(check_buf));
    return result;
}


function generate_random_decoded_binary_table(max_num_rows, max_num_cols, restricted_chars) {
    let num_rows = natural_random(1, max_num_rows);
    let num_cols = natural_random(1, max_num_cols);
    let good_keys = ['Hello', 'Avada Kedavra ', '>> ??????', '128', '#3q295 fa#(@*$*)', ' abc defg ', 'NR', 'a1', 'a2'];
    let result = [];
    let good_column = random_int(0, num_cols - 1);
    for (let r = 0; r < num_rows; r++) {
        result.push([]);
        for (let c = 0; c < num_cols; c++) {
            if (c == good_column) {
                result[r].push(random_choice(good_keys));
            } else {
                result[r].push(make_random_decoded_binary_csv_entry(0, 20, restricted_chars));
            }
        }
    }
    return result;
}


function randomly_quote_field(src, delim) {
    if (src.indexOf('"') != -1 || src.indexOf(delim) != -1 || src.indexOf('\n') != -1 || src.indexOf('\r') != -1 || random_int(0, 1) == 1) {
        let spaces_before = delim == ' ' ? '' : ' '.repeat(random_int(0, 2));
        let spaces_after = delim == ' ' ? '' : ' '.repeat(random_int(0, 2));
        let escaped = replace_all(src, '"', '""');
        return `${spaces_before}"${escaped}"${spaces_after}`;
    }
    return src;
}


function randomly_join_quoted(fields, delim) {
    let efields = [];
    for (let field of fields) {
        efields.push(randomly_quote_field(field, delim));
    }
    test_common.assert_arrays_are_equal(csv_utils.unquote_fields(efields), fields);
    return efields.join(delim);
}


function random_whitespace_join(fields) {
    let result = [' '.repeat(random_int(0, 5))];
    for (let f of fields) {
        result.push(f + ' '.repeat(random_int(1, 5)));
    }
    return result.join('');
}


function simple_join(fields, delim) {
    assert(fields.join('').indexOf(delim) == -1, 'unable to use simple policy');
    return fields.join(delim);
}


function random_smart_join(fields, delim, policy) {
    if (policy == 'simple') {
        return simple_join(fields, delim);
    } else if (policy == 'whitespace') {
        assert(delim == ' ');
        return random_whitespace_join(fields);
    } else if (policy == 'quoted' || policy == 'quoted_rfc') {
        assert(delim != '"');
        return randomly_join_quoted(fields, delim);
    } else if (policy == 'monocolumn') {
        assert(fields.length == 1);
        return fields[0];
    } else {
        assert(false, 'unknown policy');
    }
}


function make_random_comment_lines(num_lines, comment_prefix, delim_to_test) {
    let lines = [];
    let str_pool = ['""', '"', delim_to_test, comment_prefix, 'aaa', 'b', '#', ',', '\t', '\\'];
    for (let i = 0; i < num_lines; i++) {
        let num_sampled = natural_random(0, 10);
        let line = [];
        while (line.length < num_sampled) {
            line.push(random_choice(str_pool));
        }
        lines.push(comment_prefix + line.join(''));
    }
    return lines;
}


function random_merge_lines(llines, rlines) {
    let merged = [];
    let l = 0;
    let r = 0;
    while (l + r < llines.length + rlines.length) {
        let lleft = llines.length - l;
        let rleft = rlines.length - r;
        let v = random_int(0, lleft + rleft - 1);
        if (v < lleft) {
            merged.push(llines[l]);
            l += 1;
        } else {
            merged.push(rlines[r]);
            r += 1;
        }
    }
    assert(merged.length == llines.length + rlines.length);
    return merged;
}


function table_to_csv_string_random(table, delim, policy, comment_prefix=null) {
    let lines = [];
    for (let record of table) {
        lines.push(random_smart_join(record, delim, policy));
    }
    if (comment_prefix !== null) {
        let num_comment_lines = random_int(0, table.length * 2);
        let comment_lines = make_random_comment_lines(num_comment_lines, comment_prefix, delim);
        lines = random_merge_lines(lines, comment_lines);
    }
    let line_separator = random_choice(line_separators);
    let result = lines.join(line_separator);
    if (random_int(0, 1) == 1) {
        result += line_separator;
    }
    return result;
}


function PseudoWritable() {
    this.data_chunks = [];
    this.encoding = 'utf-8';

    this.setDefaultEncoding = function(encoding) {
        this.encoding = encoding;
    };

    this.write = function(data) {
        this.data_chunks.push(data);
    };

    this.get_data = function() {
        return Buffer.from(this.data_chunks.join(''), this.encoding);
    };

    this.on = function(msg_type, callback) {
        assert(msg_type === 'error');
    }
}


function string_to_randomly_encoded_stream(src_str) {
    let encoding = random_choice(['utf-8', 'binary']);
    let input_stream = new stream.Readable();
    input_stream.push(Buffer.from(src_str, encoding));
    input_stream.push(null);
    return [input_stream, encoding];
}


async function write_and_parse_back(table, encoding, delim, policy) {
    if (encoding === null)
        encoding = 'utf-8'; // Writing js string in utf-8 then reading back should be a lossless operation? Or not?
    let writer_stream = new PseudoWritable();
    let line_separator = random_choice(line_separators);
    let writer = new rbql_csv.CSVWriter(writer_stream, false, encoding, delim, policy, line_separator);
    writer._write_all(table);
    await writer.finish();
    assert(writer.get_warnings().length === 0);
    let data_buffer = writer_stream.get_data();
    let input_stream = new stream.Readable();
    input_stream.push(data_buffer);
    input_stream.push(null);
    let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, null, encoding, delim, policy);
    let output_table = await record_iterator.get_all_records();
    test_common.assert_arrays_are_equal(table, output_table);
}


function find_in_table(table, token) {
    for (let r = 0; r < table.length; r++)
        for (let c = 0; c < table[r].length; c++)
            if (table[r][c].indexOf(token) != -1)
                return true;
    return false;
}


function test_split() {

    test_common.assert_arrays_are_equal(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', true)[0], [' aaa', ' " aaa, bbb " ', ' ccc ', ' ddd ']);
    test_common.assert_arrays_are_equal(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', false)[0], [' aaa', ' aaa, bbb ', ' ccc ', ' ddd ']);

    var test_cases = [];
    test_cases.push(['hello,world', ['hello','world'], false]);
    test_cases.push(['hello,"world"', ['hello','world'], false]);
    test_cases.push(['"abc"', ['abc'], false]);
    test_cases.push(['abc', ['abc'], false]);
    test_cases.push(['', [''], false]);
    test_cases.push([',', ['',''], false]);
    test_cases.push([',,,', ['','','',''], false]);
    test_cases.push([',"",,,', ['','','','',''], false]);
    test_cases.push(['"","",,,""', ['','','','',''], false]);
    test_cases.push(['"aaa,bbb",', ['aaa,bbb',''], false]);
    test_cases.push(['"aaa,bbb",ccc', ['aaa,bbb','ccc'], false]);
    test_cases.push(['"aaa,bbb","ccc"', ['aaa,bbb','ccc'], false]);
    test_cases.push(['"aaa,bbb","ccc,ddd"', ['aaa,bbb','ccc,ddd'], false]);
    test_cases.push([' "aaa,bbb" ,  "ccc,ddd" ', ['aaa,bbb','ccc,ddd'], false]);
    test_cases.push(['"aaa,bbb",ccc,ddd', ['aaa,bbb','ccc', 'ddd'], false]);
    test_cases.push(['"a"aa" a,bbb",ccc,ddd', ['"a"aa" a', 'bbb"','ccc', 'ddd'], true]);
    test_cases.push(['"aa, bb, cc",ccc",ddd', ['aa, bb, cc','ccc"', 'ddd'], true]);
    test_cases.push(['hello,world,"', ['hello','world', '"'], true]);
    test_cases.push([' aaa, " aaa, bbb " , ccc , ddd ', [' aaa', ' aaa, bbb ', ' ccc ', ' ddd '], false]);
    test_cases.push([' aaa ,bbb ,ccc , ddd ', [' aaa ', 'bbb ', 'ccc ', ' ddd '], false]);

    for (let i = 0; i < test_cases.length; i++) {
        let [src, expected_dst, expected_warning] = test_cases[i];
        let split_result = csv_utils.split_quoted_str(src, ',');
        let test_dst = split_result[0];
        let test_warning = split_result[1];

        let split_result_preserved = csv_utils.split_quoted_str(src, ',', true);
        assert(test_warning === split_result_preserved[1], 'warnings do not match');
        assert(split_result_preserved[0].join(',') === src, 'preserved restore do not match');
        if (!expected_warning) {
            test_common.assert_arrays_are_equal(test_dst, csv_utils.unquote_fields(split_result_preserved[0]));
        }
        if (!expected_warning) {
            compare_splits(src, test_dst, expected_dst, test_warning, expected_warning);
        }
    }
}

function test_split_whitespaces() {
    var test_cases = [];

    test_cases.push(['hello world', ['hello','world'], false]);
    test_cases.push(['hello   world', ['hello','world'], false]);
    test_cases.push(['   hello   world   ', ['hello','world'], false]);
    test_cases.push(['     ', [], false]);
    test_cases.push(['', [], false]);
    test_cases.push(['   a   b  c d ', ['a', 'b', 'c', 'd'], false]);

    test_cases.push(['hello world', ['hello','world'], true]);
    test_cases.push(['hello   world', ['hello  ','world'], true]);
    test_cases.push(['   hello   world   ', ['   hello  ','world   '], true]);
    test_cases.push(['     ', [], true]);
    test_cases.push(['', [], true]);
    test_cases.push(['   a   b  c d ', ['   a  ', 'b ', 'c', 'd '], true]);

    for (let i = 0; i < test_cases.length; i++) {
        let [src, expected_dst, preserve_whitespaces] = test_cases[i];
        let test_dst = csv_utils.split_whitespace_separated_str(src, preserve_whitespaces);
        test_common.assert_arrays_are_equal(expected_dst, test_dst);
    }
}


function test_unquote() {
    var test_cases = [];
    test_cases.push(['  "hello, ""world"" aa""  " ', 'hello, "world" aa"  ']);
    for (let i = 0; i < test_cases.length; i++) {
        let unquoted = csv_utils.unquote_field(test_cases[i][0]);
        let expected = test_cases[i][1];
        assert(expected == unquoted);
    }

}


async function test_whitespace_separated_parsing() {
    let data_lines = [];
    data_lines.push('hello world');
    data_lines.push('   hello   world  ');
    data_lines.push('hello   world  ');
    data_lines.push('  hello   ');
    data_lines.push('  hello   world');
    let expected_table = [['hello', 'world'], ['hello', 'world'], ['hello', 'world'], ['hello'], ['hello', 'world']];
    let csv_data = data_lines.join('\n');
    let input_stream = new stream.Readable();
    input_stream.push(csv_data);
    input_stream.push(null);
    let delim = ' ';
    let policy = 'whitespace';
    let encoding = 'utf-8';
    let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, null, encoding, delim, policy);
    let output_table = await record_iterator.get_all_records();
    test_common.assert_arrays_are_equal(expected_table, output_table);
    await write_and_parse_back(expected_table, encoding, delim, policy);
}


function randomly_replace_columns_dictionary_style(query) {
    let adjusted_query = query;
    for (let prefix of ['a', 'b']) {
        let rgx = new RegExp(`(?:^|[^_a-zA-Z0-9])${prefix}\\.([_a-zA-Z][_a-zA-Z0-9]*)`, 'g');
        let matches = rbql.get_all_matches(rgx, query);
        for (let match of matches) {
            if (random_int(0, 1))
                continue;
            let column_name = match[1];
            let quote_style = ['"', "'"][random_int(0, 1)];
            adjusted_query = replace_all(adjusted_query, `${prefix}.${column_name}`, `${prefix}[${quote_style}${column_name}${quote_style}]`);
        }
    }
    return adjusted_query;
}


async function process_test_case(tmp_tests_dir, test_case) {
    let test_name = test_case['test_name'];
    let query = test_case['query_js'];
    if (!query)
        return;
    num_csv_tests_executed++;

    let input_table_path = test_case['input_table_path'];
    let local_debug_mode = test_common.get_default(test_case, 'debug_mode', false);
    let bulk_read = test_common.get_default(test_case, 'bulk_read', false);
    let randomly_replace_var_names = test_common.get_default(test_case, 'randomly_replace_var_names', true)
    query = query.replace('###UT_TESTS_DIR###', script_dir);
    if (randomly_replace_var_names)
        query = randomly_replace_columns_dictionary_style(query);

    let expected_output_table_path = test_common.get_default(test_case, 'expected_output_table_path', null);
    let expected_error = test_common.get_default(test_case, 'expected_error', null);
    let expected_error_exact = test_common.get_default(test_case, 'expected_error_exact', false);
    let expected_warnings = test_common.get_default(test_case, 'expected_warnings', []).sort();
    let delim = test_case['csv_separator'];
    let policy = test_case['csv_policy'];
    let encoding = test_case['csv_encoding'];
    let comment_prefix = test_common.get_default(test_case, 'comment_prefix', null);
    let with_headers = test_common.get_default(test_case, 'with_headers', false);
    let output_format = test_common.get_default(test_case, 'output_format', 'input');
    let [output_delim, output_policy] = output_format == 'input' ? [delim, policy] : rbql_csv.interpret_named_csv_format(output_format);
    let actual_output_table_path = null;
    let expected_md5 = null;
    if (expected_output_table_path !== null) {
        let output_file_name = path.basename(expected_output_table_path);
        expected_output_table_path = path.join(script_dir, expected_output_table_path);
        actual_output_table_path = path.join(tmp_tests_dir, output_file_name);
        expected_md5 = calc_file_md5(expected_output_table_path);
    } else {
        actual_output_table_path = path.join(tmp_tests_dir, 'expected_empty_file');
    }

    bulk_read = bulk_read || random_choice([true, false]);
    let options = {'bulk_read': bulk_read};

    let warnings = [];
    try {
        await rbql_csv.query_csv(query, input_table_path, delim, policy, actual_output_table_path, output_delim, output_policy, encoding, warnings, with_headers, comment_prefix, '', options);
    } catch (e) {
        if (local_debug_mode)
            throw(e);
        if(!expected_error) {
            throw(e);
        }
        if (expected_error_exact) {
            test_common.assert_equal(expected_error, e.message);
        } else {
            assert(e.message.indexOf(expected_error) != -1, `Expected error is not substring of actual. Expected error: ${expected_error}, Actual error: ${e.message}`);
        }
        return;
    }
    warnings = test_common.normalize_warnings(warnings).sort();
    test_common.assert_arrays_are_equal(expected_warnings, warnings);
    let actual_md5 = calc_file_md5(actual_output_table_path);
    assert(expected_md5 == actual_md5, `md5 mismatch in test "${test_name}". Expected table: ${expected_output_table_path}, Actual table: ${actual_output_table_path}`);
}


async function test_json_scenarios() {
    let tests_file_path = 'csv_unit_tests.json';
    let tests = JSON.parse(fs.readFileSync(tests_file_path, 'utf-8'));
    let filtered_tests = tests.filter(t => test_common.get_default(t, 'skip_others', false));
    if (filtered_tests.length) {
        console.log('Using filtered tests');
        tests = filtered_tests;
    }
    let tmp_tests_dir = 'rbql_csv_unit_tests_dir_js_' + String(Math.random()).replace('.', '_');
    tmp_tests_dir = path.join(os.tmpdir(), tmp_tests_dir);
    fs.mkdirSync(tmp_tests_dir);
    for (let test_case of tests) {
        let flaky_repeat_count = test_common.get_default(test_case, 'flaky_repeat_count', 1);
        for (let i = 0; i < flaky_repeat_count; i++) {
            await process_test_case(tmp_tests_dir, test_case);
        }
    }
    console.log(`Number of json csv tests executed: ${num_csv_tests_executed}`)
    rmtree(tmp_tests_dir);
}


function test_random_funcs() {
    while (1) {
        if (random_int(0, 1) == 0 && random_int(0, 1) == 1)
            break;
    }
    while (1) {
        if (random_choice([1, 2, 3]) == 1 && random_choice([1, 2, 3]) == 2 && random_choice([1, 2, 3]) == 3)
            break;
    }
}


function normalize_newlines_in_fields(table) {
    for (let r = 0; r < table.length; r++) {
        for (let c = 0; c < table[r].length; c++) {
            table[r][c] = replace_all(table[r][c], '\r\n', '\n');
            table[r][c] = replace_all(table[r][c], '\r', '\n');
        }
    }
}


async function do_test_record_iterator(table, delim, policy, comment_prefix=null) {
    let csv_data = table_to_csv_string_random(table, delim, policy, comment_prefix);
    if (policy == 'quoted_rfc')
        normalize_newlines_in_fields(table);
    let [stream, encoding] = string_to_randomly_encoded_stream(csv_data);
    let record_iterator = new rbql_csv.CSVRecordIterator(stream, null, encoding, delim, policy, false, comment_prefix);
    let parsed_table = await record_iterator.get_all_records();
    test_common.assert_arrays_are_equal(table, parsed_table);
    await write_and_parse_back(table, encoding, delim, policy);
}


async function test_record_iterator() {
    for (let itest = 0; itest < 100; itest++) {
        let table = generate_random_decoded_binary_table(10, 10, ['\r', '\n']);
        let delims = ['\t', ',', ';', '|'];
        let delim = random_choice(delims);
        let table_has_delim = find_in_table(table, delim);
        let policy = table_has_delim ? 'quoted' : random_choice(['quoted', 'simple']);
        await do_test_record_iterator(table, delim, policy);
    }
}


async function test_record_iterator_bulk_mode() {
    let csv_path = path.join(script_dir, 'csv_files', 'movies.tsv');
    let record_iterator = new rbql_csv.CSVRecordIterator(null, csv_path, 'utf-8', '\t', 'simple');
    let parsed_table = await record_iterator.get_all_records();
    let data = fs.readFileSync(csv_path, 'utf-8')
    let lines = data.split('\n');
    let table = [];
    for (let line of lines) {
        if (line.length)
            table.push(line.split('\t'));
    }
    test_common.assert_equal(table.length, 4464);
    test_common.assert_arrays_are_equal(table, parsed_table);
}


async function test_iterator_rfc() {
    for (let itest = 0; itest < 100; itest++) {
        let table = generate_random_decoded_binary_table(10, 10, null);
        let delims = ['\t', ',', ';', '|'];
        let delim = random_choice(delims);
        let policy = 'quoted_rfc';
        await do_test_record_iterator(table, delim, policy);
    }
}


function table_has_records_with_comment_prefix(table, comment_prefix) {
    for (let r of table) {
        if (r[0].startsWith(comment_prefix))
            return true;
    }
    return false;
}


async function test_iterator_rfc_comments() {
    for (let itest = 0; itest < 200; itest++) {
        let table = generate_random_decoded_binary_table(10, 10, null);
        let comment_prefix = random_choice(['#', '>>']);
        if (table_has_records_with_comment_prefix(table, comment_prefix))
            continue;
        let delims = ['\t', ',', ';', '|'];
        let delim = random_choice(delims);
        let policy = 'quoted_rfc';
        await do_test_record_iterator(table, delim, policy, comment_prefix);
    }
}


async function test_multicharacter_separator_parsing() {
    let data_lines = [];
    data_lines.push('aaa:=)bbb:=)ccc');
    data_lines.push('aaa :=) bbb :=)ccc ');
    let expected_table = [['aaa', 'bbb', 'ccc'], ['aaa ', ' bbb ', 'ccc ']];
    let csv_data = data_lines.join('\n');
    let input_stream = new stream.Readable();
    input_stream.push(csv_data);
    input_stream.push(null);
    let delim = ':=)';
    let policy = 'simple';
    let encoding = 'utf-8';
    let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, null, encoding, delim, policy);
    let parsed_table = await record_iterator.get_all_records();
    test_common.assert_arrays_are_equal(expected_table, parsed_table);
    await write_and_parse_back(expected_table, encoding, delim, policy);
}


async function test_monocolumn_separated_parsing() {
    for (let itest = 0; itest < 30; itest++) {
        let table = [];
        let num_rows = random_int(1, 30);
        for (let r = 0; r < num_rows; r++) {
            let min_len = r + 1 < num_rows ? 0 : 1;
            table.push([make_random_decoded_binary_csv_entry(min_len, 20, ['\r', '\n'])]);
        }
        let delim = null;
        let policy = 'monocolumn';
        let encoding = 'binary';
        let csv_data = table_to_csv_string_random(table, delim, policy);
        let input_stream = new stream.Readable();
        input_stream.push(Buffer.from(csv_data, encoding));
        input_stream.push(null);
        let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, null, encoding, delim, policy);
        let parsed_table = await record_iterator.get_all_records();
        test_common.assert_arrays_are_equal(table, parsed_table);
        await write_and_parse_back(table, encoding, delim, policy);
        test_common.assert_arrays_are_equal(table, parsed_table);
    }
}


function test_record_queue() {
    let record_queue = new rbql_csv.RecordQueue();
    record_queue.enqueue(10);
    record_queue.enqueue(20);
    test_common.assert_equal(10, record_queue.dequeue());
    test_common.assert_equal(20, record_queue.dequeue());
    test_common.assert_equal(null, record_queue.dequeue());
    test_common.assert_equal(null, record_queue.dequeue());
    record_queue.enqueue(10);
    test_common.assert_equal(10, record_queue.dequeue());
    record_queue.enqueue(20);
    test_common.assert_equal(20, record_queue.dequeue());
    record_queue.enqueue(5);
    test_common.assert_equal(5, record_queue.dequeue());
    test_common.assert_equal(null, record_queue.dequeue());
    test_common.assert_equal(null, record_queue.dequeue());
    test_common.assert_equal(null, record_queue.dequeue());
}


function vinf(do_init, zb_index) {
    return {initialize: do_init, index: zb_index};
}


function test_dictionary_variables_parsing() {
    let query = 'select a["foo bar"], a["foo"], max(a["foo"], a["lambda-beta{\'gamma\'}"]), a1, a2, a.epsilon';
    let header_columns_names = ['foo', 'foo bar', 'max', "lambda-beta{'gamma'}", "lambda-beta{'gamma2'}", "eps\\ilon", "omega", "1", "2", "....", "["];
    let expected_variables_map = {'a["foo"]': vinf(true, 0), 'a["foo bar"]': vinf(true, 1), 'a["max"]': vinf(true, 2), "a[\"lambda-beta{'gamma'}\"]": vinf(true, 3), 'a["eps\\\\ilon"]': vinf(true, 5), 'a["1"]': vinf(true, 7), 'a["2"]': vinf(true, 8), 'a["["]': vinf(true, 10), "a['foo']": vinf(false, 0), "a['foo bar']": vinf(false, 1), "a['max']": vinf(false, 2), "a['lambda-beta{\\'gamma\\'}']": vinf(false, 3), "a['eps\\\\ilon']": vinf(false, 5), "a['1']": vinf(false, 7), "a['2']": vinf(false, 8), "a['[']": vinf(false, 10), "a[`foo`]": vinf(false, 0), "a[`foo bar`]": vinf(false, 1), "a[`max`]": vinf(false, 2), "a[`lambda-beta{'gamma'}`]": vinf(false, 3), "a[`eps\\\\ilon`]": vinf(false, 5), "a[`1`]": vinf(false, 7), "a[`2`]": vinf(false, 8), "a[`[`]": vinf(false, 10)};
    let actual_variables_map = {};
    rbql.parse_dictionary_variables(query, 'a', header_columns_names, actual_variables_map);
    test_common.assert_objects_are_equal(expected_variables_map, actual_variables_map);
}


function test_attribute_variables_parsing() {
    let query = 'select a["foo bar"], a1, a2, a.epsilon, a._name + a.Surname, a["income"]';
    let header_columns_names = ['epsilon', 'foo bar', '_name', "Surname", "income", "...", "2", "200"];
    let expected_variables_map = {'a.epsilon': vinf(true, 0), 'a._name': vinf(true, 2), "a.Surname": vinf(true, 3)};
    let actual_variables_map = {};
    rbql.parse_attribute_variables(query, 'a', header_columns_names, 'CSV header line', actual_variables_map);
    test_common.assert_objects_are_equal(expected_variables_map, actual_variables_map);
}


async function test_everything() {
    test_record_queue();
    test_random_funcs();
    test_unquote();
    test_split();
    test_split_whitespaces();
    test_dictionary_variables_parsing();
    test_attribute_variables_parsing();
    await test_whitespace_separated_parsing();
    await test_record_iterator();
    await test_record_iterator_bulk_mode();
    await test_monocolumn_separated_parsing();
    await test_multicharacter_separator_parsing();
    await test_iterator_rfc();
    await test_iterator_rfc_comments();
    await test_json_scenarios();
}


function process_random_test_line(line) {
    var records = line.split('\t');
    assert(records.length == 3);
    var escaped_entry = records[0];
    var expected_warning = parseInt(records[1]);
    assert(expected_warning == 0 || expected_warning == 1);
    expected_warning = Boolean(expected_warning);
    var expected_dst = records[2].split(';');
    var split_result = csv_utils.split_quoted_str(escaped_entry, ',');
    var test_dst = split_result[0];
    var test_warning = split_result[1];

    var split_result_preserved = csv_utils.split_quoted_str(escaped_entry, ',', true);
    assert(test_warning === split_result_preserved[1]);
    assert(split_result_preserved[0].join(',') === escaped_entry);
    if (!expected_warning) {
        test_common.assert_arrays_are_equal(csv_utils.unquote_fields(split_result_preserved[0]), test_dst);
    }
    if (!expected_warning) {
        compare_splits(escaped_entry, test_dst, expected_dst, test_warning, expected_warning);
    }
}


function run_random_csv_mode(random_csv_table_path) {
    let lineReader = readline.createInterface({ input: fs.createReadStream(random_csv_table_path, {encoding: 'binary'}) });
    lineReader.on('line', process_random_test_line);
    lineReader.on('close', function () {
        console.log('Finished split unit test');
    });
}


function main() {
    console.log('Starting JS unit tests');

    var scheme = {
        '--run-random-csv-mode': {'help': 'run in random csv mode'},
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    if (args.hasOwnProperty('run-random-csv-mode')) {
        let random_table_path = args['run-random-csv-mode'];
        run_random_csv_mode(random_table_path);
        return;
    }

    debug_mode = args['dbg'];

    rbql_csv = require('../rbql-js/rbql_csv.js');
    rbql = require('../rbql-js/rbql.js');
    test_everything().then(v => { console.log('Finished JS unit tests'); }).catch(error_info => { console.log('JS tests failed:' + JSON.stringify(error_info)); console.log(error_info.stack); });
}


main();

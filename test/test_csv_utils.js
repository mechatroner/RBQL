const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');
const crypto = require('crypto');
const stream = require('stream');

const csv_utils = require('../rbql-js/csv_utils.js');
const cli_parser = require('../rbql-js/cli_parser.js');
const build_engine = require('../rbql-js/build_engine.js');
const test_common = require('./test_common.js');

var rbql_csv = null;


// FIXME add record iterator tests, see python version

const script_dir = __dirname;

var debug_mode = false;

const line_separators = ['\n', '\r\n', '\r'];


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
    min_val = Math.ceil(min_val);
    max_val = Math.floor(max_val + 1);
    return Math.floor(Math.random() * (max_val - min_val)) + min_val;
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


function compare_splits(src, test_dst, canonic_dst, test_warning, canonic_warning) {
    if (test_warning != canonic_warning || !test_common.arrays_are_equal(test_dst, canonic_dst)) {
        console.error('Error in csv split logic. Source line: ' + src);
        console.error('Test result: ' + test_dst.join(';'));
        console.error('Canonic result: ' + canonic_dst.join(';'));
        console.error('Canonic warning: ' + canonic_warning + ', Test warning: ' + test_warning);
        process.exit(1);
    }
}


function make_random_decoded_binary_csv_entry(min_len, max_len, restricted_chars) {
    let strlen = random_int(min_len, max_len);
    let bytes_set = [];
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


function randomly_quote_field(src, delim) {
    if (src.indexOf('"') != -1 || src.indexOf(delim) != -1 || random_int(0, 1) == 1) {
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
        efields.push(randomly_quote_field(field));
    }
    assert(test_common.arrays_are_equal(csv_utils.unquote_fields(efields), fields));
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
    return fields.join(this.delim);
}


function random_smart_join(fields, delim, policy) {
    if (policy == 'simple') {
        return simple_join(fields, delim);
    } else if (policy == 'whitespace') {
        assert(delim == ' ');
        return random_whitespace_join(fields);
    } else if (policy == 'quoted') {
        assert(delim != '"');
        return randomly_join_quoted(fields, delim);
    } else if (policy == 'monocolumn') {
        assert(fields.length == 1);
        return fields[0];
    } else {
        assert(false, 'unknown policy');
    }
}


function table_to_csv_string_random(table, delim, policy) {
    let line_separator = random_choice(line_separators);
    let result = [];
    for (let record of table) {
        result.push(random_smart_join(record, delim, policy));
    }
    result = result.join(line_separator);
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
}


function string_to_randomly_encoded_stream(src_str) {
    let encoding = random_choice(['utf-8', 'binary']);
    let input_stream = new stream.Readable();
    input_stream.setEncoding(encoding); // For older node versions we have to call setEncoding() before pushing anything into the stream. E.g. in node version 8 this is broken but fixed in node version 12
    input_stream.push(Buffer.from(src_str, encoding));
    input_stream.push(null);
    return [input_stream, encoding];
}


function write_and_parse_back(table, encoding, delim, policy) {
    if (encoding === null)
        encoding = 'utf-8'; // Writing js string in utf-8 then reading back should be a lossless operation? Or not?
    let writer_stream = new PseudoWritable();
    let line_separator = random_choice(line_separators);
    let writer = new rbql_csv.CSVWriter(writer_stream, true, encoding, delim, policy, line_separator);
    writer._write_all(table);
    assert(writer.get_warnings().length === 0);
    let data_buffer = writer_stream.get_data();
    let input_stream = new stream.Readable();
    input_stream.setEncoding(encoding); // For older node versions we have to call setEncoding() before pushing anything into the stream. E.g. in node version 8 this is broken but fixed in node version 12
    input_stream.push(data_buffer);
    input_stream.push(null);
    let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, encoding, delim, policy);
    record_iterator._get_all_records(function(output_table) {
        assert(test_common.tables_are_equal(table, output_table), 'Expected and output tables mismatch');
    });
}


function test_split() {

    assert(test_common.arrays_are_equal(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', true)[0], [' aaa', ' " aaa, bbb " ', ' ccc ', ' ddd ']));
    assert(test_common.arrays_are_equal(csv_utils.split_quoted_str(' aaa, " aaa, bbb " , ccc , ddd ', ',', false)[0], [' aaa', ' aaa, bbb ', ' ccc ', ' ddd ']));

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
        let [src, canonic_dst, canonic_warning] = test_cases[i];
        let split_result = csv_utils.split_quoted_str(src, ',');
        let test_dst = split_result[0];
        let test_warning = split_result[1];

        let split_result_preserved = csv_utils.split_quoted_str(src, ',', true);
        assert(test_warning === split_result_preserved[1], 'warnings do not match');
        assert(split_result_preserved[0].join(',') === src, 'preserved restore do not match');
        if (!canonic_warning) {
            assert(test_common.arrays_are_equal(test_dst, csv_utils.unquote_fields(split_result_preserved[0])), 'unquoted do not match');
        }
        if (!canonic_warning) {
            compare_splits(src, test_dst, canonic_dst, test_warning, canonic_warning);
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
        let [src, canonic_dst, preserve_whitespaces] = test_cases[i];
        let test_dst = csv_utils.split_whitespace_separated_str(src, preserve_whitespaces);
        assert(test_common.arrays_are_equal(canonic_dst, test_dst, 'whitespace split failure'));
    }
}


function test_unquote() {
    var test_cases = [];
    test_cases.push(['  "hello, ""world"" aa""  " ', 'hello, "world" aa"  ']);
    for (let i = 0; i < test_cases.length; i++) {
        let unquoted = csv_utils.unquote_field(test_cases[i][0]);
        let canonic = test_cases[i][1];
        assert(canonic == unquoted);
    }

}


// TODO Add tests: 2. random parsing 3. utf decoding errors 4. bom


function test_whitespace_separated_parsing() {
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
    let encoding = null;
    let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, encoding, delim, policy);
    record_iterator._get_all_records(function(output_table) {
        assert(test_common.tables_are_equal(expected_table, output_table), 'Expected and output tables mismatch');
        write_and_parse_back(expected_table, encoding, delim, policy);
    });
}


function test_split_lines_custom() {
    let test_cases = [];
    test_cases.push(['', []]);
    test_cases.push(['hello', ['hello']]);
    test_cases.push(['hello\nworld', ['hello', 'world']]);
    test_cases.push(['hello\rworld\n', ['hello', 'world']]);
    test_cases.push(['hello\r\nworld\rhello world\nhello\n', ['hello', 'world', 'hello world', 'hello']]);
    for (let tc of test_cases) {
        let [src, expected_res] = tc;
        let [stream, encoding] = string_to_randomly_encoded_stream(src);
        let line_iterator = new rbql_csv.CSVRecordIterator(stream, encoding, null, null);
        line_iterator._get_all_lines(function(test_res) {
            assert(test_common.arrays_are_equal(expected_res, test_res));
        });
    }
}


function process_test_case(tmp_tests_dir, tests, test_id) {
    if (test_id >= tests.length) {
        rmtree(tmp_tests_dir);
        console.log('Finished JS unit tests');
        return;
    }
    let test_case = tests[test_id];
    let test_name = test_case['test_name'];

    let query = test_case['query_js'];
    if (!query)  {
        process_test_case(tmp_tests_dir, tests, test_id + 1);
        return;
    }
    console.log('Running rbql test: ' + test_name);
    query = query.replace('###UT_TESTS_DIR###', script_dir);

    let input_table_path = test_case['input_table_path'];
    let expected_output_table_path = test_common.get_default(test_case, 'expected_output_table_path', null);
    let expected_error = test_common.get_default(test_case, 'expected_error', null);
    let expected_warnings = test_common.get_default(test_case, 'expected_warnings', []).sort();
    let delim = test_case['csv_separator'];
    let policy = test_case['csv_policy'];
    let encoding = test_case['csv_encoding'];
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

    let error_handler = function(error_type, error_msg) {
        assert(expected_error);
        assert(error_msg.indexOf(expected_error) != -1);
        process_test_case(tmp_tests_dir, tests, test_id + 1);
    };
    let success_handler = function(warnings) {
        assert(expected_error === null);
        warnings = test_common.normalize_warnings(warnings).sort();
        assert(test_common.arrays_are_equal(expected_warnings, warnings));
        let actual_md5 = calc_file_md5(actual_output_table_path);
        assert(expected_md5 == actual_md5, `md5 mismatch. Expected table: ${expected_output_table_path}, Actual table: ${actual_output_table_path}`);
        process_test_case(tmp_tests_dir, tests, test_id + 1);
    };

    rbql_csv.csv_run(query, input_table_path, delim, policy, actual_output_table_path, output_delim, output_policy, encoding, success_handler, error_handler, '');
}


function test_json_scenarios() {
    let tests_file_path = 'csv_unit_tests.json';
    let tests = JSON.parse(fs.readFileSync(tests_file_path, 'utf-8'));
    let tmp_tests_dir = 'rbql_csv_unit_tests_dir_js_' + String(Math.random()).replace('.', '_');
    tmp_tests_dir = path.join(os.tmpdir(), tmp_tests_dir);
    fs.mkdirSync(tmp_tests_dir);
    process_test_case(tmp_tests_dir, tests, 0);
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


function test_monocolumn_separated_parsing() {
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
        input_stream.setEncoding('binary'); // For older node versions we have to call setEncoding() before pushing anything into the stream. E.g. in node version 8 this is broken but fixed in node version 12
        input_stream.push(Buffer.from(csv_data, encoding));
        input_stream.push(null);
        let record_iterator = new rbql_csv.CSVRecordIterator(input_stream, encoding, delim, policy);
        record_iterator._get_all_records(function(parsed_table) {
            assert(test_common.tables_are_equal(table, parsed_table), 'Expected and output tables mismatch');
            write_and_parse_back(table, encoding, delim, policy);
            assert(test_common.tables_are_equal(table, parsed_table), 'Expected and output tables mismatch');
        });
    }
}


function test_all() {
    test_random_funcs();
    test_unquote();
    test_split();
    test_split_whitespaces();
    test_whitespace_separated_parsing();
    test_split_lines_custom();
    test_json_scenarios();
    test_monocolumn_separated_parsing();
}


function process_random_test_line(line) {
    var records = line.split('\t');
    assert(records.length == 3);
    var escaped_entry = records[0];
    var canonic_warning = parseInt(records[1]);
    assert(canonic_warning == 0 || canonic_warning == 1);
    canonic_warning = Boolean(canonic_warning);
    var canonic_dst = records[2].split(';');
    var split_result = csv_utils.split_quoted_str(escaped_entry, ',');
    var test_dst = split_result[0];
    var test_warning = split_result[1];

    var split_result_preserved = csv_utils.split_quoted_str(escaped_entry, ',', true);
    assert(test_warning === split_result_preserved[1]);
    assert(split_result_preserved[0].join(',') === escaped_entry);
    if (!canonic_warning) {
        assert(test_common.arrays_are_equal(csv_utils.unquote_fields(split_result_preserved[0]), test_dst));
    }
    if (!canonic_warning) {
        compare_splits(escaped_entry, test_dst, canonic_dst, test_warning, canonic_warning);
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
        '--auto-rebuild-engine': {'boolean': true, 'help': 'Auto rebuild engine'},
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    if (args['auto-rebuild-engine']) {
        build_engine.build_engine();
    }

    if (args.hasOwnProperty('run-random-csv-mode')) {
        let random_table_path = args['run-random-csv-mode'];
        run_random_csv_mode(random_table_path);
        return;
    }

    debug_mode = args['dbg'];

    let engine_text_current = build_engine.read_engine_text();
    let engine_text_expected = build_engine.build_engine_text();
    if (engine_text_current != engine_text_expected) {
        die("rbql.js must be rebuild from template.js and builder.js");
    }

    rbql_csv = require('../rbql-js/rbql_csv.js');
    rbql_csv.debug_mode = debug_mode;

    test_all();
}


main();

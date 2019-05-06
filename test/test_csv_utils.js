const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');

var rbql = null;
var rbq_csv = null;
const csv_utils = require('../rbql-js/csv_utils.js');
const cli_parser = require('../rbql-js/cli_parser.js');
const build_engine = require('../rbql-js/build_engine.js');


// FIXME add record iterator tests, see python version


function arrays_are_equal(a, b) {
    if (a.length != b.length)
        return false;
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i])
            return false;
    }
    return true;
}


function assert(condition, message = null) {
    if (!condition) {
        throw message || "Assertion failed";
    }
}


function compare_splits(src, test_dst, canonic_dst, test_warning, canonic_warning) {
    if (test_warning != canonic_warning || !arrays_are_equal(test_dst, canonic_dst)) {
        console.error('Error in csv split logic. Source line: ' + src);
        console.error('Test result: ' + test_dst.join(';'));
        console.error('Canonic result: ' + canonic_dst.join(';'));
        console.error('Canonic warning: ' + canonic_warning + ', Test warning: ' + test_warning);
        process.exit(1);
    }
}


//function process_random_test_line(line) {
//    // FIXME fix random test
//    var records = line.split('\t');
//    assert(records.length == 3);
//    var escaped_entry = records[0];
//    var canonic_warning = parseInt(records[1]);
//    assert(canonic_warning == 0 || canonic_warning == 1);
//    canonic_warning = Boolean(canonic_warning);
//    var canonic_dst = records[2].split(';');
//    var split_result = csv_utils.split_quoted_str(escaped_entry, ',');
//    var test_dst = split_result[0];
//    var test_warning = split_result[1];
//
//    var split_result_preserved = csv_utils.split_quoted_str(escaped_entry, ',', true);
//    assert(test_warning === split_result_preserved[1]);
//    assert(split_result_preserved[0].join(',') === escaped_entry);
//    if (!canonic_warning) {
//        assert(arrays_are_equal(csv_utils.unquote_fields(split_result_preserved[0]), test_dst));
//    }
//    if (!canonic_warning) {
//        compare_splits(escaped_entry, test_dst, canonic_dst, test_warning, canonic_warning);
//    }
//}
//



function test_split() {
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

    for (let i = 0; i < test_cases.length; i++) {
        let [src, canonic_dst, canonic_warning] = test_cases[i];
        let split_result = csv_utils.split_quoted_str(src, ',');
        let test_dst = split_result[0];
        let test_warning = split_result[1];

        let split_result_preserved = csv_utils.split_quoted_str(src, ',', true);
        assert(test_warning === split_result_preserved[1], 'warnings do not match');
        assert(split_result_preserved[0].join(',') === src, 'preserved restore do not match');
        if (!canonic_warning) {
            assert(arrays_are_equal(test_dst, csv_utils.unquote_fields(split_result_preserved[0])), 'unquoted do not match');
        }
        if (!canonic_warning) {
            compare_splits(src, test_dst, canonic_dst, test_warning, canonic_warning);
        }
    }
}

function test_split_whitespaces() {
    var test_cases = [];

    test_cases.push(['hello world', ['hello','world'], false])
    test_cases.push(['hello   world', ['hello','world'], false])
    test_cases.push(['   hello   world   ', ['hello','world'], false])
    test_cases.push(['     ', [], false])
    test_cases.push(['', [], false])
    test_cases.push(['   a   b  c d ', ['a', 'b', 'c', 'd'], false])

    test_cases.push(['hello world', ['hello ','world'], true])
    test_cases.push(['hello   world', ['hello   ','world'], true])
    test_cases.push(['   hello   world   ', ['   hello   ','world   '], true])
    test_cases.push(['     ', [], true])
    test_cases.push(['', [], true])
    test_cases.push(['   a   b  c d ', ['   a   ', 'b  ', 'c ', 'd '], true])

    for (let i = 0; i < test_cases.length; i++) {
        let [src, canonic_dst, preserve_whitespaces] = test_cases[i];
        let test_dst = csv_utils.split_whitespace_separated_str(src, preserve_whitespaces)
        assert(arrays_are_equal(canonic_dst, test_dst, 'whitespace split failure'));
    }
}


function test_unquote() {
    var test_cases = [];
    test_cases.push(['  "hello, ""world"" aa""  " ', 'hello, "world" aa"  '])
    for (let i = 0; i < test_cases.length; i++) {
        let unquoted = csv_utils.unquote_field(test_cases[i][0]);
        let canonic = test_cases[i][1];
        assert(canonic == unquoted);
    }

}


//function process_test_case(tests, test_id) {
//    if (test_id >= tests.length)
//        return;
//    let test_case = tests[test_id];
//    let test_name = test_case['test_name'];
//    console.log('running rbql test: ' + test_name);
//    let query = test_case['query_js'];
//    let input_table = test_case['input_table'];
//    let join_table = get_default(test_case, 'join_table', null);
//    let user_init_code = get_default(test_case, 'js_init_code', '');
//    let expected_output_table = get_default(test_case, 'expected_output_table', null);
//    let expected_error = get_default(test_case, 'expected_error', null);
//    let expected_warnings = get_default(test_case, 'expected_warnings', []);
//    let input_iterator = new rbql.TableIterator(input_table);
//    let output_writer = new rbql.TableWriter();
//    let join_tables_registry = join_table === null ? null : new rbql.SingleTableRegistry(join_table);
//    let error_handler = function(error_type, error_msg) {
//        assert(expected_error);
//        assert(error_msg.indexOf(expected_error) != -1);
//        process_test_case(tests, test_id + 1);
//    }
//    let success_handler = function(warnings) {
//        assert(expected_error === null);
//        warnings = normalize_warnings(warnings).sort();
//        assert(arrays_are_equal(expected_warnings, warnings));
//        let output_table = output_writer.table;
//        round_floats(output_table);
//        assert(tables_are_equal(expected_output_table, output_table), 'Expected and output tables mismatch');
//        process_test_case(tests, test_id + 1);
//    }
//    rbql.generic_run(query, input_iterator, output_writer, success_handler, error_handler, join_tables_registry, user_init_code, debug_mode);
//}


function process_test_case(tmp_tests_dir, tests, test_id) {
    if (test_id >= tests.length) {
        // FIXME remove tmp_tests_dir
        return;
    }
    let test_case = tests[test_id];
    let test_name = test_case['test_name'];
    console.log('Running rbql test: ' + test_name);
    let query = test_case['query_js'];
    let input_table_path = test_case['input_table_path'];
    let expected_output_table_path = get_default(test_case, 'expected_output_table_path', null);
    let expected_error = get_default(test_case, 'expected_error', null);
    let expected_warnings = get_default(test_case, 'expected_warnings', []);
    let delim = test_case['csv_separator'];
    let policy = test_case['csv_policy'];
    let encoding = test_case['csv_encoding'];
    let output_format = get_default(test_case, 'output_format', 'input');
    let [output_delim, output_policy] = output_format == 'input' ? [delim, policy] : csv_utils.interpret_named_csv_format(output_format);

    let input_stream = fs.createReadStream(input_table_path);
    let output_stream = output_path === null ? process.stdout : fs.createWriteStream(output_path);


    let input_iterator = new rbql.TableIterator(input_table);
    let output_writer = new rbql.TableWriter();
    let join_tables_registry = join_table === null ? null : new rbql.SingleTableRegistry(join_table);
    let error_handler = function(error_type, error_msg) {
        assert(expected_error);
        assert(error_msg.indexOf(expected_error) != -1);
        process_test_case(tmp_tests_dir, tests, test_id + 1);
    }
    let success_handler = function(warnings) {
        assert(expected_error === null);
        warnings = normalize_warnings(warnings).sort();
        assert(arrays_are_equal(expected_warnings, warnings));
        let output_table = output_writer.table;
        round_floats(output_table);
        assert(tables_are_equal(expected_output_table, output_table), 'Expected and output tables mismatch');
        process_test_case(tmp_tests_dir, tests, test_id + 1);
    }
    rbql.generic_run(query, input_iterator, output_writer, success_handler, error_handler, join_tables_registry, user_init_code, debug_mode);
}


function test_json_scenarios() {
    let tests_file_path = 'csv_unit_tests.json';
    let tests = JSON.parse(fs.readFileSync(tests_file_path, 'utf-8'));
    let tmp_tests_dir = 'rbql_csv_unit_tests_dir_js_' + String(Math.random()).replace('.', '_');
    let tmp_tests_dir = path.join(os.tmpdir(), tmp_tests_dir);
    fs.mkdirSync(tmp_tests_dir);
    process_test_case(tmp_tests_dir, tests, 0);
}


function test_all() {
    test_unquote();
    test_split();
    test_split_whitespaces();
    test_json_scenarios();
}




function main() {
    console.log('Starting JS unit tests');

    var scheme = {
        '--auto-rebuild-engine': {'boolean': true, 'help': 'Auto rebuild engine'},
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    if (args.hasOwnProperty('auto-rebuild-engine')) {
        build_engine.build_engine();
    }

    debug_mode = args.hasOwnProperty('dbg');

    let engine_text_current = build_engine.read_engine_text();
    let engine_text_expected = build_engine.build_engine_text();
    if (engine_text_current != engine_text_expected) {
        die("rbql.js must be rebuild from template.js and builder.js");
    }

    rbql = require('../rbql-js/rbql.js')
    rbql_csv = require('../rbql-js/rbql_csv.js')

    test_all();


    console.log('Finished JS unit tests');
}


main();

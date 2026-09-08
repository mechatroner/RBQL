const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');
const crypto = require('crypto');
const stream = require('stream');

const cli_parser = require('../rbql-js/cli_parser.js');
const test_common = require('./test_common.js');

var rbql_json = null;
var rbql = null;

const script_dir = __dirname;

var debug_mode = false;
let num_json_tests_executed = 0;


async function test_json_lines_writer() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonLinesWriter(writer_stream, close_stream_on_finish, 'utf-8');
    await writer.write(['foo', 'bar']);
    await writer.finish();
    let data_text = writer_stream.get_text();
    test_common.assert_equal('{"col_1": "foo", "col_2": "bar"}\n', data_text);
}


async function test_json_lines_writer_write_error() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonLinesWriter(writer_stream, close_stream_on_finish, 'utf-8');
    await writer.write(['foo', 'bar']);
    writer_stream.emulate_error(new Error('fake barbaz write error'));
    try {
        await writer.write(['foo', 'bar']);
    } catch (e) {
        test_common.assert_equal('fake barbaz write error', e.message);
        return;
    }
    test_common.assert(false, 'Expected write exception not thrown');
}


async function test_json_lines_writer_header_dups() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonLinesWriter(writer_stream, close_stream_on_finish, 'utf-8');
    writer.set_header(['foo', 'bar', 'foo']);
    let warnings = writer.get_warnings();
    test_common.assert_arrays_are_equal(['Deduplicated output json keys to avoid data loss: "foo"'], warnings);
}


async function process_test_case(tmp_tests_dir, test_case) {
    let test_name = test_case['test_name'];
    let query = test_case['query_js'];
    if (!query)
        return;
    num_json_tests_executed++;

    let input_table_path = test_case['input_table_path'];
    let local_debug_mode = test_common.get_default(test_case, 'debug_mode', false);
    query = query.replace('###UT_TESTS_DIR###', script_dir);

    let expected_output_table_path = test_common.get_default(test_case, 'expected_output_table_path', null);
    // FIXME add a test with error
    // FIXME add a test with warning
    let expected_error = test_common.get_default(test_case, 'expected_error', null) || test_common.get_default(test_case, 'expected_error_js', null);
    let expected_error_exact = test_common.get_default(test_case, 'expected_error_exact', false);
    let expected_warnings = test_common.get_default(test_case, 'expected_warnings', []).sort();
    let input_json_lines = test_common.get_default(test_case, 'input_json_lines', true);
    let output_json_lines = test_common.get_default(test_case, 'output_json_lines', true);
    let expected_md5 = null;
    let actual_output_table_path = null;
    if (expected_output_table_path !== null) {
        let output_file_name = path.basename(expected_output_table_path);
        expected_output_table_path = path.join(script_dir, expected_output_table_path);
        actual_output_table_path = path.join(tmp_tests_dir, output_file_name);
        expected_md5 = test_common.calc_file_md5(expected_output_table_path);
    } else {
        actual_output_table_path = path.join(tmp_tests_dir, 'expected_empty_file');
    }
    let absolute_output_table_path = test_common.get_default(test_case, 'absolute_output_table_path', null);
    if (absolute_output_table_path !== null)
        actual_output_table_path = absolute_output_table_path;

    let warnings = [];
    try {
        await rbql_json.query_json(query, input_table_path, actual_output_table_path, warnings);
    } catch (e) {
        if (local_debug_mode)
            throw(e);
        if(!expected_error) {
            throw(e);
        }
        if (expected_error_exact) {
            test_common.assert_equal(expected_error, e.message);
        } else {
            test_common.assert(e.message.indexOf(expected_error) != -1, `Expected error is not substring of actual. Expected error: ${expected_error}, Actual error: ${e.message}`);
        }
        return;
    }
    warnings = test_common.normalize_warnings(warnings).sort();
    test_common.assert_arrays_are_equal(expected_warnings, warnings);
    let actual_md5 = test_common.calc_file_md5(actual_output_table_path);
    test_common.assert(expected_md5 == actual_md5, `md5 mismatch in test "${test_name}". Expected table: ${expected_output_table_path}, Actual table: ${actual_output_table_path}`);
}


async function test_json_scenarios() {
    let tests_file_path = 'json_files_unit_tests.json';
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
    console.log(`Number of json json tests executed: ${num_json_tests_executed}`)
    test_common.rmtree(tmp_tests_dir);
}


async function test_everything() {
    await test_json_lines_writer();
    await test_json_lines_writer_write_error();
    await test_json_lines_writer_header_dups();
    await test_json_scenarios();
}


function main() {
    console.log('Starting JS JSON unit tests');

    var scheme = {
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    debug_mode = args['dbg'];
    test_common.set_debug_mode(debug_mode);

    rbql_json = require('../rbql-js/rbql_json.js');
    rbql = require('../rbql-js/rbql.js');
    test_everything().then(v => { console.log('Finished JS JSON unit tests'); }).catch(error_info => { console.log('JS JSON tests failed:' + JSON.stringify(error_info)); console.log(error_info.stack); });
}


main();

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


function test_basic_single_object() {
  const balancing_iterator = new rbql_json.BalancingIterator();

  // Test full object pushed at once
  balancing_iterator.add_fragment('{"foo": "bar"}');
  test_common.assert_equal(balancing_iterator.get_current_blob(), '{"foo": "bar"}', true);

  // Erasing should leave nothing
  balancing_iterator.advance();
  test_common.assert_equal(balancing_iterator.get_current_blob(), null, true);
}

function test_chunked_data_stream() {
  const balancing_iterator = new rbql_json.BalancingIterator();

  // Partial object pushes
  balancing_iterator.add_fragment('  { "key": ');
  test_common.assert_equal(balancing_iterator.get_current_blob(), null, true);

  balancing_iterator.add_fragment('"val');
  test_common.assert_equal(balancing_iterator.get_current_blob(), null, true);

  balancing_iterator.add_fragment('ue" }');
  test_common.assert_equal(balancing_iterator.get_current_blob(), '{ "key": "value" }', true);
}

function test_multiple_objects_and_erasing() {
  const balancing_iterator = new rbql_json.BalancingIterator();

  // Feed multiple objects sequentially in various chunks
  balancing_iterator.add_fragment('{"first": 1} {"second": 2} [1, 2, 3]');

  // First object
  test_common.assert_equal(balancing_iterator.get_current_blob(), '{"first": 1}', true);

  // Erase first and verify second object (with leading whitespace handled)
  balancing_iterator.advance();
  test_common.assert_equal(balancing_iterator.get_current_blob(), '{"second": 2}', true);

  // Erase second and verify third object (array)
  balancing_iterator.advance();
  test_common.assert_equal(balancing_iterator.get_current_blob(), '[1, 2, 3]', true);

  // Erase final object
  balancing_iterator.advance();
  test_common.assert_equal(balancing_iterator.get_current_blob(), null, true);
}

function test_complex_nesting_and_strings() {
  const balancing_iterator = new rbql_json.BalancingIterator();

  // Strings containing brackets and escaped quotes
  const complexJson = '{"text": "Bracket } inside \\" string { ", "nested": [1, {"a": true}]}';
  
  balancing_iterator.add_fragment(complexJson.slice(0, 20));
  test_common.assert_equal(balancing_iterator.get_current_blob(), null, true);

  balancing_iterator.add_fragment(complexJson.slice(20));
  test_common.assert_equal(balancing_iterator.get_current_blob(), complexJson, true);

  // Verify parsing the stringified object gives a valid JS structure
  const parsed = JSON.parse(balancing_iterator.get_current_blob());
  test_common.assert_objects_are_equal(
    parsed,
    { text: 'Bracket } inside " string { ', nested: [1, { a: true }] },
    true
  );
}

function test_invalid_prefix_exceptions() {
  // Test non-JSON prefix - should parse OK
  const balancing_iterator1 = new rbql_json.BalancingIterator();
  balancing_iterator1.add_fragment('non_json_prefix {"a": 1}');
  test_common.assert_equal(balancing_iterator1.get_current_blob(), "non_json_prefix", false);
  balancing_iterator1.advance();
  test_common.assert_equal(balancing_iterator1.get_current_blob(), '{"a": 1}', false);
  balancing_iterator1.advance();
  test_common.assert(balancing_iterator1.empty());
  test_common.assert(!balancing_iterator1.has_trailing_data());
  
  // Test unexpected closing brace
  const balancing_iterator2 = new rbql_json.BalancingIterator();
  let caught = false;
  try {
      balancing_iterator2.add_fragment('} {"a": 1}');
  } catch (e) {
      caught = true;
  }
  test_common.assert(caught, "Expected exception for invalid starting closing brace");
}

function run_all_json_balancing_iterator_tests() {
  test_basic_single_object();
  test_chunked_data_stream();
  test_multiple_objects_and_erasing();
  test_complex_nesting_and_strings();
  test_invalid_prefix_exceptions();
}


async function test_json_lines_writer() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonWriter(writer_stream, close_stream_on_finish, 'utf-8', '\n', /*pretty_indent=*/null, /*wrap_in_json_array=*/false);
    await writer.write(['foo', 'bar']);
    await writer.finish();
    let data_text = writer_stream.get_text();
    test_common.assert_equal('{"col_1": "foo", "col_2": "bar"}\n', data_text);
}


async function test_json_array_object_writer() {
    let writer_stream = null;
    let close_stream_on_finish = false;
    let writer = null;
    let data_text = null;
    let pretty_indent = null;
    let line_separator = '\n';
    let wrap_in_json_array = true;

    writer_stream = new test_common.PseudoWritable();
    close_stream_on_finish = false;
    writer = new rbql_json.JsonWriter(writer_stream, close_stream_on_finish, 'utf-8');
    await writer.write(['foo', 'bar']);
    await writer.finish();
    data_text = writer_stream.get_text();
    test_common.assert_equal('[\n{"col_1": "foo", "col_2": "bar"}\n]\n', data_text);

    writer_stream = new test_common.PseudoWritable();
    close_stream_on_finish = false;
    wrap_in_json_array = false;
    writer = new rbql_json.JsonWriter(writer_stream, close_stream_on_finish, 'utf-8', line_separator, pretty_indent, wrap_in_json_array);
    await writer.write(['foo', 'bar']);
    await writer.finish();
    data_text = writer_stream.get_text();
    test_common.assert_equal('{"col_1": "foo", "col_2": "bar"}\n', data_text);
}


async function test_json_lines_writer_write_error() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonWriter(writer_stream, close_stream_on_finish, 'utf-8', '\n', /*pretty_indent=*/null, /*wrap_in_json_array=*/false);
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
    let writer = new rbql_json.JsonWriter(writer_stream, close_stream_on_finish, 'utf-8', '\n', /*pretty_indent=*/null, /*wrap_in_json_array=*/false);
    writer.set_header(['foo', 'bar', 'foo']);
    let warnings = writer.get_warnings();
    test_common.assert_arrays_are_equal(['Deduplicated output json keys to avoid data loss: "foo"'], warnings);
}


async function test_stringify_json_line_python_style() {
    let input_object = {'foo':1,' leading space':'  multi,\nline,\ndata'};
    let actual = rbql_json.stringify_json_line_python_style(input_object);
    let expected = '{"foo": 1, " leading space": "  multi,\\nline,\\ndata"}';
    test_common.assert_equal(expected, actual);
}


async function test_deduplicate_header_keys() {
    let input_keys = ['a', 'b'];
    let [actual_output_keys, actual_deduplicated_keys] = rbql_json.deduplicate_header_keys(input_keys);
    test_common.assert_arrays_are_equal(['a', 'b'], actual_output_keys);
    test_common.assert_arrays_are_equal([], actual_deduplicated_keys);

    input_keys = ['a', 'a'];
    [actual_output_keys, actual_deduplicated_keys] = rbql_json.deduplicate_header_keys(input_keys);
    test_common.assert_arrays_are_equal(['a', 'a_2'], actual_output_keys);
    test_common.assert_arrays_are_equal(['a'], actual_deduplicated_keys);

    input_keys = ['a', 'a', 'a_2'];
    [actual_output_keys, actual_deduplicated_keys] = rbql_json.deduplicate_header_keys(input_keys);
    test_common.assert_arrays_are_equal(['a', 'a_2', 'a_2_2'], actual_output_keys);
    test_common.assert_arrays_are_equal(['a', 'a_2'], actual_deduplicated_keys);

    input_keys = [];
    [actual_output_keys, actual_deduplicated_keys] = rbql_json.deduplicate_header_keys(input_keys);
    test_common.assert_arrays_are_equal([], actual_output_keys);
    test_common.assert_arrays_are_equal([], actual_deduplicated_keys);
}


async function test_get_json_object_to_write() {
    let header = ['bar'];
    let fields = ['foo'];
    let actual_object = rbql_json.get_json_object_to_write(header, fields);
    test_common.assert_objects_are_equal('foo', actual_object);

    header = ['bar'];
    fields = ['foo', 'bar'];
    try {
        rbql_json.get_json_object_to_write(header, fields);
        test_common.assert(false, 'Expected get_json_object_to_write exception not thrown');
    } catch (e) {
        test_common.assert_equal('Inconsistent number of columns in output header and the current record: 1 != 2', e.message);
    }

    header = ['h1', 'h2'];
    fields = ['v1', 'v2'];
    actual_object = rbql_json.get_json_object_to_write(header, fields);
    test_common.assert_objects_are_equal({ h1: 'v1', h2: 'v2' }, actual_object);

    header = [];
    fields = ['v1', 'v2'];
    actual_object = rbql_json.get_json_object_to_write(header, fields);
    test_common.assert_objects_are_equal({ col_1: 'v1', col_2: 'v2' }, actual_object);
}


async function process_test_case(tmp_tests_dir, test_case) {
    let test_name = test_case['test_name'];
    if (debug_mode) {
        console.log("test_name:" + test_name);
    }
    let query = test_case['query_js'];
    if (!query)
        return;
    num_json_tests_executed++;

    let input_table_path = test_case['input_table_path'];
    let local_debug_mode = test_common.get_default(test_case, 'debug_mode', false);
    query = query.replace('###UT_TESTS_DIR###', script_dir);

    let expected_output_table_path = test_common.get_default(test_case, 'expected_output_table_path', null);
    // FIXME add test with input unwrap json array and output not wrapped and vice versa
    let expected_error = test_common.get_default(test_case, 'expected_error', null) || test_common.get_default(test_case, 'expected_error_js', null);
    let expected_error_exact = test_common.get_default(test_case, 'expected_error_exact', false);
    let expected_warnings = test_common.get_default(test_case, 'expected_warnings', []).sort();
    let unwrap_array = test_common.get_default(test_case, 'unwrap_array', false);
    let pretty_indent = test_common.get_default(test_case, 'pretty_indent', null);
    let params = {'pretty_indent': pretty_indent, 'unwrap_array': unwrap_array};
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
        await rbql_json.query_json(query, input_table_path, actual_output_table_path, warnings, params);
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
    run_all_json_balancing_iterator_tests();
    await test_json_lines_writer();
    await test_json_array_object_writer();
    await test_json_lines_writer_write_error();
    await test_json_lines_writer_header_dups();
    await test_deduplicate_header_keys();
    await test_stringify_json_line_python_style();
    await test_get_json_object_to_write();
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

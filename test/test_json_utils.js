const cli_parser = require('../rbql-js/cli_parser.js');
const test_common = require('./test_common.js');
const json_utils = require('../rbql-js/json_utils.js');

function test_basic_single_object() {
  const finalizer = new json_utils.JsonFinalizer();

  // Test full object pushed at once
  finalizer.add_data('{"foo": "bar"}');
  test_common.assert_equal(finalizer.get_first_object(), '{"foo": "bar"}', true);

  // Erasing should leave nothing
  finalizer.erase_first_object();
  test_common.assert_equal(finalizer.get_first_object(), null, true);
}

function test_chunked_data_stream() {
  const finalizer = new json_utils.JsonFinalizer();

  // Partial object pushes
  finalizer.add_data('  { "key": ');
  test_common.assert_equal(finalizer.get_first_object(), null, true);

  finalizer.add_data('"val');
  test_common.assert_equal(finalizer.get_first_object(), null, true);

  finalizer.add_data('ue" }');
  test_common.assert_equal(finalizer.get_first_object(), '{ "key": "value" }', true);
}

function test_multiple_objects_and_erasing() {
  const finalizer = new json_utils.JsonFinalizer();

  // Feed multiple objects sequentially in various chunks
  finalizer.add_data('{"first": 1} {"second": 2} [1, 2, 3]');

  // First object
  test_common.assert_equal(finalizer.get_first_object(), '{"first": 1}', true);

  // Erase first and verify second object (with leading whitespace handled)
  finalizer.erase_first_object();
  test_common.assert_equal(finalizer.get_first_object(), '{"second": 2}', true);

  // Erase second and verify third object (array)
  finalizer.erase_first_object();
  test_common.assert_equal(finalizer.get_first_object(), '[1, 2, 3]', true);

  // Erase final object
  finalizer.erase_first_object();
  test_common.assert_equal(finalizer.get_first_object(), null, true);
}

function test_complex_nesting_and_strings() {
  const finalizer = new json_utils.JsonFinalizer();

  // Strings containing brackets and escaped quotes
  const complexJson = '{"text": "Bracket } inside \\" string { ", "nested": [1, {"a": true}]}';
  
  finalizer.add_data(complexJson.slice(0, 20));
  test_common.assert_equal(finalizer.get_first_object(), null, true);

  finalizer.add_data(complexJson.slice(20));
  test_common.assert_equal(finalizer.get_first_object(), complexJson, true);

  // Verify parsing the stringified object gives a valid JS structure
  const parsed = JSON.parse(finalizer.get_first_object());
  test_common.assert_objects_are_equal(
    parsed,
    { text: 'Bracket } inside " string { ', nested: [1, { a: true }] },
    true
  );
}

function test_invalid_prefix_exceptions() {
  // Test non-JSON prefix
  const finalizer1 = new json_utils.JsonFinalizer();
  finalizer1.add_data('invalid_prefix {"a": 1}');
  
  let caught1 = false;
  try {
    finalizer1.get_first_object();
  } catch (e) {
    caught1 = true;
  }
  test_common.assert(caught1, "Expected exception for invalid leading text prefix");

  // Test unexpected closing brace
  const finalizer2 = new json_utils.JsonFinalizer();
  finalizer2.add_data('} {"a": 1}');

  let caught2 = false;
  try {
    finalizer2.get_first_object();
  } catch (e) {
    caught2 = true;
  }
  test_common.assert(caught2, "Expected exception for invalid starting closing brace");
}

function run_all_json_finalizer_tests() {
  test_basic_single_object();
  test_chunked_data_stream();
  test_multiple_objects_and_erasing();
  test_complex_nesting_and_strings();
  test_invalid_prefix_exceptions();
}


async function test_everything() {
    run_all_json_finalizer_tests();
}

function main() {
    console.log('Starting JS json_utils unit tests');

    var scheme = {
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    debug_mode = args['dbg'];
    test_common.set_debug_mode(debug_mode);

    rbql_json = require('../rbql-js/rbql_json.js');
    rbql = require('../rbql-js/rbql.js');
    test_everything().then(v => { console.log('Finished JS json_utils unit tests'); }).catch(error_info => { console.log('JS JSON tests failed:' + JSON.stringify(error_info)); console.log(error_info.stack); });
}


main();

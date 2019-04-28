const fs = require('fs');
const build_engine = require('../rbql-js/build_engine.js');
const cli_parser = require('../rbql-js/cli_parser.js');
var engine = null;
var debug_mode = false;

// FIXME delete all debug console.log statements at the end

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


function arrays_are_equal(a, b) {
    if (a.length != b.length)
        return false;
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) {
            //console.log('mismatch at ' + i + ' a[i] = ' + a[i] + ', b[i] = ' + b[i]);
            return false;
        }
    }
    return true;
}


function tables_are_equal(a, b) {
    if (a.length != b.length)
        return false;
    for (var i = 0; i < a.length; i++) {
        if (!arrays_are_equal(a[i], b[i]))
            return false;
    }
    return true;
}


function objects_are_equal(a, b) {
    if (a === b)
        return true;
    if (a == null || typeof a != 'object' || b == null || typeof b != 'object')
        return false;
    var num_props_in_a = 0;
    var num_props_in_b = 0;
    for (var prop in a)
         num_props_in_a += 1;
    for (var prop in b) {
        num_props_in_b += 1;
        if (!(prop in a) || !objects_are_equal(a[prop], b[prop]))
            return false;
    }
    return num_props_in_a == num_props_in_b;
}


function normalize_warnings(warnings) {
    let result = [];
    for (let warning of warnings) {
        if (warning.indexOf('Number of fields in "input" table is not consistent') != -1) {
            result.push('inconsistent input records');
        } else {
            assert(false, 'Unknown warning');
        }
    }
    return result;
}


function test_comment_strip() {
    let a = ` // a comment  `;
    let a_strp = engine.strip_comments(a);
    assert(a_strp === '');
}


function test_string_literals_separation() {
    let test_cases = [];
    test_cases.push(['Select 100 order by a1', []]);
    test_cases.push(['Select `hello` order by a1', ['`hello`']]);
    test_cases.push(['Select "hello", 100 order by a1', ['"hello"']]);
    test_cases.push(['Select "hello", *, "world" 100 order by a1 desc', ['"hello"', '"world"']])
    test_cases.push(['Select "hello", "world", "hello \\" world", "hello \\\\\\" world", "hello \\\\\\\\\\\\\\" world" order by "world"', ['"hello"', '"world"', '"hello \\" world"', '"hello \\\\\\" world"', '"hello \\\\\\\\\\\\\\" world"', '"world"']])
    for (let i = 0; i < test_cases.length; i++) {
        let test_case = test_cases[i];
        let query = test_case[0];
        let expected_literals = test_case[1];
        let [format_expression, string_literals] = engine.separate_string_literals_js(query);
        assert(arrays_are_equal(expected_literals, string_literals));
        assert(query == engine.combine_string_literals(format_expression, string_literals));
    }
}


function test_separate_actions() {
        let query = 'select top   100 *, a2, a3 inner  join /path/to/the/file.tsv on a1 == b3 where a4 == "hello" and parseInt(b3) == 100 order by parseInt(a7) desc ';
        let expected_res = {'JOIN': {'text': '/path/to/the/file.tsv on a1 == b3', 'join_subtype': 'INNER JOIN'}, 'SELECT': {'text': '*, a2, a3', 'top': 100}, 'WHERE': {'text': 'a4 == "hello" and parseInt(b3) == 100'}, 'ORDER BY': {'text': 'parseInt(a7)', 'reverse': true}};
        let test_res = engine.separate_actions(query);
        assert(objects_are_equal(test_res, expected_res));
}


function test_except_parsing() {
    let except_part = null;

    except_part = '  a1,a2,a3, a4,a5, a6 ,   a7  ,a8';
    assert('select_except(afields, [0,1,2,3,4,5,6,7])' === engine.translate_except_expression(except_part));

    except_part = 'a1 ,  a2,a3, a4,a5, a6 ,   a7  , a8  ';
    assert('select_except(afields, [0,1,2,3,4,5,6,7])' === engine.translate_except_expression(except_part));

    except_part = 'a1';
    assert('select_except(afields, [0])' === engine.translate_except_expression(except_part));
}


function test_join_parsing() {
    let join_part = null;
    let catched = false;
    join_part = '/path/to/the/file.tsv on a1 == b3';
    assert(arrays_are_equal(['/path/to/the/file.tsv', 'safe_join_get(afields, 0)', 2], engine.parse_join_expression(join_part)));

    join_part = ' file.tsv on b20== a12  ';
    assert(arrays_are_equal(['file.tsv', 'safe_join_get(afields, 11)', 19], engine.parse_join_expression(join_part)));

    join_part = '/path/to/the/file.tsv on a1==a12  ';
    catched = false;
    try {
        engine.parse_join_expression(join_part);
    } catch (e) {
        catched = true;
        assert(e.toString().indexOf('Invalid join syntax') != -1);
    }
    assert(catched);

    join_part = ' Bon b1 == a12 ';
    catched = false;
    try {
        engine.parse_join_expression(join_part);
    } catch (e) {
        catched = true;
        assert(e.toString().indexOf('Invalid join syntax') != -1);
    }
    assert(catched);
}


function test_update_translation() {
    let rbql_src = '  a1 =  a2  + b3, a2=a4  if b3 == a2 else a8, a8=   100, a30  =200/3 + 1  ';
    let indent = ' '.repeat(8);
    let expected_dst = [];
    expected_dst.push('safe_set(up_fields, 1,  a2  + b3)');
    expected_dst.push(indent + 'safe_set(up_fields, 2,a4  if b3 == a2 else a8)');
    expected_dst.push(indent + 'safe_set(up_fields, 8,   100)');
    expected_dst.push(indent + 'safe_set(up_fields, 30,200/3 + 1)');
    expected_dst = expected_dst.join('\n');
    let test_dst = engine.translate_update_expression(rbql_src, indent);
    assert(test_dst == expected_dst);
}



function test_select_translation() {
    let rbql_src = null;
    let test_dst = null;
    let canonic_dst = null;

    rbql_src = ' *, a1,  a2,a1,*,*,b1, * ,   * ';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([ a1,  a2,a1]).concat(star_fields).concat([]).concat(star_fields).concat([b1]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst, 'translation 1');

    rbql_src = ' *, a1,  a2,a1,*,*,*,b1, * ,   * ';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([ a1,  a2,a1]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([b1]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst, 'translation 2');

    rbql_src = ' * ';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,* ';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,*, * ';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,*, * , *';
    test_dst = engine.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);
}


//class TestJsonTables(unittest.TestCase):
//
//    def process_test_case(self, test_case):
//        test_name = test_case['test_name']
//        #print(test_name)
//        query = test_case.get('query_python', None)
//        if query is None:
//            self.assertTrue(test_case.get('query_js', None) is not None)
//            return # Skip this test
//        input_table = test_case['input_table']
//        join_table = test_case.get('join_table', None)
//        user_init_code = test_case.get('python_init_code', '')
//        expected_output_table = test_case.get('expected_output_table', None)
//        expected_error = test_case.get('expected_error', None)
//        expected_warnings = test_case.get('expected_warnings', [])
//        input_iterator = TableIterator(input_table)
//        output_writer = TableWriter()
//        join_tables_registry = None if join_table is None else SingleTableTestRegistry(join_table)
//
//        error_info, warnings = rbql.generic_run(query, input_iterator, output_writer, join_tables_registry, user_init_code=user_init_code)
//
//        warnings = sorted(normalize_warnings(warnings))
//        expected_warnings = sorted(expected_warnings)
//        self.assertEqual(expected_warnings, warnings, 'Inside json test: {}'.format(test_name))
//        self.assertTrue((expected_error is not None) == (error_info is not None), 'Inside json test: {}'.format(test_name))
//        if expected_error is not None:
//            self.assertTrue(error_info['message'].find(expected_error) != -1, 'Inside json test: {}'.format(test_name))
//        else:
//            output_table = output_writer.table
//            round_floats(expected_output_table)
//            round_floats(output_table)
//            self.assertEqual(expected_output_table, output_table)
//
//
//    def test_json_tables(self):
//        tests_file = os.path.join(script_dir, 'rbql_unit_tests.json')
//        with open(tests_file) as f:
//            tests = json.loads(f.read())
//            for test in tests:
//                self.process_test_case(test)


function get_default(obj, key, default_value) {
    if (obj.hasOwnProperty(key))
        return obj[key];
    return default_value;
}


function process_test_case(tests, test_id) {
    if (test_id >= tests.length)
        return;
    let test_case = tests[test_id];
    let test_name = test_case['test_name'];
    console.log('running rbql test: ' + test_name);
    let query = test_case['query_js'];
    let input_table = test_case['input_table'];
    let expected_output_table = get_default(test_case, 'expected_output_table', null);
    let expected_error = get_default(test_case, 'expected_error', null);
    let expected_warnings = get_default(test_case, 'expected_warnings', []);
    let input_iterator = new engine.TableIterator(input_table);
    let output_writer = new engine.TableWriter();
    let error_handler = function(error_type, error_msg) {
        assert(error_msg === expected_error);
        process_test_case(tests, test_id + 1);
    }
    let success_handler = function(warnings) {
        assert(expected_error === null);
        warnings = normalize_warnings(warnings).sort();
        assert(arrays_are_equal(expected_warnings, warnings));
        let output_table = output_writer.table;
        assert(tables_are_equal(expected_output_table, output_table), 'Expected and output tables mismatch');
        process_test_case(tests, test_id + 1);
    }
    engine.generic_run(query, input_iterator, output_writer, success_handler, error_handler, null, '', debug_mode);
}


function test_json_tables() {
    let tests_file_path = 'rbql_unit_tests.json';
    let tests = JSON.parse(fs.readFileSync(tests_file_path, 'utf-8'));
    process_test_case(tests, 0);
}


function test_rbql_query_parsing() {
    test_comment_strip();
    test_string_literals_separation();
    test_separate_actions();
    test_except_parsing();
    test_join_parsing();
    test_update_translation();
    test_select_translation();
    test_json_tables();
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
        die("engine.js must be rebuild from template.js and builder.js");
    }

    engine = require('../rbql-js/engine.js')

    test_rbql_query_parsing();


    console.log('Finished JS unit tests');
}


main();

const fs = require('fs');
const build_engine = require('../rbql-js/build_engine.js');
const cli_parser = require('../rbql-js/cli_parser.js');
const test_common = require('./test_common.js');
var rbql = null;
var debug_mode = false;



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


function test_comment_strip() {
    let a = ` // a comment  `;
    let a_strp = rbql.strip_comments(a);
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
        let [format_expression, string_literals] = rbql.separate_string_literals_js(query);
        test_common.assert_arrays_are_equal(expected_literals, string_literals);
        assert(query == rbql.combine_string_literals(format_expression, string_literals));
    }
}


function test_separate_actions() {
        let query = 'select top   100 *, a2, a3 inner  join /path/to/the/file.tsv on a1 == b3 where a4 == "hello" and parseInt(b3) == 100 order by parseInt(a7) desc ';
        let expected_res = {'JOIN': {'text': '/path/to/the/file.tsv on a1 == b3', 'join_subtype': 'INNER JOIN'}, 'SELECT': {'text': '*, a2, a3', 'top': 100}, 'WHERE': {'text': 'a4 == "hello" and parseInt(b3) == 100'}, 'ORDER BY': {'text': 'parseInt(a7)', 'reverse': true}};
        let test_res = rbql.separate_actions(query);
        assert(test_common.objects_are_equal(test_res, expected_res));
}


function test_except_parsing() {
    let except_part = null;

    except_part = '  a1,a2,a3, a4,a5, a[6] ,   a7  ,a8';
    assert('select_except(record_a, [0,1,2,3,4,5,6,7])' === rbql.translate_except_expression(except_part, {'a1': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a[6]': 5, 'a7': 6, 'a8': 7}));

    except_part = 'a[1] ,  a2,a3, a4,a5, a6 ,   a[7]  , a8  ';
    assert('select_except(record_a, [0,1,2,3,4,5,6,7])' === rbql.translate_except_expression(except_part, {'a[1]': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a6': 5, 'a[7]': 6, 'a8': 7}));

    except_part = 'a1';
    assert('select_except(record_a, [0])' === rbql.translate_except_expression(except_part, {'a1': 0, 'a2': 1, 'a3': 2, 'a4': 3, 'a5': 4, 'a6': 5, 'a7': 6, 'a8': 7}));
}


function test_join_parsing() {
    let join_part = '/path/to/the/file.tsv on a1 == b3';
    test_common.assert_arrays_are_equal(['/path/to/the/file.tsv', 'a1', 'b3'], rbql.parse_join_expression(join_part));

    join_part = ' file.tsv on b[20]== a.name  ';
    test_common.assert_arrays_are_equal(['file.tsv', 'b[20]', 'a.name'], rbql.parse_join_expression(join_part));

    join_part = ' Bon b1 == a.age ';
    let catched = false;
    try {
        rbql.parse_join_expression(join_part);
    } catch (e) {
        catched = true;
        assert(e.toString().indexOf('Invalid join syntax') != -1);
    }
    assert(catched);

    test_common.assert_arrays_are_equal(['safe_join_get(record_a, 0)', 1], rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'a1', 'b2'));

    catched = false;
    try {
        rbql.parse_join_expression(join_part);
        rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'a1', 'a2');
    } catch (e) {
        catched = true;
        assert(e.toString().indexOf('Invalid join syntax') != -1);
    }
    assert(catched);

    catched = false;
    try {
        rbql.parse_join_expression(join_part);
        rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'a1', 'b10');
    } catch (e) {
        catched = true;
        assert(e.toString().indexOf('Invalid join syntax') != -1);
    }
    assert(catched);

    catched = false;
    try {
        rbql.parse_join_expression(join_part);
        rbql.resolve_join_variables({'a1': 0, 'a2': 1}, {'b1': 0, 'b2': 1}, 'b1', 'b2');
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
    expected_dst.push('safe_set(up_fields, 0,  a2  + b3);');
    expected_dst.push(indent + 'safe_set(up_fields, 1,a4  if b3 == a2 else a8);');
    expected_dst.push(indent + 'safe_set(up_fields, 7,   100);');
    expected_dst.push(indent + 'safe_set(up_fields, 29,200/3 + 1);');
    expected_dst = expected_dst.join('\n');
    let test_dst = rbql.translate_update_expression(rbql_src, {'a1': 0, 'a2': 1, 'a4': 3, 'a8': 7, 'a30': 29}, indent);
    test_common.assert_equal(test_dst, expected_dst);
}



function test_select_translation() {
    let rbql_src = null;
    let test_dst = null;
    let canonic_dst = null;

    rbql_src = ' *, a1,  a2,a1,*,*,b1, * ,   * ';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([ a1,  a2,a1]).concat(star_fields).concat([]).concat(star_fields).concat([b1]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst, 'translation 1');

    rbql_src = ' *, a1,  a2,a1,*,*,*,b1, * ,   * ';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([ a1,  a2,a1]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([b1]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst, 'translation 2');

    rbql_src = ' * ';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,* ';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,*, * ';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);

    rbql_src = ' *,*, * , *';
    test_dst = rbql.translate_select_expression_js(rbql_src);
    canonic_dst = '[].concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([]).concat(star_fields).concat([])';
    assert(canonic_dst === test_dst);
}


async function test_json_tables() {
    let tests_file_path = 'rbql_unit_tests.json';
    let tests = JSON.parse(fs.readFileSync(tests_file_path, 'utf-8'));
    for (let test_case of tests) {
        let test_name = test_case['test_name'];
        console.log('Running rbql test: ' + test_name);
        let query = test_common.get_default(test_case, 'query_js', null);
        // FIXME randomly replace ai with a[i], see Python code
        if (query == null)
            continue;
        let input_table = test_case['input_table'];
        let join_table = test_common.get_default(test_case, 'join_table', null);
        let user_init_code = test_common.get_default(test_case, 'js_init_code', '');
        let expected_output_table = test_common.get_default(test_case, 'expected_output_table', null);
        let expected_error = test_common.get_default(test_case, 'expected_error', null);
        let expected_error_type = test_common.get_default(test_case, 'expected_error_type', null);
        let expected_error_exact = test_common.get_default(test_case, 'expected_error_exact', false)
        if (expected_error == null) {
            expected_error = test_common.get_default(test_case, 'expected_error_js', null);
        }
        let expected_warnings = test_common.get_default(test_case, 'expected_warnings', []);
        let output_table = [];
        let warnings = null;
        let error_type = null;
        try {
            warnings = await rbql.table_run(query, input_table, output_table, join_table, user_init_code);
        } catch (e) {
            if (e.constructor.name === 'RbqlParsingError') {
                error_type == 'query_parsing';
            } else if (e.constructor.name === 'RbqlIOHandlingError') {
                error_type == 'IO handling';
            }
            if (expected_error_type)
                test_common.assert_equal(expected_error_type, error_type);
            if(!expected_error) {
                throw(e);
            }
            if (expected_error_exact) {
                test_common.assert_equal(expected_error, e.message);
            } else {
                assert(e.message.indexOf(expected_error) != -1);
            }
            continue;
        }
        warnings = test_common.normalize_warnings(warnings).sort();
        test_common.assert_arrays_are_equal(expected_warnings, warnings);
        test_common.round_floats(output_table);
        console.log("output_table:" + output_table); //FOR_DEBUG
        test_common.assert_tables_are_equal(expected_output_table, output_table);
    }
}


async function test_direct_table_queries() {
    let output_table = [];
    let expected_table = [['foo test', 1], ['bar test', 2]];

    let warnings = await rbql.table_run('select a2 + " test", a1 limit 2', [[1, 'foo'], [2, 'bar'], [3, 'hello']], output_table);
    assert(warnings.length == 0);
    test_common.assert_tables_are_equal(expected_table, output_table);
}


async function test_everything() {
    test_comment_strip();
    test_string_literals_separation();
    test_separate_actions();
    test_except_parsing();
    test_join_parsing();
    test_update_translation();
    test_select_translation();
    await test_direct_table_queries();
    await test_json_tables();
}


function main() {
    console.log('Starting JS unit tests');

    var scheme = {
        '--auto-rebuild-engine': {'boolean': true, 'help': 'Auto rebuild engine'},
        '--dbg': {'boolean': true, 'help': 'Run tests in debug mode (require worker template from a tmp module file)'}
    };
    var args = cli_parser.parse_cmd_args(process.argv, scheme);

    if (args['auto-rebuild-engine']) {
        build_engine.build_engine();
    }

    debug_mode = args['dbg'];

    let engine_text_current = build_engine.read_engine_text();
    let engine_text_expected = build_engine.build_engine_text();
    if (engine_text_current != engine_text_expected) {
        die("rbql.js must be rebuild from template.js and builder.js");
    }

    rbql = require('../rbql-js/rbql.js')
    if (debug_mode)
        rbql.set_debug_mode();

    test_everything().then(v => { console.log('Finished JS unit tests'); }).catch(error_info => { console.log('JS tests failed:' + error_info); console.log(error_info.stack); });
}


main();

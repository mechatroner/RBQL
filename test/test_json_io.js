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

async function test_json_lines_writer() {
    let writer_stream = new test_common.PseudoWritable();
    let close_stream_on_finish = false;
    let writer = new rbql_json.JsonLinesWriter(writer_stream, close_stream_on_finish, 'utf-8');
    await writer.write(['foo', 'bar']);
    await writer.finish();
    let data_text = writer_stream.get_text();
    test_common.assert_equal('{"col_1":"foo","col_2":"bar"}\n', data_text);
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


async function test_everything() {
    await test_json_lines_writer();
    await test_json_lines_writer_write_error();
    await test_json_lines_writer_header_dups();
}


function main() {
    // FIXME add file-based test cases unit tests.
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

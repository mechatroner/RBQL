const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');
const crypto = require('crypto');
const stream = require('stream');

//const csv_utils = require('../rbql-js/csv_utils.js');
const cli_parser = require('../rbql-js/cli_parser.js');
const test_common = require('./test_common.js');

var rbql_json = null;
var rbql = null;

const script_dir = __dirname;

var debug_mode = false;

async function test_json_array_writer() {
    test_common.assert(false, 'testing the harness');
}


async function test_everything() {
    await test_json_array_writer();
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

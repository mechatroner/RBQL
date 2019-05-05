const os = require('os');
const path = require('path');
const fs = require('fs');
const readline = require('readline');

var rbql = null;
var rbq_csv = null;
const csv_utils = require('../rbql-js/csv_utils.js');


function test_everything() {
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

    test_everything();


    console.log('Finished JS unit tests');
}


main();

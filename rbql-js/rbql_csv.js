const fs = require('fs');
const os = require('os');
const path = require('path');

const rbql = require('./rbql.js');
const csv_utils = require('./csv_utils.js');


function is_ascii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}


function read_user_init_code(rbql_init_source_path) {
    return fs.readFileSync(rbql_init_source_path, 'utf-8');
}


function csv_run(query, input_stream, input_delim, input_policy, output_stream, output_delim, output_policy, csv_encoding, external_success_handler, external_error_handler, custom_init_path=null, node_debug_mode=false) {
    try {
        if (input_delim == '"' && input_policy == 'quoted') {
            external_error_handler('IO handling', 'Double quote delimiter is incompatible with "quoted" policy');
            return;
        }
        if (csv_encoding == 'latin-1')
            csv_encoding = 'binary';
        if (!is_ascii(query) && csv_encoding == 'binary') {
            external_error_handler('IO handling', 'To use non-ascii characters in query enable UTF-8 encoding instead of latin-1/binary');
            return;
        }

        let user_init_code = '';
        let default_init_source_path = path.join(os.homedir(), '.rbql_init_source.js');
        if (custom_init_path !== null) {
            user_init_code = read_user_init_code(custom_init_path);
        } else if (fs.existsSync(default_init_source_path)) {
            user_init_code = read_user_init_code(default_init_source_path);
        }

        let join_tables_registry = new csv_utils.FileSystemCSVRegistry(input_delim, input_policy, csv_encoding);
        let input_iterator = new csv_utils.CSVRecordIterator(input_stream, csv_encoding, input_delim, input_policy);
        let output_writer = new csv_utils.CSVWriter(output_stream, csv_encoding, output_delim, output_policy);

        rbql.generic_run(query, input_iterator, output_writer, external_success_handler, external_error_handler, join_tables_registry, user_init_code, node_debug_mode);
    } catch (e) {
        if (node_debug_mode) {
            console.log('Unexpected exception, dumping stack trace:');
            console.log(e.stack);
        }
        external_error_handler('unexpected', String(e));
    }
}



module.exports.is_ascii = is_ascii;
module.exports.read_user_init_code = read_user_init_code;
module.exports.csv_run = csv_run;

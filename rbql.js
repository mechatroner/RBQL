const os = require('os');
const path = require('path');
const fs = require('fs')
const readline = require('readline');

const version = '0.1.0';

const GROUP_BY = 'GROUP BY';
const UPDATE = 'UPDATE';
const SELECT = 'SELECT';
const JOIN = 'JOIN';
const INNER_JOIN = 'INNER JOIN';
const LEFT_JOIN = 'LEFT JOIN';
const STRICT_LEFT_JOIN = 'STRICT LEFT JOIN';
const ORDER_BY = 'ORDER BY';
const WHERE = 'WHERE';
const LIMIT = 'LIMIT';

const rbql_home_dir = __dirname;
const user_home_dir = os.homedir();
const table_names_settings_path = path.join(user_home_dir, '.rbql_table_names');
const table_index_path = path.join(user_home_dir, '.rbql_table_index');


function RBParsingError(msg) {
    this.msg = msg;
    this.name = 'RBParsingError';
}


function strip_js_comments(cline) {
    cline = cline.trim();
    if (cline.startsWith('//'))
        return '';
    return cline;
}


function separate_string_literals_js(rbql_expression) {
    // *? - means non-greedy *
    var rgx = /(`|\"|\')((?<!\\)(\\\\)*\\\1|.)*?\1/g;
    var match_obj = null;
    while((match_obj = rgx.exec(rbql_expression)) !== null) {
        console.log(match_obj[0]);
        //FIXME do something
    }
}


// FIXME template.js.raw must export rb_transform() function, which accepts streams instead of file names
function parse_to_js(rbql_lines, js_dst, input_delim, input_policy, out_delim, out_policy, csv_encoding, import_modules) {
    if (input_delim == '"' && input_policy == 'quoted')
        throw new RBParsingError('Double quote delimiter is incompatible with "quoted" policy');
    rbql_lines = rbql_lines.map(strip_js_comments);
    rbql_lines = rbql_lines.filter(line => line.length);
    var full_rbql_expression = rbql_lines.join(' ');
    var separation_result = separate_string_literals_js(full_rbql_expression);
    var format_expression = separation_result[0];
    var string_literals = separation_result[1];
}

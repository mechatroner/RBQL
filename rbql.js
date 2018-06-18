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


function AssertionError(msg) {
    this.msg = msg;
    this.name = 'AssertionError';
}


function assert(condition, message=null) {
    if (!condition) {
        if (!message) {
            message = 'Assertion error';
        }
        throw new AssertionError(message);
    }
}


function strip_js_comments(cline) {
    cline = cline.trim();
    if (cline.startsWith('//'))
        return '';
    return cline;
}


function str_strip(src) {
    return src.replace(/^ +| +$/g, '');
}


function replace_all(src, search, replacement) {
    return src.split(search).join(replacement);
}


function separate_string_literals_js(rbql_expression) {
    // The regex consists of 3 almost identicall parts, the only difference is quote type
    var rgx = /('(\\(\\\\)*'|[^'])*')|("(\\(\\\\)*"|[^"])*")|(`(\\(\\\\)*`|[^`])*`)/g;
    var match_obj = null;
    var format_parts = [];
    var string_literals = [];
    var idx_before = 0;
    while((match_obj = rgx.exec(rbql_expression)) !== null) {
        var literal_id = string_literals.length;
        var string_literal = match_obj[0];
        var start_index = match_obj.index;
        format_parts.push(rbql_expression.substring(idx_before, start_index));
        format_parts.push(`###RBQL_STRING_LITERAL###${literal_id}`);
        idx_before = rgx.lastIndex;
    }
    format_parts.push(idx_before, rbql_expression.length);
    var format_expression = format_parts.join('');
    format_expression = format_expression.replace(/\t/g, ' ');
    return [format_expression, string_literals];
}


function get_all_matches(regex, text) {
    result = [];
    while((match_obj = rgx.exec(rbql_expression)) !== null) {
        result.push(match_obj);
    }
    return result;
}


function locate_statements(rbql_expression) {
    statement_groups = [];
    statement_groups.push([STRICT_LEFT_JOIN, LEFT_JOIN, INNER_JOIN, JOIN]);
    statement_groups.push([SELECT]);
    statement_groups.push([ORDER_BY]);
    statement_groups.push([WHERE]);
    statement_groups.push([UPDATE]);
    statement_groups.push([GROUP_BY]);
    statement_groups.push([LIMIT]);
    result = [];
    for (var ig = 0; ig < statement_groups.length; ig++) {
        for (var is = 0; is < statement_groups[ig].length; is++) {
            var rgxp = new RegExp('(?:^| )' + replace_all(statement, ' ', ' *') + ' ', 'ig');
            var matches = get_all_matches(rgxp, rbql_expression);
            if (!matches.length)
                continue;
            if (matches.length > 1)
                throw new RBParsingError(`More than one ${statement} statements found`);
            assert(matches.length == 1);
            var match = matches[0];
            var match_str = match[0];
            result.push([match.index, match.index + match_str.length, match_str]);
        }
    }
    result.sort(function(a, b) { return a[0] - b[0]; });
    return result;
}


function separate_actions(rbql_expression) {
    rbql_expression = str_strip(rbql_expression);
    var ordered_statements = locate_statements(rbql_expression);
    var result = {};
    for (var i = 0; i < ordered_statements.length; i++) {
        var statement_start = ordered_statements[i][0];
        var span_start = ordered_statements[i][1];
        var statement = ordered_statements[i][2];
        var span_end = i + 1 < ordered_statements.length ? ordered_statements[i + 1][0] : rbql_expression.length;
        assert(statement_start < span_start);
        assert(span_start <= span_end);
        var span = rbql_expression.substring(span_start, span_end);
        var statement_params = {};
        if ([STRICT_LEFT_JOIN, LEFT_JOIN, INNER_JOIN, JOIN].indexOf(statement) != -1) {
            statement_params['join_subtype'] = statement;
            statement = JOIN;
        }

        if (statement == UPDATE) {
            if (statement_start != 0)
                throw new RBParsingError('UPDATE keyword must be at the beginning of the query');
            span = span.replace(/^ *SET/i, '');
        }

        if (statement == ORDER_BY) {
            span = span.replace(/ ASC *$/i, '');
            var new_span = span.replace('/ DESC *$/i', '');
            if (new_span != span) {
                span = new_span;
                statement_params['reverse'] = true;
            } else {
                statement_params['reverse'] = false;
            }
        }

        if (statement == SELECT) {
            if (statement_start != 0)
                throw new RBParsingError('SELECT keyword must be at the beginning of the query');
            var match = /^ *TOP *([0-9]+) /i.exec(span);
            if (match !== null) {
                statement_params['top'] = parseInt(match[1]);
                span = span.substr(match.index + match[0].length);
            }
            match = /^ *DISTINCT *(COUNT)? /i.exec(span);
            if (match !== null) {
                statement_params['distinct'] = true;
                if (match[1]) {
                    statement_params['distinct_count'] = true;
                }
                span = span.substr(match.index + match[0].length);
            }
        }
        statement_params['text'] = str_strip(span);
        result[statement] = statement_params;
    }
    if (!result.hasOwnProperty(SELECT) && !result.hasOwnProperty(UPDATE)) {
        throw new RBParsingError('Query must contain either SELECT or UPDATE statement');
    }
    assert(result.hasOwnProperty(SELECT) != result.hasOwnProperty(UPDATE));
    return result;
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
    var rb_actions = separate_actions(format_expression);
}

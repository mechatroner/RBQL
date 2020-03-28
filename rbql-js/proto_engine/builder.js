const external_js_template_text = codegeneration_pseudo_function_include_combine("template.js");
// ^ The expression above will cause builder.js and tempalte.js to be combined to autogenerate rbql.js: builder.js + template.js -> ../rbql.js
// Expression is written as a function to pacify the linter.
// Unit tests will ensure that rbql.js is indeed a concatenation of builder.js and template.js


// This module works with records only. It is CSV-agnostic.
// Do not add CSV-related logic or variables/functions/objects like "delim", "separator" etc


// TODO get rid of functions with "_js" suffix


// TODO replace prototypes with classes: this improves readability


const version = '0.16.0';

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
const EXCEPT = 'EXCEPT';


class RbqlParsingError extends Error {}
class RbqlIOHandlingError extends Error {}
class AssertionError extends Error {}
class RbqlRuntimeError extends Error {}

var debug_mode = false;

function assert(condition, message=null) {
    if (!condition) {
        if (!message) {
            message = 'Assertion error';
        }
        throw new AssertionError(message);
    }
}


function regexp_escape(text) {
    // From here: https://stackoverflow.com/a/6969486/2898283
    return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');  // $& means the whole matched text
}


function get_ambiguous_error_msg(variable_name) {
    return `Ambiguous variable name: "${variable_name}" is present both in input and in join tables`;
}


function get_all_matches(regexp, text) {
    var result = [];
    let match_obj = null;
    while((match_obj = regexp.exec(text)) !== null) {
        result.push(match_obj);
    }
    return result;
}


function replace_all(src, search, replacement) {
    return src.split(search).join(replacement);
}


function str_strip(src) {
    return src.replace(/^ +| +$/g, '');
}


function rbql_meta_format(template_src, meta_params) {
    for (const [key, value] of Object.entries(meta_params)) {
        var template_src_upd = replace_all(template_src, key, value);
        assert(template_src_upd != template_src);
        template_src = template_src_upd;
    }
    return template_src;
}


function strip_comments(cline) {
    cline = cline.trim();
    if (cline.startsWith('//'))
        return '';
    return cline;
}


function combine_string_literals(backend_expression, string_literals) {
    for (var i = 0; i < string_literals.length; i++) {
        backend_expression = replace_all(backend_expression, `###RBQL_STRING_LITERAL${i}###`, string_literals[i]);
    }
    return backend_expression;
}


function parse_basic_variables(query_text, prefix, dst_variables_map) {
    assert(prefix == 'a' || prefix == 'b');
    let rgx = new RegExp(`(?:^|[^_a-zA-Z0-9])${prefix}([1-9][0-9]*)(?:$|(?=[^_a-zA-Z0-9]))`, 'g');
    let matches = get_all_matches(rgx, query_text);
    for (let i = 0; i < matches.length; i++) {
        let field_num = parseInt(matches[i][1]);
        dst_variables_map[prefix + String(field_num)] = {initialize: true, index: field_num - 1};
    }
}


function parse_array_variables(query_text, prefix, dst_variables_map) {
    assert(prefix == 'a' || prefix == 'b');
    let rgx = new RegExp(`(?:^|[^_a-zA-Z0-9])${prefix}\\[([1-9][0-9]*)\\]`, 'g');
    let matches = get_all_matches(rgx, query_text);
    for (let i = 0; i < matches.length; i++) {
        let field_num = parseInt(matches[i][1]);
        dst_variables_map[`${prefix}[${field_num}]`] = {initialize: true, index: field_num - 1};
    }
}


function js_string_escape_column_name(column_name, quote_char) {
    column_name = column_name.replace(/\\/g, '\\\\');
    column_name = column_name.replace(/\n/g, '\\n');
    column_name = column_name.replace(/\r/g, '\\r');
    column_name = column_name.replace(/\t/g, '\\t');
    if (quote_char === "'")
        return column_name.replace(/'/g, "\\'");
    if (quote_char === '"')
        return column_name.replace(/"/g, '\\"');
    assert(quote_char === "`");
    return column_name.replace(/`/g, "\\`");
}


function query_probably_has_dictionary_variable(query_text, column_name) {
    let rgx = new RegExp('[-a-zA-Z0-9_:;+=!.,()%^#@&* ]+', 'g');
    let continuous_name_segments = get_all_matches(rgx, column_name);
    for (let continuous_segment of continuous_name_segments) {
        if (query_text.indexOf(continuous_segment) == -1)
            return false;
    }
    return true;
}


function parse_dictionary_variables(query_text, prefix, column_names, dst_variables_map) {
    // The purpose of this algorithm is to minimize number of variables in varibale_map to improve performance, ideally it should be only variables from the query

    // FIXME to prevent typos in attribute names either use query-based variable parsing which can properly handle back-tick strings or wrap "a" and "b" variables with ES6 Proxies https://stackoverflow.com/a/25658975/2898283
    assert(prefix === 'a' || prefix === 'b');
    let dict_test_rgx = new RegExp(`(?:^|[^_a-zA-Z0-9])${prefix}\\[`);
    if (query_text.search(dict_test_rgx) == -1)
        return;
    for (let i = 0; i < column_names.length; i++) {
        let column_name = column_names[i];
        if (query_probably_has_dictionary_variable(query_text, column_name)) {
            let escaped_column_name = js_string_escape_column_name(column_name, '"');
            dst_variables_map[`${prefix}["${escaped_column_name}"]`] = {initialize: true, index: i};
            escaped_column_name = js_string_escape_column_name(column_name, "'");
            dst_variables_map[`${prefix}['${escaped_column_name}']`] = {initialize: false, index: i};
            escaped_column_name = js_string_escape_column_name(column_name, "`");
            dst_variables_map[`${prefix}[\`${escaped_column_name}\`]`] = {initialize: false, index: i};
        }
    }
}


function parse_attribute_variables(query_text, prefix, column_names, column_names_source, dst_variables_map) {
    // The purpose of this algorithm is to minimize number of variables in varibale_map to improve performance, ideally it should be only variables from the query

    assert(prefix === 'a' || prefix === 'b');
    let rgx = new RegExp(`(?:^|[^_a-zA-Z0-9])${prefix}\\.([_a-zA-Z][_a-zA-Z0-9]*)`, 'g');
    let matches = get_all_matches(rgx, query_text);
    let column_names_from_query = matches.map(v => v[1]);
    for (let column_name of column_names_from_query) {
        let zero_based_idx = column_names.indexOf(column_name);
        if (zero_based_idx != -1) {
            dst_variables_map[`${prefix}.${column_name}`] = {initialize: true, index: zero_based_idx};
        } else {
            throw new RbqlParsingError(`Unable to find column "${column_name}" in ${prefix == 'a' ? 'input' : 'join'} ${column_names_source}`);
        }
    }
}


function map_variables_directly(query_text, column_names, dst_variables_map) {
    for (let i = 0; i < column_names.length; i++) {
        let column_name = column_names[i];
        if ( /^[_a-zA-Z][_a-zA-Z0-9]*$/.exec(column_name) === null)
            throw new RbqlIOHandlingError(`Unable to use column name "${column_name}" as RBQL/JS variable`)
        if (query_text.indexOf(column_name) != -1)
            dst_variables_map[column_name] = {initialize: true, index: i};
    }
}


function ensure_no_ambiguous_variables(query_text, input_column_names, join_column_names) {
    let join_column_names_set = new Set(join_column_names);
    for (let column_name of input_column_names) {
        if (join_column_names_set.has(column_name) && query_text.indexOf(column_name) != -1) // False positive is tolerable here
            throw new RbqlParsingError(get_ambiguous_error_msg(column_name));
    }
}


function parse_join_expression(src) {
    src = str_strip(src);
    const invalid_join_syntax_error = 'Invalid join syntax. Valid syntax: <JOIN> /path/to/B/table on a... == b... [and a... == b... [and ... ]]';
    let rgx = /^ *([^ ]+) +on +/i;
    let match = rgx.exec(src);
    if (match === null)
        throw new RbqlParsingError(invalid_join_syntax_error);
    let table_id = match[1];
    src = src.substr(match[0].length);

    let variable_pairs = [];
    var pair_rgx = /^([^ =]+) *==? *([^ =]+)/;
    var and_rgx = /^ +(and|&&) +/i;
    while (true) {
        match = pair_rgx.exec(src);
        if (match === null)
            throw new RbqlParsingError(invalid_join_syntax_error);
        variable_pairs.push([match[1], match[2]]);
        src = src.substr(match[0].length)
        if (!src.length)
            break;
        match = and_rgx.exec(src);
        if (match === null)
            throw new RbqlParsingError(invalid_join_syntax_error);
        src = src.substr(match[0].length);
    }
    return [table_id, variable_pairs];
}


function resolve_join_variables(input_variables_map, join_variables_map, variable_pairs, string_literals) {
    let lhs_variables = [];
    let rhs_indices = [];
    const valid_join_syntax_msg = 'Valid JOIN syntax: <JOIN> /path/to/B/table on a... == b... [and a... == b... [and ... ]]';
    for (let variable_pair of variable_pairs) {
        let [join_var_1, join_var_2] = variable_pair;
        join_var_1 = combine_string_literals(join_var_1, string_literals);
        join_var_2 = combine_string_literals(join_var_2, string_literals);
        if (input_variables_map.hasOwnProperty(join_var_1) && join_variables_map.hasOwnProperty(join_var_1))
            throw new RbqlParsingError(get_ambiguous_error_msg(join_var_1));
        if (input_variables_map.hasOwnProperty(join_var_2) && join_variables_map.hasOwnProperty(join_var_2))
            throw new RbqlParsingError(get_ambiguous_error_msg(join_var_2));
        if (input_variables_map.hasOwnProperty(join_var_2))
            [join_var_1, join_var_2] = [join_var_2, join_var_1];

        let [lhs_key_index, rhs_key_index] = [null, null];
        if (['NR', 'a.NR', 'aNR'].indexOf(join_var_1) != -1) {
            lhs_key_index = -1;
        } else if (input_variables_map.hasOwnProperty(join_var_1)) {
            lhs_key_index = input_variables_map[join_var_1].index;
        } else {
            throw new RbqlParsingError(`Unable to parse JOIN expression: Input table does not have field "${join_var_1}"\n${valid_join_syntax_msg}`);
        }

        if (['b.NR', 'bNR'].indexOf(join_var_2) != -1) {
            rhs_key_index = -1;
        } else if (join_variables_map.hasOwnProperty(join_var_2)) {
            rhs_key_index = join_variables_map[join_var_2].index;
        } else {
            throw new RbqlParsingError(`Unable to parse JOIN expression: Join table does not have field "${join_var_2}"\n${valid_join_syntax_msg}`);
        }

        let lhs_join_var_expression = lhs_key_index == -1 ? 'NR' : `safe_join_get(record_a, ${lhs_key_index})`;
        rhs_indices.push(rhs_key_index);
        lhs_variables.push(lhs_join_var_expression);
    }
    return [lhs_variables, rhs_indices];
}


function generate_common_init_code(query_text, variable_prefix) {
    assert(variable_prefix == 'a' || variable_prefix == 'b');
    let result = [];
    result.push(`${variable_prefix} = new Object();`);
    let base_var = variable_prefix == 'a' ? 'NR' : 'bNR';
    let attr_var = `${variable_prefix}.NR`;
    if (query_text.indexOf(attr_var) != -1)
        result.push(`${attr_var} = ${base_var};`);
    if (variable_prefix == 'a' && query_text.indexOf('aNR') != -1)
        result.push('aNR = NR;');
    return result;
}


function generate_init_statements(query_text, variables_map, join_variables_map, indent) {
    let code_lines = generate_common_init_code(query_text, 'a');
    let simple_var_name_rgx = /^[_0-9a-zA-Z]+$/;
    for (const [variable_name, var_info] of Object.entries(variables_map)) {
        if (var_info.initialize) {
            let variable_declaration_keyword = simple_var_name_rgx.exec(variable_name) ? 'var ' : '';
            code_lines.push(`${variable_declaration_keyword}${variable_name} = safe_get(record_a, ${var_info.index});`);
        }
    }
    if (join_variables_map) {
        code_lines = code_lines.concat(generate_common_init_code(query_text, 'b'));
        for (const [variable_name, var_info] of Object.entries(join_variables_map)) {
            if (var_info.initialize) {
                let variable_declaration_keyword = simple_var_name_rgx.exec(variable_name) ? 'var ' : '';
                code_lines.push(`${variable_declaration_keyword}${variable_name} = record_b === null ? null : safe_get(record_b, ${var_info.index});`);
            }
        }
    }
    for (let i = 1; i < code_lines.length; i++) {
        code_lines[i] = indent + code_lines[i];
    }
    return code_lines.join('\n');
}


function replace_star_count(aggregate_expression) {
    var rgx = /(^|,) *COUNT\( *\* *\) *(?:$|(?=,))/ig;
    var result = aggregate_expression.replace(rgx, '$1 COUNT(1)');
    return str_strip(result);
}


function replace_star_vars(rbql_expression) {
    let star_rgx = /(?:^|,) *(\*|a\.\*|b\.\*) *(?=$|,)/g;
    let matches = get_all_matches(star_rgx, rbql_expression);
    let last_pos = 0;
    let result = '';
    for (let match of matches) {
        let star_expression = match[1];
        let replacement_expression = ']).concat(' + {'*': 'star_fields', 'a.*': 'record_a', 'b.*': 'record_b'}[star_expression] + ').concat([';
        if (last_pos < match.index)
            result += rbql_expression.substring(last_pos, match.index);
        result += replacement_expression;
        last_pos = match.index + match[0].length + 1 // Adding one to skip the lookahead comma
    }
    result += rbql_expression.substring(last_pos);
    return result;
}


function translate_update_expression(update_expression, input_variables_map, string_literals, indent) {
    let first_assignment = str_strip(update_expression.split('=')[0]);
    let first_assignment_error = `Unable to parse "UPDATE" expression: the expression must start with assignment, but "${first_assignment}" does not look like an assignable field name`;

    let assignment_looking_rgx = /(?:^|,) *(a[.#a-zA-Z0-9\[\]_]*) *=(?=[^=])/g;
    let update_statements = [];
    let pos = 0;
    while (true) {
        let match = assignment_looking_rgx.exec(update_expression);
        if (update_statements.length == 0 && (match === null || match.index != 0)) {
            throw new RbqlParsingError(first_assignment_error);
        }
        if (match === null) {
            update_statements[update_statements.length - 1] += str_strip(update_expression.substr(pos)) + ');';
            break;
        }
        if (update_statements.length)
            update_statements[update_statements.length - 1] += str_strip(update_expression.substring(pos, match.index)) + ');';
        let dst_var_name = combine_string_literals(str_strip(match[1]), string_literals);
        if (!input_variables_map.hasOwnProperty(dst_var_name))
            throw new RbqlParsingError(`Unable to parse "UPDATE" expression: Unknown field name: "${dst_var_name}"`);
        let var_index = input_variables_map[dst_var_name].index;
        let current_indent = update_statements.length ? indent : '';
        update_statements.push(`${current_indent}safe_set(up_fields, ${var_index}, `);
        pos = match.index + match[0].length;
    }
    return combine_string_literals(update_statements.join('\n'), string_literals);
}


function translate_select_expression_js(select_expression) {
    var translated = replace_star_count(select_expression);
    translated = replace_star_vars(translated);
    translated = str_strip(translated);
    if (!translated.length)
        throw new RbqlParsingError('"SELECT" expression is empty');
    return `[].concat([${translated}])`;
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
        string_literals.push(string_literal);
        var start_index = match_obj.index;
        format_parts.push(rbql_expression.substring(idx_before, start_index));
        format_parts.push(`###RBQL_STRING_LITERAL${literal_id}###`);
        idx_before = rgx.lastIndex;
    }
    format_parts.push(rbql_expression.substring(idx_before));
    var format_expression = format_parts.join('');
    format_expression = format_expression.replace(/\t/g, ' ');
    return [format_expression, string_literals];
}


function locate_statements(rbql_expression) {
    let statement_groups = [];
    statement_groups.push([STRICT_LEFT_JOIN, LEFT_JOIN, INNER_JOIN, JOIN]);
    statement_groups.push([SELECT]);
    statement_groups.push([ORDER_BY]);
    statement_groups.push([WHERE]);
    statement_groups.push([UPDATE]);
    statement_groups.push([GROUP_BY]);
    statement_groups.push([LIMIT]);
    statement_groups.push([EXCEPT]);
    var result = [];
    for (var ig = 0; ig < statement_groups.length; ig++) {
        for (var is = 0; is < statement_groups[ig].length; is++) {
            var statement = statement_groups[ig][is];
            var rgxp = new RegExp('(?:^| )' + replace_all(statement, ' ', ' *') + '(?= )', 'ig');
            var matches = get_all_matches(rgxp, rbql_expression);
            if (!matches.length)
                continue;
            if (matches.length > 1)
                throw new RbqlParsingError(`More than one "${statement}" statements found`);
            assert(matches.length == 1);
            var match = matches[0];
            var match_str = match[0];
            result.push([match.index, match.index + match_str.length, statement]);
            break; // Break to avoid matching a sub-statement from the same group e.g. "INNER JOIN" -> "JOIN"
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
                throw new RbqlParsingError('UPDATE keyword must be at the beginning of the query');
            span = span.replace(/^ *SET/i, '');
        }

        if (statement == ORDER_BY) {
            span = span.replace(/ ASC *$/i, '');
            var new_span = span.replace(/ DESC *$/i, '');
            if (new_span != span) {
                span = new_span;
                statement_params['reverse'] = true;
            } else {
                statement_params['reverse'] = false;
            }
        }

        if (statement == SELECT) {
            if (statement_start != 0)
                throw new RbqlParsingError('SELECT keyword must be at the beginning of the query');
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
        throw new RbqlParsingError('Query must contain either SELECT or UPDATE statement');
    }
    assert(result.hasOwnProperty(SELECT) != result.hasOwnProperty(UPDATE));
    return result;
}


function find_top(rb_actions) {
    if (rb_actions.hasOwnProperty(LIMIT)) {
        var rgx = /^[0-9]+$/;
        if (rgx.exec(rb_actions[LIMIT]['text']) === null) {
            throw new RbqlParsingError('LIMIT keyword must be followed by an integer');
        }
        var result = parseInt(rb_actions[LIMIT]['text']);
        return result;
    }
    var select_action = rb_actions[SELECT];
    if (select_action && select_action.hasOwnProperty('top')) {
        return select_action['top'];
    }
    return null;
}


function indent_user_init_code(user_init_code) {
    let source_lines = user_init_code.split(/(?:\r\n)|\r|\n/);
    source_lines = source_lines.map(line => '    ' + line);
    return source_lines.join('\n');
}


function translate_except_expression(except_expression, input_variables_map, string_literals) {
    let skip_vars = except_expression.split(',');
    skip_vars = skip_vars.map(str_strip);
    let skip_indices = [];
    for (let var_name of skip_vars) {
        var_name = combine_string_literals(var_name, string_literals);
        if (!input_variables_map.hasOwnProperty(var_name))
            throw new RbqlParsingError(`Unknown field in EXCEPT expression: "${var_name}"`);
        skip_indices.push(input_variables_map[var_name].index);
    }
    skip_indices = skip_indices.sort((a, b) => a - b);
    let indices_str = skip_indices.join(',');
    return `select_except(record_a, [${indices_str}])`;
}


function HashJoinMap(record_iterator, key_indices) {
    this.max_record_len = 0;
    this.hash_map = new Map();
    this.record_iterator = record_iterator;
    this.key_index = null;
    this.key_indices = null;
    this.nr = 0;

    this.get_single_key = function(nr, fields) {
        if (this.key_index >= fields.length)
            throw new RbqlRuntimeError(`No field with index ${this.key_index + 1} at record ${this.nr} in "B" table`);
        return this.key_index === -1 ? this.nr : fields[this.key_index];
    }

    this.get_multi_key = function(nr, fields) {
        let result = [];
        for (let ki of this.key_indices) {
            if (ki >= fields.length)
                throw new RbqlRuntimeError(`No field with index ${ki + 1} at record ${this.nr} in "B" table`);
            result.push(ki === -1 ? this.nr : fields[ki]);
        }
        return JSON.stringify(result);
    }

    if (key_indices.length == 1) {
        this.key_index = key_indices[0];
        this.polymorphic_get_key = this.get_single_key;
    } else {
        this.key_indices = key_indices;
        this.polymorphic_get_key = this.get_multi_key;
    }

    this.build = async function() {
        while (true) {
            let fields = await this.record_iterator.get_record();
            if (fields === null)
                break;
            this.nr += 1;
            let nf = fields.length;
            this.max_record_len = Math.max(this.max_record_len, nf);
            let key = this.polymorphic_get_key(this.nr, fields);
            let key_records = this.hash_map.get(key);
            if (key_records === undefined) {
                this.hash_map.set(key, [[this.nr, nf, fields]]);
            } else {
                key_records.push([this.nr, nf, fields]);
            }
        }
    };

    this.get_join_records = function(key) {
        let result = this.hash_map.get(key);
        if (result === undefined)
            return [];
        return result;
    };

    this.get_warnings = function() {
        return this.record_iterator.get_warnings();
    };
}


function cleanup_query(query_text) {
    return query_text.split('\n').map(strip_comments).filter(line => line.length).join(' ');
}


async function parse_to_js(query_text, js_template_text, input_iterator, join_tables_registry, user_init_code) {
    user_init_code = indent_user_init_code(user_init_code);
    query_text = cleanup_query(query_text);
    var [format_expression, string_literals] = separate_string_literals_js(query_text);
    var input_variables_map = await input_iterator.get_variables_map(query_text);

    var rb_actions = separate_actions(format_expression);

    var js_meta_params = {};
    js_meta_params['__RBQLMP__user_init_code'] = user_init_code;
    js_meta_params['__RBQLMP__version'] = version;

    if (rb_actions.hasOwnProperty(ORDER_BY) && rb_actions.hasOwnProperty(UPDATE))
        throw new RbqlParsingError('"ORDER BY" is not allowed in "UPDATE" queries');


    if (rb_actions.hasOwnProperty(GROUP_BY)) {
        if (rb_actions.hasOwnProperty(ORDER_BY) || rb_actions.hasOwnProperty(UPDATE))
            throw new RbqlParsingError('"ORDER BY", "UPDATE" and "DISTINCT" keywords are not allowed in aggregate queries');
        var aggregation_key_expression = rb_actions[GROUP_BY]['text'];
        js_meta_params['__RBQLMP__aggregation_key_expression'] = '[' + combine_string_literals(aggregation_key_expression, string_literals) + ']';
    } else {
        js_meta_params['__RBQLMP__aggregation_key_expression'] = 'null';
    }

    let join_map = null;
    let join_variables_map = null;
    if (rb_actions.hasOwnProperty(JOIN)) {
        var [rhs_table_id, variable_pairs] = parse_join_expression(rb_actions[JOIN]['text']);
        if (join_tables_registry === null)
            throw new RbqlParsingError('JOIN operations are not supported by the application');
        let join_record_iterator = join_tables_registry.get_iterator_by_table_id(rhs_table_id);
        if (!join_record_iterator)
            throw new RbqlParsingError(`Unable to find join table: "${rhs_table_id}"`);
        join_variables_map = await join_record_iterator.get_variables_map(query_text);
        let [lhs_variables, rhs_indices] = resolve_join_variables(input_variables_map, join_variables_map, variable_pairs, string_literals);
        js_meta_params['__RBQLMP__join_operation'] = `"${rb_actions[JOIN]['join_subtype']}"`;
        js_meta_params['__RBQLMP__lhs_join_var'] = lhs_variables.length == 1 ? lhs_variables[0] : 'JSON.stringify([' + lhs_variables.join(',') + '])';
        join_map = new HashJoinMap(join_record_iterator, rhs_indices);
    } else {
        js_meta_params['__RBQLMP__join_operation'] = 'null';
        js_meta_params['__RBQLMP__lhs_join_var'] = 'null';
    }

    if (rb_actions.hasOwnProperty(WHERE)) {
        var where_expression = rb_actions[WHERE]['text'];
        if (/[^!=]=[^=]/.exec(where_expression)) {
            throw new RbqlParsingError('Assignments "=" are not allowed in "WHERE" expressions. For equality test use "==" or "==="');
        }
        js_meta_params['__RBQLMP__where_expression'] = combine_string_literals(where_expression, string_literals);
    } else {
        js_meta_params['__RBQLMP__where_expression'] = 'true';
    }


    if (rb_actions.hasOwnProperty(UPDATE)) {
        var update_expression = translate_update_expression(rb_actions[UPDATE]['text'], input_variables_map, string_literals, ' '.repeat(8));
        js_meta_params['__RBQLMP__writer_type'] = '"simple"';
        js_meta_params['__RBQLMP__select_expression'] = 'null';
        js_meta_params['__RBQLMP__update_statements'] = combine_string_literals(update_expression, string_literals);
        js_meta_params['__RBQLMP__is_select_query'] = '0';
        js_meta_params['__RBQLMP__top_count'] = 'null';
        js_meta_params['__RBQLMP__init_column_vars_update'] = combine_string_literals(generate_init_statements(format_expression, input_variables_map, join_variables_map, ' '.repeat(4)), string_literals);
        js_meta_params['__RBQLMP__init_column_vars_select'] = '';
    }

    if (rb_actions.hasOwnProperty(SELECT)) {
        js_meta_params['__RBQLMP__init_column_vars_update'] = '';
        js_meta_params['__RBQLMP__init_column_vars_select'] = combine_string_literals(generate_init_statements(format_expression, input_variables_map, join_variables_map, ' '.repeat(4)), string_literals);
        var top_count = find_top(rb_actions);
        js_meta_params['__RBQLMP__top_count'] = top_count === null ? 'null' : String(top_count);
        if (rb_actions[SELECT].hasOwnProperty('distinct_count')) {
            js_meta_params['__RBQLMP__writer_type'] = '"uniq_count"';
        } else if (rb_actions[SELECT].hasOwnProperty('distinct')) {
            js_meta_params['__RBQLMP__writer_type'] = '"uniq"';
        } else {
            js_meta_params['__RBQLMP__writer_type'] = '"simple"';
        }
        if (rb_actions.hasOwnProperty(EXCEPT)) {
            js_meta_params['__RBQLMP__select_expression'] = translate_except_expression(rb_actions[EXCEPT]['text'], input_variables_map, string_literals);
        } else {
            let select_expression = translate_select_expression_js(rb_actions[SELECT]['text']);
            js_meta_params['__RBQLMP__select_expression'] = combine_string_literals(select_expression, string_literals);
        }
        js_meta_params['__RBQLMP__update_statements'] = '';
        js_meta_params['__RBQLMP__is_select_query'] = '1';
    }

    if (rb_actions.hasOwnProperty(ORDER_BY)) {
        var order_expression = rb_actions[ORDER_BY]['text'];
        js_meta_params['__RBQLMP__sort_key_expression'] = combine_string_literals(order_expression, string_literals);
        js_meta_params['__RBQLMP__reverse_flag'] = rb_actions[ORDER_BY]['reverse'] ? 'true' : 'false';
        js_meta_params['__RBQLMP__sort_flag'] = 'true';
    } else {
        js_meta_params['__RBQLMP__sort_key_expression'] = 'null';
        js_meta_params['__RBQLMP__reverse_flag'] = 'false';
        js_meta_params['__RBQLMP__sort_flag'] = 'false';
    }
    var js_code = rbql_meta_format(js_template_text, js_meta_params);
    return [js_code, join_map];
}


function load_module_from_file(js_code) {
    let os = require('os');
    let path = require('path');
    let fs = require('fs');
    var tmp_dir = os.tmpdir();
    var script_filename = 'rbconvert_' + String(Math.random()).replace('.', '_') + '.js';
    let tmp_worker_module_path = path.join(tmp_dir, script_filename);
    fs.writeFileSync(tmp_worker_module_path, js_code);
    let worker_module = require(tmp_worker_module_path);
    return worker_module;
}


function make_inconsistent_num_fields_warning(table_name, inconsistent_records_info) {
    let keys = Object.keys(inconsistent_records_info);
    let entries = [];
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        let record_id = inconsistent_records_info[key];
        entries.push([record_id, key]);
    }
    entries.sort(function(a, b) { return a[0] - b[0]; });
    assert(entries.length > 1);
    let [record_1, num_fields_1] = entries[0];
    let [record_2, num_fields_2] = entries[1];
    let warn_msg = `Number of fields in "${table_name}" table is not consistent: `;
    warn_msg += `e.g. record ${record_1} -> ${num_fields_1} fields, record ${record_2} -> ${num_fields_2} fields`;
    return warn_msg;
}


function TableIterator(table, column_names=null, normalize_column_names=true, variable_prefix='a') {
    this.table = table;
    this.column_names = column_names;
    this.normalize_column_names = normalize_column_names;
    this.variable_prefix = variable_prefix;
    this.nr = 0;
    this.fields_info = new Object();
    this.stopped = false;


    this.stop = function() {
        this.stopped = true;
    };


    this.get_variables_map = async function(query_text) {
        let variable_map = new Object();
        parse_basic_variables(query_text, this.variable_prefix, variable_map);
        parse_array_variables(query_text, this.variable_prefix, variable_map);
        if (this.column_names !== null) {
            if (this.table.length && this.column_names.length != this.table[0].length)
                throw new RbqlIOHandlingError('List of column names and table records have different lengths');
            if (this.normalize_column_names) {
                parse_dictionary_variables(query_text, this.variable_prefix, this.column_names, variable_map);
                parse_attribute_variables(query_text, this.variable_prefix, this.column_names, 'column names list', variable_map);
            } else {
                map_variables_directly(query_text, this.column_names, variable_map);
            }
        }
        return variable_map;
    };


    this.get_record = async function() {
        if (this.stopped)
            return null;
        if (this.nr >= this.table.length)
            return null;
        let record = this.table[this.nr];
        this.nr += 1;
        let num_fields = record.length;
        if (!this.fields_info.hasOwnProperty(num_fields))
            this.fields_info[num_fields] = this.nr;
        return record;
    };

    this.get_warnings = function() {
        if (Object.keys(this.fields_info).length > 1)
            return [make_inconsistent_num_fields_warning('input', this.fields_info)];
        return [];
    };
}


function TableWriter(external_table) {
    this.table = external_table;

    this.write = function(fields) {
        this.table.push(fields);
    };

    this.get_warnings = function() {
        return [];
    };

    this.finish = async function() {};
}


function SingleTableRegistry(table, column_names=null, normalize_column_names=true, table_id='B') {
    this.table = table;
    this.table_id = table_id;
    this.column_names = column_names;
    this.normalize_column_names = normalize_column_names;

    this.get_iterator_by_table_id = function(table_id) {
        if (table_id !== this.table_id)
            throw new RbqlIOHandlingError(`Unable to find join table: "${table_id}"`);
        return new TableIterator(this.table, this.column_names, this.normalize_column_names, 'b');
    };
}


async function query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry=null, user_init_code='') {
    let [js_code, join_map] = await parse_to_js(query_text, external_js_template_text, input_iterator, join_tables_registry, user_init_code);
    let rbql_worker = null;
    try {
        if (debug_mode) {
            // This version works a little faster than eval below. The downside is that a temporary file is created
            rbql_worker = load_module_from_file(js_code);
        } else {
            let module = {'exports': {}};
            eval('(function(){' + js_code + '})()');
            rbql_worker = module.exports;
        }
    } catch (e) {
        if (e instanceof SyntaxError) { 
            if (query_text.toLowerCase().indexOf(' like ') != -1)
                throw new SyntaxError(e.message + "\nRBQL doesn't support LIKE operator, use like() function instead e.g. ... WHERE like(a1, 'foo%bar') ... "); // UT JSON
            if (query_text.toLowerCase().indexOf(' from ') != -1)
                throw new SyntaxError(e.message + "\nRBQL doesn't use \"FROM\" keyword, e.g. you can query 'SELECT *' without FROM"); // UT JSON
        }
        throw e;
    }
    await rbql_worker.rb_transform(input_iterator, join_map, output_writer);
    output_warnings.push(...input_iterator.get_warnings());
    if (join_map)
        output_warnings.push(...join_map.get_warnings());
    output_warnings.push(...output_writer.get_warnings());
}


async function query_table(query_text, input_table, output_table, output_warnings, join_table=null, input_column_names=null, join_column_names=null, normalize_column_names=true, user_init_code='') {
    if (!normalize_column_names && input_column_names !== null && join_column_names !== null)
        ensure_no_ambiguous_variables(query_text, input_column_names, join_column_names);
    let input_iterator = new TableIterator(input_table, input_column_names, normalize_column_names);
    let output_writer = new TableWriter(output_table);
    let join_tables_registry = join_table === null ? null : new SingleTableRegistry(join_table, join_column_names, normalize_column_names);
    await query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code);
}


function set_debug_mode() {
    debug_mode = true;
}


function exception_to_error_info(e) {
    let exceptions_type_map = {
        'RbqlRuntimeError': 'query execution',
        'RbqlParsingError': 'query parsing',
        'SyntaxError': 'JS syntax error',
        'RbqlIOHandlingError': 'IO handling'
    };
    let error_type = 'unexpected';
    if (e.constructor && e.constructor.name && exceptions_type_map.hasOwnProperty(e.constructor.name)) {
        error_type = exceptions_type_map[e.constructor.name];
    }
    let error_msg = e.hasOwnProperty('message') ? e.message : String(e);
    return [error_type, error_msg];
}


module.exports.version = version;
module.exports.query = query;
module.exports.query_table = query_table;

module.exports.TableIterator = TableIterator;
module.exports.TableWriter = TableWriter;
module.exports.SingleTableRegistry = SingleTableRegistry;
module.exports.parse_basic_variables = parse_basic_variables;
module.exports.parse_array_variables = parse_array_variables;
module.exports.parse_dictionary_variables = parse_dictionary_variables;
module.exports.parse_attribute_variables = parse_attribute_variables;
module.exports.get_all_matches = get_all_matches;

module.exports.strip_comments = strip_comments;
module.exports.separate_actions = separate_actions;
module.exports.separate_string_literals_js = separate_string_literals_js;
module.exports.combine_string_literals = combine_string_literals;
module.exports.translate_except_expression = translate_except_expression;
module.exports.parse_join_expression = parse_join_expression;
module.exports.resolve_join_variables = resolve_join_variables;
module.exports.translate_update_expression = translate_update_expression;
module.exports.translate_select_expression_js = translate_select_expression_js;

module.exports.exception_to_error_info = exception_to_error_info;

module.exports.set_debug_mode = set_debug_mode;

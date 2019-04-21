const external_js_template_text = codegeneration_pseudo_function_include_combine("template.js");
// ^ The expression above will cause builder.js and tempalte.js to be combined to autogenerate engine.js: builder.js + template.js -> ../engine.js
// Expression is written as a function to pacify the linter. 
// Unit tests will ensure that engine.js is indeed a concatenation of builder.js and template.js


// This module works with records only. It is CSV-agnostic. 
// Do not add CSV-related logic or variables/functions/objects like "delim", "separator" etc


// TODO rename STRICT_LEFT_JOIN -> STRICT_JOIN
// TODO get rid of functions with "_js" suffix

// TODO rewrite with async/await ?

// FIXME concatenate template.js and builder.js into the third file: package.js
// Unit tests will make sure that package.js is indeed concatenation of template.js and builder.js.
// This hack will simplify interface

const version = '0.5.0';

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
class RbqlRutimeError extends Error {}
class AssertionError extends Error {}


function assert(condition, message=null) {
    if (!condition) {
        if (!message) {
            message = 'Assertion error';
        }
        throw new AssertionError(message);
    }
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


// FIXME do we need to add exception_to_error_info() function? see python version...


function rbql_meta_format(template_src, meta_params) {
    for (var key in meta_params) {
        if (!meta_params.hasOwnProperty(key))
            continue;
        var value = meta_params[key];
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


function parse_join_expression(src) {
    var rgx = /^ *([^ ]+) +on +([ab][0-9]+) *== *([ab][0-9]+) *$/i;
    var match = rgx.exec(src);
    if (match === null) {
        throw new RbqlParsingError('Invalid join syntax. Must be: "<JOIN> /path/to/B/table on a<i> == b<j>"');
    }
    var table_id = match[1];
    var avar = match[2];
    var bvar = match[3];
    if (avar.charAt(0) == 'b') {
        [avar, bvar] = [bvar, avar];
    }
    if (avar.charAt(0) != 'a' || bvar.charAt(0) != 'b') {
        throw new RbqlParsingError('Invalid join syntax. Must be: "<JOIN> /path/to/B/table on a<i> == b<j>"');
    }
    avar = parseInt(avar.substr(1)) - 1;
    var lhs_join_var = `safe_join_get(afields, ${avar})`;
    let rhs_key_index = parseInt(bvar.substr(1)) - 1;
    return [table_id, lhs_join_var, rhs_key_index];
}


function generate_init_statements(column_vars, indent) {
    var init_statements = [];
    for (var i = 0; i < column_vars.length; i++) {
        var var_name = column_vars[i];
        var var_group = var_name.charAt(0);
        var zero_based_idx = parseInt(var_name.substr(1)) - 1;
        if (var_group == 'a') {
            init_statements.push(`var ${var_name} = afields[${zero_based_idx}];`);
        } else {
            init_statements.push(`var ${var_name} = bfields === null ? undefined : bfields[${zero_based_idx}];`);
        }
    }
    for (var i = 1; i < init_statements.length; i++) {
        init_statements[i] = indent + init_statements[i];
    }
    return init_statements.join('\n');
}


function replace_star_count(aggregate_expression) {
    var rgx = /(^|,) *COUNT\( *\* *\) *(?:$|(?=,))/g;
    var result = aggregate_expression.replace(rgx, '$1 COUNT(1)');
    return str_strip(result);
}


function replace_star_vars(rbql_expression) {
    var middle_star_rgx = /(?:^|,) *\* *(?=, *\* *($|,))/g;
    rbql_expression = rbql_expression.replace(middle_star_rgx, ']).concat(star_fields).concat([');
    var last_star_rgx = /(?:^|,) *\* *(?:$|,)/g;
    rbql_expression = rbql_expression.replace(last_star_rgx, ']).concat(star_fields).concat([');
    return rbql_expression;
}


function translate_update_expression(update_expression, indent) {
    var rgx = /(?:^|,) *a([1-9][0-9]*) *=(?=[^=])/g;
    var translated = update_expression.replace(rgx, '\nsafe_set(up_fields, $1,');
    var update_statements = translated.split('\n');
    update_statements = update_statements.map(str_strip);
    if (update_statements.length < 2 || update_statements[0] != '') {
        throw new RbqlParsingError('Unable to parse "UPDATE" expression');
    }
    update_statements = update_statements.slice(1);
    for (var i = 0; i < update_statements.length; i++) {
        update_statements[i] = update_statements[i] + ')';
    }
    for (var i = 1; i < update_statements.length; i++) {
        update_statements[i] = indent + update_statements[i];
    }
    var translated = update_statements.join('\n');
    return translated;
}


function translate_select_expression_js(select_expression) {
    var translated = replace_star_count(select_expression);
    translated = replace_star_vars(translated);
    translated = str_strip(translated);
    if (!translated.length) {
        throw new RbqlParsingError('"SELECT" expression is empty');
    }
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
        format_parts.push(`###RBQL_STRING_LITERAL###${literal_id}`);
        idx_before = rgx.lastIndex;
    }
    format_parts.push(rbql_expression.substring(idx_before));
    var format_expression = format_parts.join('');
    format_expression = format_expression.replace(/\t/g, ' ');
    return [format_expression, string_literals];
}


function combine_string_literals(backend_expression, string_literals) {
    for (var i = 0; i < string_literals.length; i++) {
        backend_expression = replace_all(backend_expression, `###RBQL_STRING_LITERAL###${i}`, string_literals[i]);
    }
    return backend_expression;
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
                throw new RbqlParsingError(`More than one ${statement} statements found`);
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
        var result = parseInt(rb_actions[LIMIT]['text']);
        if (isNaN(result)) {
            throw new RbqlParsingError('LIMIT keyword must be followed by an integer');
        }
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


function extract_column_vars(rbql_expression) {
    var rgx = /(?:^|[^_a-zA-Z0-9])([ab][1-9][0-9]*)(?:$|(?=[^_a-zA-Z0-9]))/g;
    var result = [];
    var seen = {};
    var matches = get_all_matches(rgx, rbql_expression);
    for (var i = 0; i < matches.length; i++) {
        var var_name = matches[i][1];
        if (!seen.hasOwnProperty(var_name)) {
            result.push(var_name);
            seen[var_name] = 1;
        }
    }
    return result;
}


function translate_except_expression(except_expression) {
    let skip_vars = except_expression.split(',');
    let skip_indices = [];
    let rgx = /^a[1-9][0-9]*$/;
    for (let i = 0; i < skip_vars.length; i++) {
        let skip_var = str_strip(skip_vars[i]);
        let match = rgx.exec(skip_var);
        if (match === null) {
            throw new RbqlParsingError('Invalid EXCEPT syntax');
        }
        skip_indices.push(parseInt(skip_var.substring(1)) - 1);
    }
    skip_indices = skip_indices.sort((a, b) => a - b);
    let indices_str = skip_indices.join(',');
    return `select_except(afields, [${indices_str}])`;
}


function HashJoinMap(record_iterator, key_index) {
    this.max_record_len = 0;
    this.hash_map = new Map();
    this.record_iterator = record_iterator;
    this.key_index = key_index;
    this.error_msg = null;
    this.external_error_handler = null;
    this.external_success_handler = null;
    this.nr = 0;

    this.finish_build = function() {
        if (this.error_msg === null) {
            this.external_success_handler();
        } else {
            this.external_error_handler('IO handling', error_msg);
        }
    }

    this.add_record = function(record) {
        this.nr += 1;
        let num_fields = record.length;
        this.max_record_len = Math.max(this.max_record_len, num_fields);
        if (this.key_index >= num_fields) {
            // FIXME unit test this condition
            this.error_msg = `No "b ${this.key_index + 1)}" field at record: ${this.nr} in "B" table`;
            this.record_iterator.finish();
        }
        let key = record[this.key_index];
        let key_records = this.hash_map.get(key);
        if (key_records === undefined) {
            this.hash_map.set(key, [record]);
        } else {
            key_records.push(record);
        }
    }

    this.build = function(success_callback, error_callback) {
        this.external_success_handler = success_callback;
        this.external_error_handler = error_callback;
        this.record_iterator.set_record_callback(this.add_record);
        this.record_iterator.set_finish_callback(this.finish_build);
    }

    this.get_join_records = function(key) {
        let result = this.hash_map.get(key);
        if (result === undefined)
            return [];
        return result;
    }

    this.get_warnings = function() {
        return this.record_iterator.get_warnings();
    }
}


function parse_to_js(query, js_template_text, join_tables_registry, user_init_code) {
    let rbql_lines = query.split('\n');
    rbql_lines = rbql_lines.map(strip_comments);
    rbql_lines = rbql_lines.filter(line => line.length);
    var full_rbql_expression = rbql_lines.join(' ');
    var column_vars = extract_column_vars(full_rbql_expression);
    var [format_expression, string_literals] = separate_string_literals_js(full_rbql_expression);
    var rb_actions = separate_actions(format_expression);

    var js_meta_params = {};
    js_meta_params['__RBQLMP__user_init_code'] = user_init_code;

    if (rb_actions.hasOwnProperty(ORDER_BY) && rb_actions.hasOwnProperty(UPDATE))
        throw new RbqlParsingError('"ORDER BY" is not allowed in "UPDATE" queries');

    if (rb_actions.hasOwnProperty(GROUP_BY)) {
        if (rb_actions.hasOwnProperty(ORDER_BY) || rb_actions.hasOwnProperty(UPDATE))
            throw new RbqlParsingError('"ORDER BY" and "UPDATE" are not allowed in aggregate queries');
        var aggregation_key_expression = rb_actions[GROUP_BY]['text'];
        js_meta_params['__RBQLMP__aggregation_key_expression'] = '[' + combine_string_literals(aggregation_key_expression, string_literals) + ']';
    } else {
        js_meta_params['__RBQLMP__aggregation_key_expression'] = 'null';
    }

    let join_map = null;
    if (rb_actions.hasOwnProperty(JOIN)) {
        var [rhs_table_id, lhs_join_var, rhs_key_index] = parse_join_expression(rb_actions[JOIN]['text']);
        js_meta_params['__RBQLMP__join_operation'] = rb_actions[JOIN]['join_subtype'];
        js_meta_params['__RBQLMP__lhs_join_var'] = lhs_join_var;
        let join_record_iterator = join_tables_registry.get_iterator_by_table_id(rhs_table_id);
        if (!join_record_iterator)
            throw new RbqlParsingError(`Unable to find join table: "${rhs_table_id}"`)
        join_map = HashJoinMap(join_record_iterator, rhs_key_index);
    } else {
        js_meta_params['__RBQLMP__join_operation'] = 'VOID';
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
        var update_expression = translate_update_expression(rb_actions[UPDATE]['text'], ' '.repeat(8));
        js_meta_params['__RBQLMP__writer_type'] = 'simple';
        js_meta_params['__RBQLMP__select_expression'] = 'null';
        js_meta_params['__RBQLMP__update_statements'] = combine_string_literals(update_expression, string_literals);
        js_meta_params['__RBQLMP__is_select_query'] = 'false';
        js_meta_params['__RBQLMP__top_count'] = 'null';
    }

    js_meta_params['__RBQLMP__init_column_vars_update'] = generate_init_statements(column_vars, ' '.repeat(4));
    js_meta_params['__RBQLMP__init_column_vars_select'] = generate_init_statements(column_vars, ' '.repeat(8));

    if (rb_actions.hasOwnProperty(SELECT)) {
        var top_count = find_top(rb_actions);
        js_meta_params['__RBQLMP__top_count'] = top_count === null ? 'null' : String(top_count);
        if (rb_actions[SELECT].hasOwnProperty('distinct_count')) {
            js_meta_params['__RBQLMP__writer_type'] = 'uniq_count';
        } else if (rb_actions[SELECT].hasOwnProperty('distinct')) {
            js_meta_params['__RBQLMP__writer_type'] = 'uniq';
        } else {
            js_meta_params['__RBQLMP__writer_type'] = 'simple';
        }
        if (rb_actions.hasOwnProperty(EXCEPT)) {
            js_meta_params['__RBQLMP__select_expression'] = translate_except_expression(rb_actions[EXCEPT]['text']);
        } else {
            let select_expression = translate_select_expression_js(rb_actions[SELECT]['text']);
            js_meta_params['__RBQLMP__select_expression'] = combine_string_literals(select_expression, string_literals);
        }
        js_meta_params['__RBQLMP__update_statements'] = '';
        js_meta_params['__RBQLMP__is_select_query'] = 'true';
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


//def generic_run(query, input_iterator, output_writer, join_tables_registry=None, user_init_code='', convert_only_dst=None):
//    # Join registry can cotain info about any number of tables (e.g. about one table "B" only)
//    try:
//        user_init_code = indent_user_init_code(user_init_code)
//        rbql_home_dir = os.path.dirname(os.path.abspath(__file__))
//        with codecs.open(os.path.join(rbql_home_dir, 'template.py'), encoding='utf-8') as py_src:
//            py_template_text = py_src.read()
//        python_code, join_map = parse_to_py(query, py_template_text, join_tables_registry, user_init_code)
//        if convert_only_dst is not None:
//            write_python_module(python_code, convert_only_dst)
//            return (None, [])
//        with RbqlPyEnv() as worker_env:
//            write_python_module(python_code, worker_env.module_path)
//            # TODO find a way to report module_path if exception is thrown.
//            # One way is just to always create a symlink like "rbql_module_debug" inside tmp_dir.
//            # It would point to the last module if lauch failed, or just a dangling ref.
//            # Generated modules are not re-runnable by themselves now anyway.
//            rbconvert = worker_env.import_worker()
//            success = rbconvert.rb_transform(input_iterator, join_map, output_writer)
//            assert success, 'Unexpected error during RBQL query execution'
//            input_warnings = input_iterator.get_warnings()
//            join_warnings = join_map.get_warnings() if join_map is not None else []
//            output_warnings = output_writer.get_warnings()
//            warnings = input_warnings + join_warnings + output_warnings
//            worker_env.remove_env_dir()
//            return (None, warnings)
//    except Exception as e:
//        error_info = exception_to_error_info(e)
//        return (error_info, [])
//    finally:
//        input_iterator.finish()
//        output_writer.finish()


function load_module_from_string(module_name, node_module_string) {
    var module = {'exports': {}};
    eval('(function(){' + node_module_string + '})()');
    eval(`${module_name} = module.exports;`);
}


function generic_run(query, input_iterator, output_writer, external_success_cb, external_error_handler, join_tables_registry=null, user_init_code='') {
    try {
        user_init_code = indent_user_init_code(user_init_code);
        let [js_code, join_map] = parse_to_js(query, external_js_template_text, join_tables_registry, user_init_code);
        load_module_from_string('rbql_worker', js_code);
        rbql_worker.rb_transform(input_iterator, join_map, output_writer, external_success_cb, external_error_handler);
    } catch (e) {
        if (e instanceof RbqlParsingError) {
            external_error_handler('query parsing', e.error_msg);
        } else {
            external_error_handler('unexpected', 'Unexpected exception: ' + e);
        }
    }
}





//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
// OLD CODE IS BELOW
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////


const os = require('os');
const path = require('path');
const fs = require('fs');

const version = '0.5.0';

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

const rbql_home_dir = __dirname;
const user_home_dir = os.homedir();
const table_names_settings_path = path.join(user_home_dir, '.rbql_table_names');
const table_index_path = path.join(user_home_dir, '.rbql_table_index');
const default_init_source_path = path.join(user_home_dir, '.rbql_init_source.js');

const default_csv_encoding = 'latin-1';


// FIXME separate CSV-related logic from core RBQL model, the same way as it is already done with Python implementation


class RbqlParsingError extends Error {}


class AssertionError extends Error {}


function expanduser(filepath) {
    if (filepath.charAt(0) === '~') {
        return path.join(process.env.HOME, filepath.slice(1));
    }
    return filepath;
}


function try_read_index(index_path) {
    var content = null;
    try {
        content = fs.readFileSync(index_path, 'utf-8');
    } catch (e) {
        return [];
    }
    var lines = content.split('\n');
    var records = [];
    for (var i = 0; i < lines.length; i++) {
        if (!lines[i])
            continue;
        var record = lines[i].split('\t');
        records.push(record);
    }
    return records;
}


function get_index_record(index_path, key) {
    var records = try_read_index(index_path);
    for (var i = 0; i < records.length; i++) {
        if (records[i].length && records[i][0] == key) {
            return records[i];
        }
    }
    return null;
}


function find_table_path(table_id) {
    var candidate_path = expanduser(table_id);
    if (fs.existsSync(candidate_path)) {
        return candidate_path;
    }
    var name_record = get_index_record(table_names_settings_path, table_id);
    if (name_record && name_record.length > 1 && fs.existsSync(name_record[1])) {
        return name_record[1];
    }
    return null;
}


function normalize_delim(delim) {
    if (delim == 'TAB')
        return '\t';
    return delim;
}


function is_ascii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}


function parse_to_js_almost_web(src_table_path, dst_table_path, query, js_template_text, input_delim, input_policy, out_delim, out_policy, csv_encoding, custom_init_path=null) {
    if (input_delim == '"' && input_policy == 'quoted')
        throw new RbqlParsingError('Double quote delimiter is incompatible with "quoted" policy');
    if (csv_encoding == 'latin-1')
        csv_encoding = 'binary';
    if (!is_ascii(query) && csv_encoding == 'binary') {
        throw new RbqlParsingError('To use non-ascii characters in query enable UTF-8 encoding instead of latin-1/binary');
    }
    let rbql_lines = query.split('\n');
    rbql_lines = rbql_lines.map(strip_comments);
    rbql_lines = rbql_lines.filter(line => line.length);
    var full_rbql_expression = rbql_lines.join(' ');
    var column_vars = extract_column_vars(full_rbql_expression);
    var [format_expression, string_literals] = separate_string_literals_js(full_rbql_expression);
    var rb_actions = separate_actions(format_expression);

    let user_init_code = '';
    if (custom_init_path !== null) {
        user_init_code = make_user_init_code(custom_init_path);
    } else if (fs.existsSync(default_init_source_path)) {
        user_init_code = make_user_init_code(default_init_source_path);
    }

    var js_meta_params = {};
    js_meta_params['__RBQLMP__user_init_code'] = user_init_code;
    js_meta_params['__RBQLMP__rbql_home_dir'] = escape_string_literal(rbql_home_dir);
    js_meta_params['__RBQLMP__input_delim'] = escape_string_literal(input_delim);
    js_meta_params['__RBQLMP__input_policy'] = input_policy;
    js_meta_params['__RBQLMP__csv_encoding'] = csv_encoding;
    js_meta_params['__RBQLMP__src_table_path'] = src_table_path === null ? "null" : "'" + escape_string_literal(src_table_path) + "'";
    js_meta_params['__RBQLMP__dst_table_path'] = dst_table_path === null ? "null" : "'" + escape_string_literal(dst_table_path) + "'";
    js_meta_params['__RBQLMP__output_delim'] = escape_string_literal(out_delim);
    js_meta_params['__RBQLMP__output_policy'] = out_policy;

    if (rb_actions.hasOwnProperty(GROUP_BY)) {
        if (rb_actions.hasOwnProperty(ORDER_BY) || rb_actions.hasOwnProperty(UPDATE))
            throw new RbqlParsingError('"ORDER BY" and "UPDATE" are not allowed in aggregate queries');
        var aggregation_key_expression = rb_actions[GROUP_BY]['text'];
        js_meta_params['__RBQLMP__aggregation_key_expression'] = '[' + combine_string_literals(aggregation_key_expression, string_literals) + ']';
    } else {
        js_meta_params['__RBQLMP__aggregation_key_expression'] = 'null';
    }

    if (rb_actions.hasOwnProperty(JOIN)) {
        var [rhs_table_id, lhs_join_var, rhs_join_var] = parse_join_expression(rb_actions[JOIN]['text']);
        var rhs_table_path = find_table_path(rhs_table_id);
        if (!rhs_table_path) {
            throw new RbqlParsingError(`Unable to find join B table: ${rhs_table_id}`);
        }
        var [join_delim, join_policy] = [input_delim, input_policy];
        var join_format_record = get_index_record(table_index_path, rhs_table_path)
        if (join_format_record && join_format_record.length >= 3) {
            join_delim = normalize_delim(join_format_record[1]);
            join_policy = join_format_record[2];
        }
        js_meta_params['__RBQLMP__join_operation'] = rb_actions[JOIN]['join_subtype'];
        js_meta_params['__RBQLMP__rhs_table_path'] = "'" + escape_string_literal(rhs_table_path) + "'";
        js_meta_params['__RBQLMP__lhs_join_var'] = lhs_join_var;
        js_meta_params['__RBQLMP__rhs_join_var'] = rhs_join_var;
        js_meta_params['__RBQLMP__join_delim'] = escape_string_literal(join_delim);
        js_meta_params['__RBQLMP__join_policy'] = join_policy;
    } else {
        js_meta_params['__RBQLMP__join_operation'] = 'VOID';
        js_meta_params['__RBQLMP__rhs_table_path'] = 'null';
        js_meta_params['__RBQLMP__lhs_join_var'] = 'null';
        js_meta_params['__RBQLMP__rhs_join_var'] = 'null';
        js_meta_params['__RBQLMP__join_delim'] = '';
        js_meta_params['__RBQLMP__join_policy'] = '';
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
        var update_expression = translate_update_expression(rb_actions[UPDATE]['text'], ' '.repeat(8));
        js_meta_params['__RBQLMP__writer_type'] = 'simple';
        js_meta_params['__RBQLMP__select_expression'] = 'null';
        js_meta_params['__RBQLMP__update_statements'] = combine_string_literals(update_expression, string_literals);
        js_meta_params['__RBQLMP__is_select_query'] = 'false';
        js_meta_params['__RBQLMP__top_count'] = 'null';
    }

    js_meta_params['__RBQLMP__init_column_vars_update'] = generate_init_statements(column_vars, ' '.repeat(4));
    js_meta_params['__RBQLMP__init_column_vars_select'] = generate_init_statements(column_vars, ' '.repeat(8));

    if (rb_actions.hasOwnProperty(SELECT)) {
        var top_count = find_top(rb_actions);
        js_meta_params['__RBQLMP__top_count'] = top_count === null ? 'null' : String(top_count);
        if (rb_actions[SELECT].hasOwnProperty('distinct_count')) {
            js_meta_params['__RBQLMP__writer_type'] = 'uniq_count';
        } else if (rb_actions[SELECT].hasOwnProperty('distinct')) {
            js_meta_params['__RBQLMP__writer_type'] = 'uniq';
        } else {
            js_meta_params['__RBQLMP__writer_type'] = 'simple';
        }
        if (rb_actions.hasOwnProperty(EXCEPT)) {
            js_meta_params['__RBQLMP__select_expression'] = translate_except_expression(rb_actions[EXCEPT]['text']);
        } else {
            let select_expression = translate_select_expression_js(rb_actions[SELECT]['text']);
            js_meta_params['__RBQLMP__select_expression'] = combine_string_literals(select_expression, string_literals);
        }
        js_meta_params['__RBQLMP__update_statements'] = '';
        js_meta_params['__RBQLMP__is_select_query'] = 'true';
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
    var result_script = rbql_meta_format(js_template_text, js_meta_params);
    return result_script;
}


function parse_to_js(src_table_path, dst_table_path, query, js_dst, input_delim, input_policy, out_delim, out_policy, csv_encoding, custom_init_path=null) {
    var js_template_text = fs.readFileSync(path.join(rbql_home_dir, 'template.js'), 'utf-8');
    var result_script = parse_to_js_almost_web(src_table_path, dst_table_path, query, js_template_text, input_delim, input_policy, out_delim, out_policy, csv_encoding, custom_init_path);
    fs.writeFileSync(js_dst, result_script);
}



function make_inconsistent_num_fields_hr_warning(table_name, inconsistent_lines_info) {
    let keys = Object.keys(inconsistent_lines_info);
    let entries = [];
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        let line_id = inconsistent_lines_info[key];
        entries.push([line_id, key]);
    }
    entries.sort(function(a, b) { return a[0] - b[0]; });
    assert(entries.length > 1);
    let [lnum_1, num_fields_1] = entries[0];
    let [lnum_2, num_fields_2] = entries[1];
    let warn_msg = `Number of fields in ${table_name} table is not consistent. `;
    warn_msg += `E.g. there are ${num_fields_1} fields at line ${lnum_1}, and ${num_fields_2} fields at line ${lnum_2}.`;
    return warn_msg;
}


function make_warnings_human_readable(warnings) {
    let result = [];
    let keys = Object.keys(warnings);
    for (let i = 0; i < keys.length; i++) {
        let warning_type = keys[i];
        let warning_value = warnings[warning_type];
        if (warning_type == 'null_value_in_output') {
            result.push('undefined/null values in output were replaced by empty strings.');
        } else if (warning_type == 'delim_in_simple_output') {
            result.push('Some result set fields contain output separator.');
        } else if (warning_type == 'output_switch_to_csv') {
            // ATTENTION: External tools depend on the exact wording of the following message:
            result.push('Output has multiple fields: using "CSV" output format instead of "Monocolumn"');
        } else if (warning_type == 'utf8_bom_removed') {
            result.push('UTF-8 Byte Order Mark BOM was found and removed.');
        } else if (warning_type == 'defective_csv_line_in_input') {
            result.push(`Defective double quote escaping in input table. E.g. at line ${warning_value}.`);
        } else if (warning_type == 'defective_csv_line_in_join') {
            result.push(`Defective double quote escaping in join table. E.g. at line ${warning_value}.`);
        } else if (warning_type == 'input_fields_info') {
            result.push(make_inconsistent_num_fields_hr_warning('input', warning_value));
        } else if (warning_type == 'join_fields_info') {
            result.push(make_inconsistent_num_fields_hr_warning('join', warning_value));
        } else {
            throw new Error(`Unknown warning type: ${warning_type}`);
        }
    }
    for (let i = 0; i < result.length; i++) {
        assert(result[i].indexOf('\n') == -1);
    }
    return result;
}


module.exports.version = version;
module.exports.assert = assert;
module.exports.default_csv_encoding = default_csv_encoding;
module.exports.parse_to_js = parse_to_js;
module.exports.parse_to_js_almost_web = parse_to_js_almost_web;
module.exports.make_warnings_human_readable = make_warnings_human_readable;
module.exports.strip_comments = strip_comments;
module.exports.separate_string_literals_js = separate_string_literals_js;
module.exports.translate_select_expression_js = translate_select_expression_js;
module.exports.translate_except_expression = translate_except_expression;

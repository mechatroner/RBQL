#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');

var rbql = null;
var rbql_csv = null;
const csv_utils = require('./csv_utils.js');
const cli_parser = require('./cli_parser.js');


// TODO implement query history like in Python version. "readline" modules allows to do that, see "completer" parameter.


function die(error_msg) {
    console.error('Error: ' + error_msg);
    process.exit(1);
}

let out_format_names = ['csv', 'tsv', 'monocolumn', 'input'];

var tmp_worker_module_path = null;
var error_format = 'hr';
var interactive_mode = false;
var user_input_reader = null;
var args = null;


function show_error(msg) {
    if (interactive_mode) {
        console.log('\x1b[31;1mError:\x1b[0m ' + msg);
    } else {
        console.error('Error: ' + msg);
    }
    if (fs.existsSync(tmp_worker_module_path)) {
        let output_func = interactive_mode ? console.log : console.error;
        output_func('Generated module was saved here: ' + tmp_worker_module_path);
    }
}


function show_warning(msg) {
    if (interactive_mode) {
        console.log('\x1b[33;1mWarning:\x1b[0m ' + msg);
    } else {
        console.error('Warning: ' + msg);
    }
}


function normalize_delim(delim) {
    if (delim == 'TAB')
        return '\t';
    if (delim == '\\t')
        return '\t';
    return delim;
}


function get_default(src, key, default_val) {
    return src.hasOwnProperty(key) ? src[key] : default_val;
}


function cleanup_tmp() {
    if (fs.existsSync(tmp_worker_module_path)) {
        fs.unlinkSync(tmp_worker_module_path);
    }
}


function report_warnings_json(warnings) {
    if (warnings !== null) {
        var warnings_report = JSON.stringify({'warnings': warnings});
        process.stderr.write(warnings_report);
    }
}


function report_error_json(error_type, error_msg) {
    let report = new Object();
    report.error_type = error_type;
    report.error = error_msg;
    process.stderr.write(JSON.stringify(report));
    if (fs.existsSync(tmp_worker_module_path)) {
        console.log('\nGenerated module was saved here: ' + tmp_worker_module_path);
    }
}


function finish_query_with_error(error_type, error_msg) {
    if (error_format == 'hr') {
        show_error(error_type + ': ' + error_msg);
    } else {
        report_error_json(error_type, error_msg);
    }
    if (!interactive_mode) {
        process.exit(1);
    } else {
        show_query_prompt();
    }
}


function get_default_policy(delim) {
    if ([';', ','].indexOf(delim) != -1) {
        return 'quoted';
    } else if (delim == ' ') {
        return 'whitespace';
    } else {
        return 'simple';
    }
}


function is_delimited_table(sampled_lines, delim, policy) {
    if (sampled_lines.length < 10)
        return false;
    let num_fields = null;
    for (var i = 0; i < sampled_lines.length; i++) {
        let [fields, warning] = csv_utils.smart_split(sampled_lines[i], delim, policy, true);
        if (warning)
            return false;
        if (num_fields === null)
            num_fields = fields.length;
        if (num_fields < 2 || num_fields != fields.length)
            return false;
    }
    return true;
}


function sample_lines(table_path, callback_func) {
    let input_reader = readline.createInterface({ input: fs.createReadStream(table_path) });
    let sampled_lines = [];
    input_reader.on('line', line => {
        sampled_lines.push(line);
        if (sampled_lines.length >= 10)
            input_reader.close();
    });
    input_reader.on('close', () => { callback_func(sampled_lines); });
}


function sample_records(table_path, delim, policy, callback_func) {
    // TODO rewrite with record iterator to support newlines in fields
    sample_lines(table_path, (sampled_lines) => {
        let records = [];
        let bad_lines = [];
        for (var i = 0; i < sampled_lines.length; i++) {
            let [fields, warning] = csv_utils.smart_split(sampled_lines[i], delim, policy, true);
            if (warning)
                bad_lines.push(i + 1);
            records.push(fields);
        }
        callback_func(records, bad_lines);
    });
}


function autodetect_delim_policy(table_path, sampled_lines) {
    let autodetection_dialects = [['\t', 'simple'], [',', 'quoted'], [';', 'quoted']];
    for (var i = 0; i < autodetection_dialects.length; i++) {
        let [delim, policy] = autodetection_dialects[i];
        if (is_delimited_table(sampled_lines, delim, policy))
            return [delim, policy];
    }
    if (table_path.endsWith('.csv'))
        return [',', 'quoted'];
    if (table_path.endsWith('.tsv'))
        return ['\t', 'simple'];
    return [null, null];
}


function print_colorized(records, delim, show_column_names) {
    let reset_color_code = '\x1b[0m';
    let color_codes = ['\x1b[0m', '\x1b[31m', '\x1b[32m', '\x1b[33m', '\x1b[34m', '\x1b[35m', '\x1b[36m', '\x1b[31;1m', '\x1b[32;1m', '\x1b[33;1m'];
    for (let r = 0; r < records.length; r++) {
        let out_fields = [];
        for (let c = 0; c < records[r].length; c++) {
            let color_code = color_codes[c % color_codes.length];
            let field = records[r][c];
            let colored_field = show_column_names ? `${color_code}a${c + 1}:${field}` : color_code + field;
            out_fields.push(colored_field);
        }
        let out_line = out_fields.join(delim) + reset_color_code;
        console.log(out_line);
    }
}


function handle_query_success(warnings, output_path, delim, policy) {
    cleanup_tmp();
    if (error_format == 'hr') {
        if (warnings !== null) {
            for (let i = 0; i < warnings.length; i++) {
                show_warning(warnings[i]);
            }
        }
        if (interactive_mode) {
            user_input_reader.close();
            sample_records(output_path, delim, policy, (records, _bad_lines) => {
                console.log('\nOutput table preview:');
                console.log('====================================');
                print_colorized(records, delim, false);
                console.log('====================================');
                console.log('Success! Result table was saved to: ' + output_path);
            });
        }
    } else {
        report_warnings_json(warnings);
    }
}


function run_with_js() {
    var delim = normalize_delim(args['delim']);
    var policy = args['policy'] ? args['policy'] : get_default_policy(delim);
    var query = args['query'];
    if (!query) {
        finish_query_with_error('Parsing Error', 'RBQL query is empty');
        return;
    }
    var input_path = get_default(args, 'input', null);
    var output_path = get_default(args, 'output', null);
    var csv_encoding = args['encoding'];
    var output_delim = get_default(args, 'out-delim', null);
    var output_policy = get_default(args, 'out-policy', null);
    let init_source_file = get_default(args, 'init-source-file', null);
    let output_format = args['out-format'];
    if (output_delim === null) {
        [output_delim, output_policy] = output_format == 'input' ? [delim, policy] : rbql_csv.interpret_named_csv_format(output_format);
    }

    let handle_success = function(warnings) {
        handle_query_success(warnings, output_path, delim, policy);
    };

    if (args['debug-mode'])
        rbql_csv.set_debug_mode();
    let user_init_code = '';
    if (init_source_file !== null)
        user_init_code = rbql_csv.read_user_init_code(init_source_file);
    rbql_csv.csv_run(query, input_path, delim, policy, output_path, output_delim, output_policy, csv_encoding, handle_success, finish_query_with_error, user_init_code);
}


function get_default_output_path(input_path, delim) {
    let well_known_extensions = {',': '.csv', '\t': '.tsv'};
    if (well_known_extensions.hasOwnProperty(delim))
        return input_path + well_known_extensions[delim];
    return input_path + '.txt';
}


function show_query_prompt() {
    user_input_reader.question('Input SQL-like RBQL query and press Enter:\n> ', (query) => {
        args.query = query.trim();
        run_with_js();
    });
}


function show_preview(input_path, delim, policy) {
    if (!delim) {
        die('Unable to autodetect table delimiter. Provide column separator explicitly with "--delim" option');
    }
    args.delim = delim;
    args.policy = policy;
    sample_records(input_path, delim, policy, (records, bad_lines) => {
        console.log('Input table preview:');
        console.log('====================================');
        print_colorized(records, delim, true);
        console.log('====================================\n');
        if (bad_lines.length)
            show_warning('Some input lines have quoting errors. Line numbers: ' + bad_lines.join(','));
        if (!args.output) {
            args.output = get_default_output_path(input_path, delim);
            show_warning('Output path was not provided. Result set will be saved as: ' + args.output);
        }
        user_input_reader = readline.createInterface({ input: process.stdin, output: process.stdout });
        show_query_prompt();
    });
}


function start_preview_mode(args) {
    let input_path = get_default(args, 'input', null);
    if (!input_path) {
        show_error('Input file must be provided in interactive mode. You can use stdin input only in non-interactive mode');
        process.exit(1);
    }
    if (error_format != 'hr') {
        show_error('Only default "hr" error format is supported in interactive mode');
        process.exit(1);
    }
    let delim = get_default(args, 'delim', null);
    let policy = null;
    if (delim !== null) {
        delim = normalize_delim(delim);
        policy = args['policy'] ? args['policy'] : get_default_policy(delim);
        show_preview(input_path, delim, policy);
    } else {
        sample_lines(input_path, (sampled_lines) => {
            let [delim, policy] = autodetect_delim_policy(input_path, sampled_lines);
            show_preview(input_path, delim, policy);
        });
    }
}


function main() {
    var scheme = {
        '--query': {'help': 'Query string in rbql. Run in interactive mode if empty', 'metavar': 'QUERY'},
        '--input': {'help': 'Read csv table from FILE instead of stdin', 'metavar': 'FILE'},
        '--output': {'help': 'Write output table to FILE instead of stdout', 'metavar': 'FILE'},
        '--delim': {'help': 'Delimiter character or multicharacter string, e.g. "," or "###"', 'metavar': 'DELIM'},
        '--policy': {'help': 'Split policy. Supported values: "simple", "quoted", "quoted_rfc", "whitespace", "monocolumn"', 'metavar': 'POLICY'},
        '--encoding': {'default': 'latin-1', 'help': 'Manually set csv table encoding', 'metavar': 'ENCODING'},
        '--out-format': {'default': 'input', 'help': 'Output format. Supported values: ' + out_format_names.map(v => `"${v}"`).join(', '), 'metavar': 'FORMAT'},
        '--out-delim': {'help': 'Output delim. Use with "out-policy". Overrides out-format', 'metavar': 'DELIM'},
        '--out-policy': {'help': 'Output policy. Use with "out-delim". Overrides out-format', 'metavar': 'POLICY'},
        '--error-format': {'default': 'hr', 'help': 'Errors and warnings format. [hr|json]', 'hidden': true},
        '--version': {'boolean': true, 'help': 'Print RBQL version and exit'},
        '--auto-rebuild-engine': {'boolean': true, 'help': 'Auto rebuild engine', 'hidden': true},
        '--debug-mode': {'boolean': true, 'help': 'Run in debug mode', 'hidden': true},
        '--init-source-file': {'help': 'Path to init source file to use instead of ~/.rbql_init_source.js', 'hidden': true}
    };
    args = cli_parser.parse_cmd_args(process.argv, scheme);

    if (args['auto-rebuild-engine']) {
        let build_engine = require('./build_engine.js');
        build_engine.build_engine();
    }

    rbql = require('./rbql.js');
    rbql_csv = require('./rbql_csv.js');

    if (args['version']) {
        console.log(rbql.version);
        process.exit(0);
    }

    if (args.encoding == 'latin-1')
        args.encoding = 'binary';

    error_format = args['error-format'];

    if (args.hasOwnProperty('query')) {
        interactive_mode = false;
        if (!args.delim) {
            die('Separator must be provided with "--delim" option in non-interactive mode');
        }
        run_with_js();
    } else {
        interactive_mode = true;
        start_preview_mode(args);
    }
}


if (require.main === module) {
    main();
}



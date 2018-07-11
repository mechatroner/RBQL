const os = require('os');
const path = require('path');
const fs = require('fs');

const rbql = require('./rbql.js');


function die(error_msg) {
    console.error('Error: ' + error_msg);
    process.exit(1);
}


var tmp_worker_module_path = null;


function show_help(scheme) {
    console.log('Options:\n');
    for (var k in scheme) {
        console.log(k);
        if (scheme[k].hasOwnProperty('default')) {
            console.log('    Default: "' + scheme[k]['default'] + '"');
        }
        console.log('    ' + scheme[k]['help']);
        console.log();
    }
}


function normalize_cli_key(cli_key) {
    return cli_key.replace(/^-*/, '');
}


function parse_cmd_args(cmd_args, scheme) {
    var result = {};
    if (cmd_args.length <= 2 || !cmd_args[0].endsWith('node')) {
        die('script must be envoked like this: "/path/to/node cli_rbql.js arg1 arg2 ..."');
    }
    for (var arg_key in scheme) {
        var arg_info = scheme[arg_key];
        if (arg_info.hasOwnProperty('default'))
            result[normalize_cli_key(arg_key)] = arg_info['default'];
    }
    cmd_args = cmd_args.slice(2);
    var i = 0;
    while(i < cmd_args.length) {
        var arg_key = cmd_args[i];
        if (arg_key == '--help') {
            show_help(scheme);
            process.exit(0);
        }
        i += 1;
        if (!scheme.hasOwnProperty(arg_key)) {
            die(`unknown argument: ${arg_key}`);
        }
        var arg_info = scheme[arg_key];
        var normalized_key = normalize_cli_key(arg_key);
        if (arg_info['boolean']) {
            result[normalized_key] = true;
            continue;    
        }
        if (i >= cmd_args.length) {
            die(`no CLI value for key: ${arg_key}`);
        }
        var arg_value = cmd_args[i];
        i += 1;
        result[normalized_key] = arg_value;
    }
    return result;
}


function normalize_delim(delim) {
    if (delim == 'TAB')
        return '\t';
    if (delim == '\\t')
        return '\t';
    return delim;
}


function interpret_format(format_name) {
    rbql.assert(['csv', 'tsv', 'monocolumn'].indexOf(format_name) != -1, 'unknown format');
    if (format_name == 'monocolumn')
        return ['', 'monocolumn'];
    if (format_name == 'csv')
        return [',', 'quoted'];
    return ['\t', 'simple'];
}


function get_default(src, key, default_val) {
    return src.hasOwnProperty(key) ? src[key] : default_val;
}


function cleanup_tmp() {
    if (fs.existsSync(tmp_worker_module_path)) {
        fs.unlinkSync(tmp_worker_module_path);
    }
}


function cli_success_cb(warnings) {
    if (warnings !== null) {
        var warnings_report = JSON.stringify({'warnings': warnings});
        process.stderr.write(warnings_report);
    }
}


function handle_worker_success(warnings) {
    cleanup_tmp();
    cli_success_cb(warnings);
}


function handle_worker_failure(error_msg) {
    //cleanup_tmp();
    var report = new Object();
    report.error = error_msg
    process.stderr.write(JSON.stringify(report));
    if (fs.existsSync(tmp_worker_module_path)) {
        console.log('\nGenerated module was saved here: ' + tmp_worker_module_path);
    }
    process.exit(1);
}


function run_with_js(args) {
    var delim = normalize_delim(args['delim']);
    var policy = args['policy'];
    if (!policy) {
        policy = [';', ','].indexOf(delim) == -1 ? 'simple' : 'quoted';
    }
    var query = args['query'];
    if (!query) {
        die('RBQL query is empty');
    }
    var input_path = get_default(args, 'input_table_path', null);
    var output_path = get_default(args, 'output_table_path', null);
    var csv_encoding = args['csv_encoding'];
    var output_delim = get_default(args, 'out_delim', null);
    var output_policy = get_default(args, 'out_policy', null);
    if (output_delim === null) {
        [output_delim, output_policy] = interpret_format(args['out_format']);
    }
    var rbql_lines = [query];
    var tmp_dir = os.tmpdir();
    var script_filename = 'rbconvert_' + String(Math.random()).replace('.', '_') + '.js';
    tmp_worker_module_path = path.join(tmp_dir, script_filename);
    rbql.parse_to_js(input_path, output_path, rbql_lines, tmp_worker_module_path, delim, policy, output_delim, output_policy, csv_encoding);
    if (args.hasOwnProperty('parse_only')) {
        console.log('Worker module location: ' + tmp_worker_module_path);
        return;
    }
    var worker_module = require(tmp_worker_module_path);
    worker_module.run_on_node(handle_worker_success, handle_worker_failure);
}


function main() {
    var scheme = {
        '--delim': {'default': 'TAB', 'help': 'Delimiter'},
        '--policy': {'help': 'Split policy'},
        '--out_format': {'default': 'tsv', 'help': 'Output format'},
        '--out_delim': {'help': 'Output delim. Use with "out_policy". Overrides out_format'},
        '--out_policy': {'help': 'Output policy. Use with "out_delim". Overrides out_format'},
        '--query': {'help': 'Query string in rbql'},
        '--input_table_path': {'help': 'Read csv table from FILE instead of stdin'},
        '--output_table_path': {'help': 'Write output table to FILE instead of stdout'},
        '--csv_encoding': {'default': rbql.default_csv_encoding, 'help': 'Manually set csv table encoding'},
        '--parse_only': {'boolean': true, 'help': 'Create worker module and exit'},
        '--version': {'boolean': true, 'help': 'Script language to use in query'}
    };
    var args = parse_cmd_args(process.argv, scheme);

    if (args.hasOwnProperty('version')) {
        console.log(rbql.version);
        process.exit(0);
    }

    run_with_js(args);
}

main();

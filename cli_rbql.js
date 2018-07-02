const os = require('os');
const path = require('path');
const fs = require('fs');

const rbql = require('./rbql.js');


function die(error_msg) {
    console.error(error_msg);
    process.exit(1);
}


function show_help(scheme) {
    console.log('Options:\n');
    for (var k in scheme) {
        console.log(k);
        console.log('    ' + scheme[k]['help'] + '\n');
    }
}


function parse_cmd_args(cmd_args, scheme) {
    var result = {};
    if (cmd_args.length <= 2 || !cmd_args[0].endsWith('node')) {
        die('Error: the script must be envoked like this: "/path/to/node cli_rbql.js arg1 arg2 ..."');
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
            die(`Error: unknown argument: ${arg_key}`);
        }
        arg_info = scheme[arg_key];
        if (arg_info['boolean']) {
            result[arg_key] = true;
            continue;    
        }
        if (i >= cmd_args.length) {
            die(`Error: no CLI value for key: ${arg_key}`);
        }
        var arg_value = cmd_args[i];
        i += 1;
        result[arg_key] = arg_value;
    }
    return result;
}


function main() {
    var scheme = {
        '--delim': {'help': 'Delimiter'},
        '--policy': {'help': 'Split policy'},
        '--out_format': {'help': 'Output format'},
    };
    var config = parse_cmd_args(process.argv, scheme);
    console.log(config);
}

main();

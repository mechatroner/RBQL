function die(error_msg) {
    console.error('Error: ' + error_msg);
    process.exit(1);
}


function normalize_cli_key(cli_key) {
    return cli_key.replace(/^-*/, '');
}


function show_help(scheme) {
    console.log('Options:\n');
    for (var k in scheme) {
        if (scheme[k].hasOwnProperty('hidden')) {
            continue;
        }
        console.log(k);
        if (scheme[k].hasOwnProperty('default')) {
            console.log('    Default: "' + scheme[k]['default'] + '"');
        }
        console.log('    ' + scheme[k]['help']);
        console.log();
    }
}


function parse_cmd_args(cmd_args, scheme) {
    var result = {};
    for (var arg_key in scheme) {
        var arg_info = scheme[arg_key];
        if (arg_info.hasOwnProperty('default'))
            result[normalize_cli_key(arg_key)] = arg_info['default'];
        if (arg_info.hasOwnProperty('boolean'))
            result[normalize_cli_key(arg_key)] = false;
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



module.exports.parse_cmd_args = parse_cmd_args;

const fs = require('fs');


function is_ascii(str) {
    return /^[\x00-\x7F]*$/.test(str);
}


function read_user_init_code(rbql_init_source_path) {
    return fs.readFileSync(rbql_init_source_path, 'utf-8');
}


module.exports.is_ascii = is_ascii;
module.exports.read_user_init_code = read_user_init_code;

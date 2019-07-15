const rbql = require('rbql');

let user_query = 'SELECT a1, parseInt(a2) % 1000 WHERE a3 != "USA" LIMIT 5';
let error_handler = function(error_type, error_msg) {
    console.log('Error: ' + error_type + ': ' + error_msg);
}
let success_handler = function(warnings) {
    if (warnings.length)
        console.log('warnings: ' + JSON.stringify(warnings));
    console.log('output table: output.csv');
}
rbql.csv_run(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8', success_handler, error_handler);

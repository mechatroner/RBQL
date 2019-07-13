const rbql = require('rbql')

let input_table = [
    ['Roosevelt',1858,'USA'],
    ['Napoleon',1769,'France'],
    ['Dmitri Mendeleev',1834,'Russia'],
    ['Jane Austen',1775,'England'],
    ['Hayao Miyazaki',1941,'Japan'],
];
let user_query = 'SELECT a1, a2 % 1000 WHERE a3 != "USA" LIMIT 3';
let output_table = [];
let error_handler = function(error_type, error_msg) {
    console.log('Error: ' + error_type + ': ' + error_msg);
}
let success_handler = function(warnings) {
    console.log('warnings: ' + JSON.stringify(warnings));
    console.log('output table: ' + JSON.stringify(output_table));
}
rbql.table_run(user_query, input_table, output_table, success_handler, error_handler);

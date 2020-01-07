const rbql_csv = require('rbql_csv');
let user_query = 'SELECT a1, parseInt(a2) % 1000 WHERE a3 != "USA" LIMIT 5';
let error_handler = function(exception) {
    console.log('Error: ' + String(exception));
}
let warnings = [];
let success_handler = function() {
    if (warnings.length)
        console.log('warnings: ' + JSON.stringify(warnings));
    console.log('output table: output.csv');
}
rbql_csv.query_csv(user_query, 'input.csv', ',', 'quoted', 'output.csv', ',', 'quoted', 'utf-8', warnings).then(success_handler).catch(error_handler);

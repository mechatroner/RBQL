const rbql = require('./rbql.js');
const rbql_csv = require('./rbql_csv.js');

// Main API
module.exports.version = rbql.version;

module.exports.query = rbql.query;
module.exports.query_table = rbql.query_table;
module.exports.query_csv = rbql_csv.query_csv;


// Misc API
module.exports.exception_to_error_info = rbql.exception_to_error_info;

module.exports.TableIterator = rbql.TableIterator;
module.exports.TableWriter = rbql.TableWriter;
module.exports.SingleTableRegistry = rbql.SingleTableRegistry;

module.exports.CSVRecordIterator = rbql_csv.CSVRecordIterator;
module.exports.CSVWriter = rbql_csv.CSVWriter;
module.exports.FileSystemCSVRegistry = rbql_csv.FileSystemCSVRegistry;


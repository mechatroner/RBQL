const os = require('os');
const fs = require('fs');
const readline = require('readline');
const path = require('path');


class RbqlIOHandlingError extends Error {}
class AssertionError extends Error {}


let field_regular_expression = '"((?:[^"]*"")*[^"]*)"';
let field_rgx = new RegExp('^' + field_regular_expression);
let field_rgx_external_whitespaces = new RegExp('^' + ' *'+ field_regular_expression + ' *');


function assert(condition, message=null) {
    if (!condition) {
        if (!message) {
            message = 'Assertion error';
        }
        throw new AssertionError(message);
    }
}


function interpret_named_csv_format(format_name) {
    format_name = format_name.toLowerCase();
    if (format_name == 'monocolumn')
        return ['', 'monocolumn'];
    if (format_name == 'csv')
        return [',', 'quoted'];
    if (format_name == 'tsv')
        return ['\t', 'simple'];
    throw new RbqlIOHandlingError(`Unknown format name: "${format_name}"`);
}


function extract_next_field(src, dlm, preserve_quotes_and_whitespaces, allow_external_whitespaces, cidx, result) {
    var warning = false;
    let src_cur = src.substring(cidx);
    let rgx = allow_external_whitespaces ? field_rgx_external_whitespaces : field_rgx;
    let match_obj = rgx.exec(src_cur);
    if (match_obj !== null) {
        let match_end = match_obj[0].length;
        if (cidx + match_end == src.length || src[cidx + match_end] == dlm) {
            if (preserve_quotes_and_whitespaces) {
                result.push(match_obj[0]);
            } else {
                result.push(match_obj[1].replace(/""/g, '"'));
            }
            return [cidx + match_end + 1, false];
        }
        warning = true;
    }
    var uidx = src.indexOf(dlm, cidx);
    if (uidx == -1)
        uidx = src.length;
    var field = src.substring(cidx, uidx);
    warning = warning || field.indexOf('"') != -1;
    result.push(field);
    return [uidx + 1, warning];
}


function split_quoted_str(src, dlm, preserve_quotes_and_whitespaces=false) {
    if (src.indexOf('"') == -1) // Optimization for most common case
        return [src.split(dlm), false];
    var result = [];
    var cidx = 0;
    var warning = false;
    let allow_external_whitespaces = dlm != ' ';
    while (cidx < src.length) {
        var extraction_report = extract_next_field(src, dlm, preserve_quotes_and_whitespaces, allow_external_whitespaces, cidx, result);
        cidx = extraction_report[0];
        warning = warning || extraction_report[1];
    }
    if (src.charAt(src.length - 1) == dlm)
        result.push('');
    return [result, warning];
}


function quote_field(src, delim) {
    if (src.indexOf('"') != -1 || src.indexOf(delim) != -1) {
        var escaped = src.replace(/"/g, '""');
        escaped = '"' + escaped + '"';
        return escaped;
    }
    return src;
}


function unquote_field(field) {
    let rgx = /^ *"((?:[^"]*"")*[^"]*)" *$/;
    let match_obj = rgx.exec(field);
    if (match_obj !== null) {
        return match_obj[1].replace(/""/g, '"');
    }
    return field;
}


function unquote_fields(fields) {
    return fields.map(unquote_field);
}


function split_whitespace_separated_str(src, preserve_whitespaces=false) {
    var rgxp = preserve_whitespaces ? new RegExp(' *[^ ]+ *', 'g') : new RegExp('[^ ]+', 'g');
    let result = [];
    let match_obj = null;
    while((match_obj = rgxp.exec(src)) !== null) {
        result.push(match_obj[0]);
    }
    if (preserve_whitespaces) {
        for (let i = 0; i < result.length - 1; i++) {
            result[i] = result[i].slice(0, -1);
        }
    }
    return result;
}


function smart_split(src, dlm, policy, preserve_quotes_and_whitespaces) {
    if (policy === 'simple')
        return [src.split(dlm), false];
    if (policy === 'whitespace')
        return [split_whitespace_separated_str(src, preserve_quotes_and_whitespaces), false];
    if (policy === 'monocolumn')
        return [[src], false];
    return split_quoted_str(src, dlm, preserve_quotes_and_whitespaces);
}


function remove_utf8_bom(line, assumed_source_encoding) {
    if (assumed_source_encoding == 'binary' && line.length >= 3 && line.charCodeAt(0) === 0xEF && line.charCodeAt(1) === 0xBB && line.charCodeAt(2) === 0xBF) {
        return line.substring(3);
    }
    if (assumed_source_encoding == 'utf-8' && line.length >= 1 && line.charCodeAt(0) === 0xFEFF) {
        return line.substring(1);
    }
    return line;
}


function make_inconsistent_num_fields_warning(table_name, inconsistent_records_info) {
    let keys = Object.keys(inconsistent_records_info);
    let entries = [];
    for (let i = 0; i < keys.length; i++) {
        let key = keys[i];
        let record_id = inconsistent_records_info[key];
        entries.push([record_id, key]);
    }
    entries.sort(function(a, b) { return a[0] - b[0]; });
    assert(entries.length > 1);
    let [record_1, num_fields_1] = entries[0];
    let [record_2, num_fields_2] = entries[1];
    let warn_msg = `Number of fields in "${table_name}" table is not consistent: `;
    warn_msg += `e.g. record ${record_1} -> ${num_fields_1} fields, record ${record_2} -> ${num_fields_2} fields`;
    return warn_msg;
}


function expanduser(filepath) {
    if (filepath.charAt(0) === '~') {
        return path.join(os.homedir(), filepath.slice(1));
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
    let table_names_settings_path = path.join(os.homedir(), '.rbql_table_names');
    var name_record = get_index_record(table_names_settings_path, table_id);
    if (name_record && name_record.length > 1 && fs.existsSync(name_record[1])) {
        return name_record[1];
    }
    return null;
}



function CSVRecordIterator(stream, encoding, delim, policy, table_name='input') {
    this.stream = stream;
    this.encoding = encoding;
    if (this.encoding) {
        this.stream.setEncoding(this.encoding);
    }
    this.delim = delim;
    this.policy = policy;
    this.table_name = table_name;
    this.line_reader = null;

    this.external_record_callback = null;
    this.external_finish_callback = null;
    this.external_line_callback = null;
    this.line_reader_closed = true;
    this.finished = false;

    this.utf8_bom_removed = false;
    this.first_defective_line = null;

    this.fields_info = new Object();
    this.NR = 0;

    this.set_record_callback = function(external_record_callback) {
        this.external_record_callback = external_record_callback;
    };


    this.set_finish_callback = function(external_finish_callback) {
        this.external_finish_callback = external_finish_callback;
    };

    this._set_line_callback = function(external_line_callback) {
        this.external_line_callback = external_line_callback;
    };


    this.process_line = function(line) {
        if (this.finished) {
            return;
        }
        if (this.NR === 0) {
            var clean_line = remove_utf8_bom(line, this.encoding);
            if (clean_line != line) {
                line = clean_line;
                this.utf8_bom_removed = true;
            }
        }
        this.NR += 1;
        var [record, warning] = smart_split(line, this.delim, this.policy, false);
        if (warning && this.first_defective_line === null)
            this.first_defective_line = this.NR;
        let num_fields = record.length;
        if (!this.fields_info.hasOwnProperty(num_fields))
            this.fields_info[num_fields] = this.NR;
        this.external_record_callback(record);
    };


    this._get_all_records = function(external_records_callback) {
        let records = [];
        let record_callback = function(record) {
            records.push(record);
        };
        let finish_callback = function() {
            external_records_callback(records);
        };
        this.set_record_callback(record_callback);
        this.set_finish_callback(finish_callback);
        this.start();
    };


    this._get_all_lines = function(external_lines_callback) {
        let lines = [];
        let line_callback = function(line) {
            lines.push(line);
        };
        let finish_callback = function() {
            external_lines_callback(lines);
        };
        this._set_line_callback(line_callback);
        this.set_finish_callback(finish_callback);
        this.start();
    };


    this.start = function() {
        this.line_reader = readline.createInterface({ input: this.stream });
        this.line_reader_closed = false;
        if (!this.external_line_callback) {
            this.line_reader.on('line', (line) => { this.process_line(line); });
        } else {
            this.line_reader.on('line', (line) => { this.external_line_callback(line); });
        }
        this.line_reader.on('close', () => { this.line_reader_closed = true; this.finish(); });
    };


    this.finish = function() {
        if (!this.line_reader_closed) {
            this.line_reader_closed = true;
            this.line_reader.close();
        }
        if (!this.finished) {
            this.finished = true;
            this.external_finish_callback();
        }
    };


    this.get_warnings = function() {
        let result = [];
        if (this.first_defective_line !== null)
            result.push(`Defective double quote escaping in ${this.table_name} table. E.g. at line ${this.first_defective_line}`);
        if (this.utf8_bom_removed)
            result.push(`UTF-8 Byte Order Mark (BOM) was found and skipped in ${this.table_name} table`);
        if (Object.keys(this.fields_info).length > 1)
            result.push(make_inconsistent_num_fields_warning('input', this.fields_info));
        return result;
    };
}


function CSVWriter(stream, encoding, delim, policy, line_separator='\n') {
    this.stream = stream;
    if (encoding)
        this.stream.setDefaultEncoding(encoding);
    this.delim = delim;
    this.policy = policy;
    this.line_separator = line_separator;

    this.null_in_output = false;
    this.delim_in_simple_output = false;


    this.quoted_join = function(fields) {
        let delim = this.delim;
        var quoted_fields = fields.map(function(v) { return quote_field(String(v), delim); });
        return quoted_fields.join(this.delim);
    };


    this.mono_join = function(fields) {
        if (fields.length > 1) {
            throw new RbqlIOHandlingError('Unable to use "Monocolumn" output format: some records have more than one field');
        }
        return fields[0];
    };


    this.simple_join = function(fields) {
        var res = fields.join(this.delim);
        if (fields.join('').indexOf(this.delim) != -1) {
            this.delim_in_simple_output = true;
        }
        return res;
    };


    if (policy == 'simple') {
        this.output_join = this.simple_join;
    } else if (policy == 'quoted') {
        this.output_join = this.quoted_join;
    } else if (policy == 'monocolumn') {
        this.output_join = this.mono_join;
    } else if (policy == 'whitespace') {
        this.output_join = this.simple_join;
    } else {
        throw new RbqlIOHandlingError('Unknown output csv policy');
    }


    this.replace_null_values = function(out_fields) {
        for (var i = 0; i < out_fields.length; i++) {
            if (out_fields[i] == null) {
                this.null_in_output = true;
                out_fields[i] = '';
            }
        }
    };


    this.write = function(fields) {
        this.replace_null_values(fields);
        this.stream.write(this.output_join(fields));
        this.stream.write(this.line_separator);
    };


    this._write_all = function(table) {
        for (let i = 0; i < table.length; i++) {
            this.write(table[i]);
        }
    }


    this.finish = function() {
        // Looks like there is no way to flush the stream
    };


    this.get_warnings = function() {
        let result = [];
        if (this.null_in_output)
            result.push('None values in output were replaced by empty strings');
        if (this.delim_in_simple_output)
            result.push('Some output fields contain separator');
        return result;
    };

}



function FileSystemCSVRegistry(delim, policy, encoding) {
    this.delim = delim;
    this.policy = policy;
    this.encoding = encoding;
    this.stream = null;
    this.record_iterator = null;

    this.get_iterator_by_table_id = function(table_id) {
        let table_path = find_table_path(table_id);
        if (table_path === null) {
            throw new RbqlIOHandlingError(`Unable to find join table "${table_id}"`);
        }
        this.stream = fs.createReadStream(table_path);
        this.record_iterator = new CSVRecordIterator(this.stream, this.encoding, this.delim, this.policy, table_id);
        return this.record_iterator;
    };


    this.finish = function() {
        // TODO call this function from somewhere, like we do in Python
        if (this.record_iterator !== null)
            this.record_iterator.finish();
    };
}


module.exports.split_quoted_str = split_quoted_str;
module.exports.split_whitespace_separated_str = split_whitespace_separated_str;
module.exports.smart_split = smart_split;
module.exports.CSVRecordIterator = CSVRecordIterator;
module.exports.CSVWriter = CSVWriter;
module.exports.FileSystemCSVRegistry = FileSystemCSVRegistry;
module.exports.unquote_field = unquote_field;
module.exports.unquote_fields = unquote_fields;
module.exports.interpret_named_csv_format = interpret_named_csv_format;

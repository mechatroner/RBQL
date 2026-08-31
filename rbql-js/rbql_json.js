const fs = require('fs');
const os = require('os');
const path = require('path');
const util = require('util');

const rbql = require('./rbql.js');
const csv_utils = require('./csv_utils.js');

class RbqlIOHandlingError extends Error {}

function deduplicate_header_keys(header) {
    // This algorithm is O(N^2) but we don't expect to have a lot of keys.
    // FIXME add unit tests for this.
    let unique_keys = [];
    let deduplicated_keys = new Set();
    for (let h of header) {
        let unique_key = h;
        let dedup_counter = 1;
        while (unique_keys.indexOf(unique_key) != -1) {
            dedup_counter += 1;
            unique_key = `${h}_${dedup_counter}`;
        }
        unique_keys.push(unique_key);
        if (dedup_counter != 1)
            deduplicated_keys.add(h);
    }
    return [unique_keys, Array.from(deduplicated_keys).sort()];
}


function get_json_object_to_write(header, fields) {
    // FIXME add unit tests.
    if (fields.length == 1)
        return fields[0];
    if (header.length && fields.length != header.length)
        throw new RbqlIOHandlingError(`Inconsistent number of columns in output header and the current record: ${header.length} != ${fields.length}`);
    let result = {};
    for (let i = 0; i < fields.length; i++) {
        let key_name = i < header.length ? header[i] : `col_${i + 1}`;
        result[key_name] = fields[i];
    }
    return result;
}


class JsonLinesWriter extends rbql.RBQLOutputWriter {
    // FIXME add unit tests.
    constructor(stream, close_stream_on_finish, encoding, line_separator='\n') {
        super();
        this.stream = stream;
        this.encoding = encoding;
        if (encoding)
            this.stream.setDefaultEncoding(encoding);
        this.stream.on('error', (error_obj) => { this.store_first_error(error_obj); })
        this.line_separator = line_separator;

        this.close_stream_on_finish = close_stream_on_finish;

        this.header = [];
        this.deduplicated_keys = [];
        this.first_error = null;
    }

    store_first_error(error_obj) {
        // Store only first error because it is typically more important than the subsequent ones.
        if (this.first_error === null)
            this.first_error = error_obj;
    }

    set_header(header) {
        if (header !== null) {
            [this.header, this.deduplicated_keys] = deduplicate_header_keys(header);
        }
    }

    async finish() {
        let close_stream_on_finish = this.close_stream_on_finish;
        let output_stream = this.stream;
        let output_encoding = this.encoding;
        let writer_error = this.first_error;
        let finish_promise = new Promise(function(resolve, reject) {
            if (writer_error !== null) {
                reject(writer_error);
            }
            if (close_stream_on_finish) {
                output_stream.end('', output_encoding, () => { resolve(); });
            } else {
                setTimeout(() => { resolve(); }, 0);
            }
        });
        return finish_promise;
    };


    get_warnings() {
        let result = [];
        if (this.deduplicated_keys.length > 0) {
            let keys_to_report = this.deduplicated_keys.map(key => `"${key}"`).join(', ');
            result.push(`Deduplicated output json keys to avoid data loss: ${keys_to_report}`);
        }
        return result;
    };


    async do_write(object_to_write) {
        this.stream.write(JSON.stringify(object_to_write));
        this.stream.write(this.line_separator);
        let writer_error = this.first_error;
        return new Promise(function(resolve, reject) {
            if (writer_error !== null) {
                reject(writer_error);
            } else {
                resolve();
            }
        });
    }


    async write(fields) {
        let object_to_write = get_json_object_to_write(this.header, fields);
        await this.do_write(object_to_write);
        // The return value is needed for the stacked writers architecture in rbql.js.
        // So far the only writer that can return `false` is the TopWriter which uses it as a flag to request stop when the limit is reached.
        // This csv writer always succeeds (unless there is an exception).
        return true;
    };
}


module.exports.JsonLinesWriter = JsonLinesWriter;

const fs = require('fs');
const os = require('os');
const path = require('path');
const util = require('util');

const rbql = require('./rbql.js');
const csv_utils = require('./csv_utils.js');

class RbqlIOHandlingError extends Error {}


// FIXME add json file test with unicode chars. For py and js versions. Use both input stream and input path modes.

function assert(condition, message=null) {
    if (!condition) {
        if (!message) {
            message = 'Assertion error';
        }
        throw new AssertionError(message);
    }
}


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
    constructor(stream, close_stream_on_finish, encoding='utf-8', line_separator='\n') {
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
        // This writer always succeeds (unless there is an exception).
        return true;
    };
}


class JsonLinesRecordIterator extends rbql.RBQLInputIterator {
    // TODO add query modifier with "noheaders" this would name keys as `a1`, `a2`, etc.
    // FIXME adjust
    // FIXME add unit tests
    constructor(stream, encoding='utf-8', table_name='input', variable_prefix='a') {
        super();
        this.stream = stream;
        this.encoding = encoding;
        this.table_name = table_name;
        this.variable_prefix = variable_prefix;


        this.decoder = null;
        if (encoding == 'utf-8') {
            // This was copied from the csv impl, see comments there.
            this.decoder = new util.TextDecoder(encoding, {fatal: true, stream: true});
        }

        this.input_exhausted = false;
        this.started = false;

        this.NR = 0; // Record number
        this.NL = 0; // Line number

        this.partially_decoded_line = '';
        this.partially_decoded_line_ends_with_cr = false;

        // Holds an external "resolve" function which is called when everything is fine.
        this.resolve_current_record = null;
        // Holds an external "reject" function which is called when error has occured.
        this.reject_current_record = null;
        // Holds last exception if we don't have any reject callbacks from clients yet.
        this.current_exception = null;

        this.produced_records_queue = new csv_utils.RecordQueue();
    }

    get_header() {
        // FIXME consider if this is a hack or not. Test queries with stars like `SELECT a.*, a.*` or `SELECT *`.
        // We might not need this (i.e. we can have it return None as the default impl) if we support the write_header flag, see the rbql_engine.py comments.
        return [`${this.variable_prefix}1`];
    }

    reset_external_callbacks() {
        // Drop external callbacks simultaneously since promises can only resolve once, see: https://stackoverflow.com/a/18218542/2898283
        this.reject_current_record = null;
        this.resolve_current_record = null;
    }

    try_propagate_exception() {
        if (this.current_exception && this.reject_current_record) {
            let reject = this.reject_current_record;
            let exception = this.current_exception;
            this.reset_external_callbacks();
            this.current_exception = null;
            reject(exception);
        }
    }


    store_or_propagate_exception(exception) {
        if (this.current_exception === null)
            // Ignore subsequent exceptions if we already have an unreported error. This way we prioritize earlier errors over the more recent ones.
            this.current_exception = exception;
        this.try_propagate_exception();
    }


    try_resolve_next_record() {
        this.try_propagate_exception();
        if (this.resolve_current_record === null)
            return;

        let record = this.produced_records_queue.dequeue();
        if (record === null && !this.input_exhausted)
            return;
        let resolve = this.resolve_current_record;
        this.reset_external_callbacks();
        resolve(record);
    };


    async get_record() {
        if (!this.started)
            await this.start();
        if (this.stream && this.stream.isPaused())
            this.stream.resume();

        let parent_iterator = this;
        let current_record_promise = new Promise(function(resolve, reject) {
            parent_iterator.resolve_current_record = resolve;
            parent_iterator.reject_current_record = reject;
        });
        this.try_resolve_next_record();
        return current_record_promise;
    };


    process_record_line(line) {
        this.NR += 1;
        this.produced_records_queue.enqueue([JSON.parse(line)]);
        this.try_resolve_next_record();
    };


    process_line(line) {
        this.NL += 1;
        this.process_record_line(line);
    };


    process_data_stream_chunk(data_chunk) {
        let decoded_string = null;
        if (this.decoder) {
            try {
                decoded_string = this.decoder.decode(data_chunk);
            } catch (e) {
                if (e instanceof TypeError) {
                    this.store_or_propagate_exception(new RbqlIOHandlingError(utf_decoding_error));
                } else {
                    this.store_or_propagate_exception(e);
                }
                return;
            }
        } else {
            decoded_string = data_chunk.toString(this.encoding);
        }
        let line_starts_with_lf = decoded_string.length && decoded_string[0] == '\n';
        let first_line_index = line_starts_with_lf && this.partially_decoded_line_ends_with_cr ? 1 : 0;
        this.partially_decoded_line_ends_with_cr = decoded_string.length && decoded_string[decoded_string.length - 1] == '\r';
        let lines = csv_utils.split_lines(decoded_string);
        lines[0] = this.partially_decoded_line + lines[0];
        assert(first_line_index == 0 || lines[0].length == 0);
        this.partially_decoded_line = lines.pop();
        for (let i = first_line_index; i < lines.length; i++) {
            this.process_line(lines[i]);
        }
    };


    process_data_stream_end() {
        this.input_exhausted = true;
        if (this.partially_decoded_line.length) {
            let last_line = this.partially_decoded_line;
            this.partially_decoded_line = '';
            this.process_line(last_line);
        }
        this.try_resolve_next_record();
    };


    stop() {
        if (this.stream)
            this.stream.destroy(); // TODO consider using pause() instead
    };


    async start() {
        if (this.started)
            return;
        this.started = true;
        this.stream.on('data', (data_chunk) => { this.process_data_stream_chunk(data_chunk); });
        this.stream.on('end', () => { this.process_data_stream_end(); });
    };


    get_warnings() {
        let result = [];
        return result;
    };
}

async function query_json(query_text, input_path, output_path, output_warnings, user_init_code='') {
    let input_stream = input_path === null ? process.stdin : fs.createReadStream(input_path);
    let [output_stream, close_output_on_finish] = output_path === null ? [process.stdout, false] : [fs.createWriteStream(output_path), true];

    let default_init_source_path = path.join(os.homedir(), '.rbql_init_source.js');
    if (user_init_code == '' && fs.existsSync(default_init_source_path)) {
        user_init_code = read_user_init_code(default_init_source_path);
    }
    let input_file_dir = input_path ? path.dirname(input_path) : null;
    let join_tables_registry = null;
    let input_iterator = new JsonLinesRecordIterator(input_stream);
    let output_writer = new JsonLinesWriter(output_stream, close_output_on_finish);
    await rbql.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code);
}


module.exports.JsonLinesWriter = JsonLinesWriter;
module.exports.JsonLinesRecordIterator = JsonLinesRecordIterator;
module.exports.query_json = query_json;

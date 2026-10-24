const fs = require('fs');
const os = require('os');
const path = require('path');
const util = require('util');

const rbql = require('./rbql.js');
const csv_utils = require('./csv_utils.js');


const backslashChar = '\\';

class RbqlJsonParsingError extends Error {
    constructor(message) {
        super(message);
        this.name = 'RbqlJsonParsingError';
    }
}

class UnparsedJsonBlob {
    constructor() {
        this.bracket_stack = [];
        this.inside_string = false;
        this.backslash_escape_flag = false;
    }
}

class BalancingIterator {
    constructor(unwrap_mode = false) {
        this.buf = '';
        this.unwrap_mode = unwrap_mode;
        this.parsed_objects = []; // There is no deque in JS so we use an array. Therefore `advance()` takes O(N).
        this.current = new UnparsedJsonBlob();
        this.open_bracket_skipped = this.unwrap_mode ? false : true;
        this.closing_bracket_skipped = false;
        this.num_parsed = 0;
    }

    add_fragment(fragment) {
        const seek_start_pos = this.buf.length;
        this.buf += fragment;
        this._parse_forward(seek_start_pos);
    }

    get_current_object() {
        return this.parsed_objects[0];
    }

    advance() {
        return this.parsed_objects.shift(); // Equivalent to popleft(), but O(N)
    }

    empty() {
        return this.parsed_objects.length === 0;
    }

    has_trailing_data() {
        return this.buf.length > 0;
    }

    finish() {
        if (this.unwrap_mode && !this.closing_bracket_skipped) {
            throw new RbqlJsonParsingError("Closing square bracket `]` at the end is required in unwrap mode");
        }
        if (!this.current.inside_string && this.current.bracket_stack.length === 0) {
            this._consume_fragment(this.buf);
            this.buf = '';
        }
    }

    _consume_fragment(fragment) {
        fragment = fragment.trim();
        if (fragment.length > 0) {
            try {
                this.parsed_objects.push(JSON.parse(fragment));
            } catch (e) {
                throw new RbqlJsonParsingError(`Failed to deserialize a record after the first ${this.num_parsed} records: ${e}`);
            }
            this.num_parsed += 1;
            this.current = new UnparsedJsonBlob();
        }
    }

    skip_open_square_bracket(idx) {
        while (idx < this.buf.length && isSpace(this.buf[idx])) {
            idx += 1;
        }
        if (idx >= this.buf.length) {
            return idx;
        }
        if (this.buf[idx] !== '[') {
            throw new RbqlJsonParsingError(`Input data must be wrapped in JSON array in unwrap mode. Expected \`[\` got \`${this.buf[idx]}\` char`);
        }
        this.open_bracket_skipped = true;
        return idx + 1;
    }

    skip_trailing_spaces(idx) {
        while (idx < this.buf.length) {
            if (isSpace(this.buf[idx])) {
                idx += 1;
                continue;
            }
            throw new RbqlJsonParsingError("Trailing data after the closing square bracket `]` in unwrap mode");
        }
        return idx;
    }

    _parse_forward(seek_start_pos) {
        let i = seek_start_pos;
        if (!this.open_bracket_skipped) {
            i = this.skip_open_square_bracket(i);
            this.buf = this.buf.slice(i);
            i = 0;
        }
        while (i < this.buf.length) {
            if (this.closing_bracket_skipped) {
                this.skip_trailing_spaces(i);
                return;
            }
            let object_can_end = false;
            let end_offset = 0; // This is to be able to drop trailing `,` or `]` in the unwrap mode.
            
            if (this.current.inside_string) {
                if (this.buf[i] === '"' && !this.current.backslash_escape_flag) {
                    this.current.inside_string = false;
                    // This is to handle a sequence of json strings like this `"str1""str2""str3"` - strings don't have to be separated with whitespaces.
                    object_can_end = true;
                }
                if (this.buf[i] === backslashChar) {
                    this.current.backslash_escape_flag = !this.current.backslash_escape_flag;
                } else {
                    // We don't check that the preceeding backslash cretes a valid escape sequence.
                    // This allows invalid escape sequences inside strings e.g. "hello\ world" but it is fine since external parsing would catch it anyway.
                    this.current.backslash_escape_flag = false;
                }
            } else if (this.buf[i] === '"') {
                this.current.inside_string = true;
                this.current.backslash_escape_flag = false; // Just in case although it is probably redundant since we would reset the flag anyway when finishing the previous string.
            } else if (this.buf[i] === '{' || this.buf[i] === '[') {
                this.current.bracket_stack.push(this.buf[i]);
            } else if (this.buf[i] === '}') {
                if (this.current.bracket_stack.length === 0 || this.current.bracket_stack.pop() !== '{') {
                    throw new RbqlJsonParsingError(`Found an unbalanced closing curly bracket \`}\` after parsing the first ${this.num_parsed} records`);
                }
                object_can_end = true;
            } else if (this.buf[i] === ']') {
                if (this.unwrap_mode && this.current.bracket_stack.length === 0 && !this.closing_bracket_skipped) {
                    this.closing_bracket_skipped = true;
                    end_offset = 1;
                } else if (this.current.bracket_stack.length === 0 || this.current.bracket_stack.pop() !== '[') {
                    throw new RbqlJsonParsingError(`Found an unbalanced closing square bracket \`]\` after parsing the first ${this.num_parsed} records`);
                }
                object_can_end = true;
            } else if (this.buf[i] === ',' && this.unwrap_mode) {
                // TODO This would make a sequence of commas a valid noop span, consider fixing it.
                end_offset = 1;
                object_can_end = true;
            } else if (isSpace(this.buf[i])) {
                object_can_end = true;
            }
            
            i += 1;
            
            if (object_can_end && this.current.bracket_stack.length === 0) {
                const fragment = this.buf.slice(0, i - end_offset);
                this.buf = this.buf.slice(i);
                i = 0;
                this._consume_fragment(fragment);
            }
        }
    }
}

// Helper function mimicking Python's str.isspace() for single characters
function isSpace(ch) {
    return /\s/.test(ch);
}


class RbqlIOHandlingError extends Error {}


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


function stringify_json_line_python_style(obj) {
    // The default JSON.stringify() unfortunatelly doesn't produce pretty json lines like in json lines examples here: https://jsonlines.org/examples/
    // Python by default stringifies with extra readability whitespaces e.g. `{'foo':1,'bar':2}` becomes `{"foo": 1, "bar": 2}`.
    // The third `spaces` arg in `JSON.stringify` makes it pretty print the whole thing with newlines, so we post-format it via this hack.
    return JSON.stringify(obj, null, 1).replace(/^ +/gm, "").replace(/,\n/g, ", \n").replace(/\n/g, "");
}


class JsonWriter extends rbql.RBQLOutputWriter {
    constructor(stream, close_stream_on_finish, encoding='utf-8', line_separator='\n', pretty_indent=null, wrap_into_array=false) {
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
        this.num_records_written = 0;
        this.pretty_indent = pretty_indent;
        this.wrap_into_array = wrap_into_array;
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
        if (this.wrap_into_array) {
            if (this.num_records_written == 0) {
                // Output an empty array if no entries were produced.
                this.stream.write('[');
            }
            this.stream.write(this.line_separator);
            this.stream.write(']');
            this.stream.write(this.line_separator); // POSIX requires a newline at the end of text files.
        } else {
            if (this.num_records_written > 0) {
                this.stream.write(this.line_separator); // POSIX requires a newline at the end of text files.
            }
        }
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
        if (this.num_records_written == 0) {
            if (this.wrap_into_array) {
                this.stream.write('[');
                this.stream.write(this.line_separator);
            }
        } else {
            if (this.wrap_into_array) {
                this.stream.write(',');
            }
            this.stream.write(this.line_separator);
        }
        if (this.pretty_indent === null) {
            this.stream.write(stringify_json_line_python_style(object_to_write));
        } else {
            this.stream.write(JSON.stringify(object_to_write, null, this.pretty_indent));
        }
        this.num_records_written += 1;
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

    handle_query_modifier(modifier) {
        if (modifier == 'wrap') {
            assert(this.num_records_written == 0)
            this.wrap_into_array = true;
        }
    }
}


class JsonIterator extends rbql.RBQLInputIterator {
    // TODO add query modifier with "noheaders" this would name keys as `a1`, `a2`, etc.
    constructor(stream, unwrap_mode=false, encoding='utf-8', table_name='input', variable_prefix='a') {
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

        // Holds an external "resolve" function which is called when everything is fine.
        this.resolve_current_record = null;
        // Holds an external "reject" function which is called when error has occured.
        this.reject_current_record = null;
        // Holds last exception if we don't have any reject callbacks from clients yet.
        this.current_exception = null;

        this.balancing_iterator = new BalancingIterator(unwrap_mode);
        this.produced_records_queue = new csv_utils.RecordQueue();
    }

    get_header() {
        // Returning "a1" as a column name actually has a side effect because it would try to initialize a.a1 and a['a1'] values in rbql engine.
        return [this.variable_prefix];
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
        try {
            this.balancing_iterator.add_fragment(decoded_string);
        } catch (e) {
            this.store_or_propagate_exception(e);
        }
        while (!this.balancing_iterator.empty()) {
            try {
                this.produced_records_queue.enqueue([this.balancing_iterator.advance()]);
            } catch (e) {
                this.store_or_propagate_exception(e);
                return;
            }
            this.try_resolve_next_record();
        }
    };


    process_data_stream_end() {
        this.input_exhausted = true;
        try {
            this.balancing_iterator.finish();
        } catch (e) {
            this.store_or_propagate_exception(e);
        }
        if (this.balancing_iterator.has_trailing_data()) {
            this.store_or_propagate_exception(new RbqlIOHandlingError(`Trailing data in ${this.table_name} stream that can't be parsed as JSON objects`));
        }
        // This last call is needed so that null is dequeued from the queue which initiaties the query completion.
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

    handle_query_modifier(modifier) {
        if (modifier == 'unwrap') {
            assert(!this.started)
            this.balancing_iterator = new BalancingIterator(/*unwrap_mode=*/true);
        }
    }
}

async function query_json(query_text, input_path, output_path, output_warnings, params) {
    let user_init_code = params.hasOwnProperty('user_init_code') ? params['user_init_code'] : '';
    let pretty_indent = params.hasOwnProperty('pretty_indent') ? params['pretty_indent'] : null;
    let unwrap_array = params.hasOwnProperty('unwrap_array') ? params['unwrap_array'] : false;
    let wrap_into_array = params.hasOwnProperty('wrap_into_array') ? params['wrap_into_array'] : false;
    let input_stream = input_path === null ? process.stdin : fs.createReadStream(input_path);
    let [output_stream, close_output_on_finish] = output_path === null ? [process.stdout, false] : [fs.createWriteStream(output_path), true];

    let default_init_source_path = path.join(os.homedir(), '.rbql_init_source.js');
    if (user_init_code == '' && fs.existsSync(default_init_source_path)) {
        user_init_code = read_user_init_code(default_init_source_path);
    }
    let input_file_dir = input_path ? path.dirname(input_path) : null;
    let join_tables_registry = null;

    let input_iterator = new JsonIterator(input_stream, unwrap_array);
    let output_writer = new JsonWriter(output_stream, close_output_on_finish, 'utf-8', '\n', /*pretty_indent=*/pretty_indent, /*wrap_into_array=*/wrap_into_array);

    await rbql.query(query_text, input_iterator, output_writer, output_warnings, join_tables_registry, user_init_code);
}


module.exports.deduplicate_header_keys = deduplicate_header_keys;
module.exports.stringify_json_line_python_style = stringify_json_line_python_style;
module.exports.get_json_object_to_write = get_json_object_to_write;
module.exports.JsonWriter = JsonWriter;
module.exports.JsonIterator = JsonIterator;
module.exports.BalancingIterator = BalancingIterator;
module.exports.query_json = query_json;

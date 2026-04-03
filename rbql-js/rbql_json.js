const fs = require('fs');
const os = require('os');
const path = require('path');
const util = require('util');

const rbql = require('./rbql.js');
//const csv_utils = require('./csv_utils.js');


class JsonLinesRecordIterator extends rbql.RBQLInputIterator {
    constructor(stream, json_path, encoding, table_name='input', variable_prefix='a') {
        super();
        this.stream = stream;
        this.json_path = json_path;
        assert((this.stream === null) != (this.json_path === null));
        this.encoding = encoding;

        this.table_name = table_name;
        this.variable_prefix = variable_prefix;

        this.decoder = null;
        if (encoding == 'utf-8' && this.json_path === null) {
            // Unfortunately util.TextDecoder has serious flaws:
            // 1. It doesn't work in Node without ICU: https://nodejs.org/api/util.html#util_new_textdecoder_encoding_options
            // 2. It is broken in Electron: https://github.com/electron/electron/issues/18733

            // Technically we can implement our own custom streaming text decoder, using the 3 following technologies:
            // 1. decode-encode validation method from https://stackoverflow.com/a/32279283/2898283
            // 2. Scanning buffer chunks for non-continuation utf-8 bytes from the end of the buffer:
            //    src_buffer -> (buffer_before, buffer_after) where buffer_after is very small(a couple of bytes) and buffer_before is large and ends with a non-continuation bytes
            // 3. Internal buffer to store small tail part from the previous buffer
            this.decoder = new util.TextDecoder(encoding, {fatal: true, stream: true});
        }

        this.input_exhausted = false;
        this.started = false;

        this.utf8_bom_removed = false; // BOM doesn't get automatically removed by the decoder when utf-8 file is treated as latin-1

        this.NR = 0; // Record number
        this.NL = 0; // Line number (NL != NR when the CSV file has comments or multiline fields)

        this.line_aggregator = new csv_utils.MultilineRecordAggregator(comment_prefix, comment_regex);

        //this.partially_decoded_line = '';
        //this.partially_decoded_line_ends_with_cr = false;

        // Holds an external "resolve" function which is called when everything is fine.
        this.resolve_current_record = null;
        // Holds an external "reject" function which is called when error has occured.
        this.reject_current_record = null;
        // Holds last exception if we don't have any reject callbacks from clients yet.
        this.current_exception = null;

        this.produced_records_queue = new RecordQueue();

        //this.process_line_polymorphic = policy == 'quoted_rfc' ? this.process_partial_rfc_record_line : this.process_record_line_simple;

        //this.polymorphic_split = csv_utils.get_polymorphic_split_function(this.delim, this.policy, false);
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

        let record = null;
        if (this.first_record_should_be_emitted && this.header_preread_complete) {
            this.first_record_should_be_emitted = false;
            record = this.first_record;
        } else {
            record = this.produced_records_queue.dequeue();
        }

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


    async get_all_records(num_records=null) {
        let records = [];
        while (true) {
            let record = await this.get_record();
            if (record === null)
                break;
            records.push(record);
            if (num_records && records.length >= num_records) {
                this.stop();
                break;
            }
        }
        return records;
    };


    process_record_line_simple(line) {
        if (this.comment_prefix && line.startsWith(this.comment_prefix))
            return; // Just skip the line
        if (this.comment_regex && line.search(this.comment_regex) != -1)
            return; // Just skip the line
        this.process_record_line(line);
    }


    process_record_line(line) {
        this.NR += 1;
        var [record, warning] = this.polymorphic_split(line);
        if (this.trim_whitespaces) {
            record = record.map((v) => v.trim());
        }
        if (warning) {
            if (this.first_defective_line === null) {
                this.first_defective_line = this.NL;
                if (this.policy == 'quoted_rfc')
                    this.store_or_propagate_exception(new RbqlIOHandlingError(`Inconsistent double quote escaping in ${this.table_name} table at record ${this.NR}, line ${this.NL}`));
            }
        }
        let num_fields = record.length;
        if (!this.fields_info.has(num_fields))
            this.fields_info.set(num_fields, this.NR);
        this.produced_records_queue.enqueue(record);
        this.try_resolve_next_record();
    };


    process_line(line) {
        this.NL += 1;
        if (this.NL === 1) {
            var clean_line = remove_utf8_bom(line, this.encoding);
            if (clean_line != line) {
                line = clean_line;
                this.utf8_bom_removed = true;
            }
        }
        this.process_line_polymorphic(line);
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


    process_data_bulk(data_blob) {
        let decoded_string = data_blob.toString(this.encoding);
        if (this.encoding == 'utf-8') {
            // Using hacky comparison method from here: https://stackoverflow.com/a/32279283/2898283
            // TODO get rid of this once TextDecoder is really fixed or when alternative method of reliable decoding appears
            let control_buffer = Buffer.from(decoded_string, 'utf-8');
            if (Buffer.compare(data_blob, control_buffer) != 0) {
                this.store_or_propagate_exception(new RbqlIOHandlingError(utf_decoding_error));
                return;
            }
        }
        let lines = csv_utils.split_lines(decoded_string);
        if (lines.length && lines[lines.length - 1].length == 0)
            lines.pop();
        for (let i = 0; i < lines.length; i++) {
            this.process_line(lines[i]);
        }
        if (this.line_aggregator.is_inside_multiline_record()) {
            this.process_record_line(this.line_aggregator.get_full_line('\n'));
        }
        this.input_exhausted = true;
        this.try_resolve_next_record(); // Should be a NOOP here?
    }


    process_data_stream_end() {
        this.input_exhausted = true;
        if (this.partially_decoded_line.length) {
            let last_line = this.partially_decoded_line;
            this.partially_decoded_line = '';
            this.process_line(last_line);
        }
        if (this.line_aggregator.is_inside_multiline_record()) {
            this.process_record_line(this.line_aggregator.get_full_line('\n'));
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
        if (this.stream) {
            this.stream.on('data', (data_chunk) => { this.process_data_stream_chunk(data_chunk); });
            this.stream.on('end', () => { this.process_data_stream_end(); });
        } else {
            let parent_iterator = this;
            return new Promise(function(resolve, reject) {
                fs.readFile(parent_iterator.json_path, (err, data_blob) => {
                    if (err) {
                        reject(err);
                    } else {
                        parent_iterator.process_data_bulk(data_blob);
                        resolve();
                    }
                });
            });
        }
    };


    get_warnings() {
        let result = [];
        if (this.first_defective_line !== null)
            result.push(`Inconsistent double quote escaping in ${this.table_name} table. E.g. at line ${this.first_defective_line}`);
        if (this.utf8_bom_removed)
            result.push(`UTF-8 Byte Order Mark (BOM) was found and skipped in ${this.table_name} table`);
        if (this.fields_info.size > 1)
            result.push(make_inconsistent_num_fields_warning(this.table_name, this.fields_info));
        return result;
    };
}

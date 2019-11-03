function die(error_msg) {
    console.error('Error: ' + error_msg);
    process.exit(1);
}

function fail(error_msg, exit_at_error, silent) {
    if (!error_msg)
        error_msg = 'Assertion failed';
    if (exit_at_error)
        die(error_msg);
    if (!silent)
        console.error(error_msg);
    return false;
}


function round_floats(src_table) {
    for (let r = 0; r < src_table.length; r++) {
        for (let c = 0; c < src_table[r].length; c++) {
            if ((typeof src_table[r][c]) == 'number' && !(src_table[r][c] === parseInt(src_table[r][c], 10))) {
                src_table[r][c] = parseFloat(src_table[r][c].toFixed(3));
            }
        }
    }
}


function assert(condition, message=null, exit_at_error=true, silent=false) {
    if (condition)
        return true;
    return fail(message, exit_at_error, silent);
}


function assert_equal(a, b, exit_at_error=true, silent=false) {
    if (a != b)
        return fail(`Assertion error: assert_equal has failed: a = "${a}", b = "${b}"`, exit_at_error, silent);
    return true;
}


function assert_arrays_are_equal(a, b, exit_at_error=true, silent=false) {
    if (a.length != b.length) {
        let error_msg = `Arrays have different length: a.length = ${a.length}, b.length = ${b.length}\na: ${JSON.stringify(a)}\nb: ${JSON.stringify(b)}`;
        return fail(error_msg, exit_at_error, silent);
    }
    for (var i = 0; i < a.length; i++) {
        if (Array.isArray(a[i]) != Array.isArray(b[i])) {
            let error_msg = `Subarray mismatch: a[${i}] is array: ${Array.isArray(a[i])}, b[${i}] is array: ${Array.isArray(b[i])}`;
            return fail(error_msg, exit_at_error, silent);
        }
        if (Array.isArray(a[i])) {
            if (!assert_arrays_are_equal(a[i], b[i], exit_at_error, silent)) {
                return fail('Assertion failed', exit_at_error, silent);
            }
        } else if (a[i] !== b[i]) {
            let error_msg = `Array mismatch at ${i} a[i] = ${a[i]}, b[i] = ${b[i]}`;
            return fail(`Array mismatch at ${i} a[i] = ${a[i]}, b[i] = ${b[i]}`, exit_at_error, silent);
        }
    }
    return true;
}


function assert_tables_are_equal(a, b, exit_at_error=true, silent=false) {
    // FIXME get rid of this, replace all usages with assert_arrays_are_equal() - it can check tables too
    if (a.length != b.length) {
        return fail(`a.length = ${a.length} != b.length = ${b.length}`, exit_at_error, silent);
    }
    for (var i = 0; i < a.length; i++) {
        if (!assert_arrays_are_equal(a[i], b[i], false))
            return fail(`Mismatch at row ${i}`, exit_at_error, silent);
    }
    return true;
}


function assert_objects_are_equal(a, b, exit_at_error=true, silent=false, current_path='') {
    if (a === b)
        return true;
    if (a == null)
        return fail(`a is null for path: ${current_path}`, exit_at_error, silent);
    if (b == null)
        return fail(`b is null for path: ${current_path}`, exit_at_error, silent);
    if (typeof a != 'object' && typeof b != 'object')
        return fail(`a = ${a} and b = ${b} have different values for path: ${current_path}`, exit_at_error, silent);
    if (typeof a != 'object')
        return fail(`a is not object for path: ${current_path}`, exit_at_error, silent);
    if (typeof b != 'object')
        return fail(`b is not object for path: ${current_path}`, exit_at_error, silent);
    for (var prop in b) {
        let child_path = current_path + '.' + prop;
        if (!(prop in a))
            return fail(`a does not have property: ${child_path}`, exit_at_error, silent);
        if (!assert_objects_are_equal(a[prop], b[prop], exit_at_error, silent, child_path))
            return false;
    }
    for (var prop in a) {
        if (!(prop in b)) {
            let child_path = current_path + '.' + prop;
            return fail(`b does not have property: ${child_path}`, exit_at_error, silent);
        }
    }
    return true;
}


function normalize_warnings(warnings) {
    let result = [];
    for (let warning of warnings) {
        if (warning.indexOf('Number of fields in "input" table is not consistent') != -1) {
            result.push('inconsistent input records');
        } else if (warning.indexOf('Defective double quote escaping') != -1) {
            result.push('defective double quote escaping');
        } else if (warning.indexOf('None values in output were replaced by empty strings') != -1) {
            result.push('null values in output were replaced');
        } else if (warning === 'UTF-8 Byte Order Mark (BOM) was found and skipped in input table') {
            result.push('BOM removed from input');
        } else {
            assert(false, 'Unknown warning');
        }
    }
    return result;
}


function get_default(obj, key, default_value) {
    if (obj.hasOwnProperty(key))
        return obj[key];
    return default_value;
}


module.exports.get_default = get_default;
module.exports.normalize_warnings = normalize_warnings;
module.exports.assert_objects_are_equal = assert_objects_are_equal;
module.exports.assert = assert;
module.exports.assert_tables_are_equal = assert_tables_are_equal;
module.exports.assert_arrays_are_equal = assert_arrays_are_equal;
module.exports.assert_equal = assert_equal;
module.exports.round_floats = round_floats;

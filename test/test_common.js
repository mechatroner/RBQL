function round_floats(src_table) {
    for (let r = 0; r < src_table.length; r++) {
        for (let c = 0; c < src_table[r].length; c++) {
            if ((typeof src_table[r][c]) == 'number' && !(src_table[r][c] === parseInt(src_table[r][c], 10))) {
                src_table[r][c] = parseFloat(src_table[r][c].toFixed(3));
            }
        }
    }
}


function arrays_are_equal(a, b) {
    if (a.length != b.length)
        return false;
    for (var i = 0; i < a.length; i++) {
        if (a[i] !== b[i]) {
            console.log('mismatch at ' + i + ' a[i] = ' + a[i] + ', b[i] = ' + b[i]);
            return false;
        }
    }
    return true;
}


function tables_are_equal(a, b) {
    if (a.length != b.length)
        return false;
    for (var i = 0; i < a.length; i++) {
        if (!arrays_are_equal(a[i], b[i]))
            return false;
    }
    return true;
}


function objects_are_equal(a, b) {
    if (a === b)
        return true;
    if (a == null || typeof a != 'object' || b == null || typeof b != 'object')
        return false;
    var num_props_in_a = 0;
    var num_props_in_b = 0;
    for (var prop in a)
         num_props_in_a += 1;
    for (var prop in b) {
        num_props_in_b += 1;
        if (!(prop in a) || !objects_are_equal(a[prop], b[prop]))
            return false;
    }
    return num_props_in_a == num_props_in_b;
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
module.exports.objects_are_equal = objects_are_equal;
module.exports.tables_are_equal = tables_are_equal;
module.exports.arrays_are_equal = arrays_are_equal;
module.exports.round_floats = round_floats;

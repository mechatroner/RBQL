let field_regular_expression = '"((?:[^"]*"")*[^"]*)"';
let field_rgx = new RegExp('^' + field_regular_expression);
let field_rgx_external_whitespaces = new RegExp('^' + ' *'+ field_regular_expression + ' *')

function extract_next_field(src, dlm, preserve_quotes, allow_external_whitespaces, cidx, result) {
    var warning = false;
    let src_cur = src.substring(cidx);
    let rgx = allow_external_whitespaces ? field_rgx_external_whitespaces : field_rgx;
    let match_obj = rgx.exec(src_cur);
    if (match_obj !== null) {
        let match_end = match_obj[0].length;
        if (cidx + match_end == src.length || src[cidx + match_end] == dlm) {
            if (preserve_quotes) {
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


function split_quoted_str(src, dlm, preserve_quotes=false) {
    if (src.indexOf('"') == -1) // Optimization for most common case
        return [src.split(dlm), false];
    var result = [];
    var cidx = 0;
    var warning = false;
    let allow_external_whitespaces = dlm != ' ';
    while (cidx < src.length) {
        var extraction_report = extract_next_field(src, dlm, preserve_quotes, allow_external_whitespaces, cidx, result);
        cidx = extraction_report[0];
        warning = warning || extraction_report[1];
    }
    if (src.charAt(src.length - 1) == dlm)
        result.push('');
    return [result, warning];
}


function occurrences(string, subString, allowOverlapping=false) {
    // @author Vitim.us https://gist.github.com/victornpb/7736865

    string += "";
    subString += "";
    if (subString.length <= 0) return (string.length + 1);

    var n = 0,
        pos = 0,
        step = allowOverlapping ? 1 : subString.length;

    while (true) {
        pos = string.indexOf(subString, pos);
        if (pos >= 0) {
            ++n;
            pos += step;
        } else break;
    }
    return n;
}


function split_whitespace_separated_str(src, preserve_whitespaces=false) {
    var rgxp = preserve_whitespaces ? new RegExp(' *[^ ]+ *', 'g') : new RegExp('[^ ]+', 'g');
    let result = [];
    let match_obj = null;
    while((match_obj = rgxp.exec(src)) !== null) {
        result.push(match_obj[0]);
    }
    return result;
}


function smart_split(src, dlm, policy, preserve_quotes) {
    if (policy === 'simple')
        return [src.split(dlm), false];
    if (policy === 'monocolumn')
        return [[src], false];
    if (policy === 'whitespace')
        return [split_whitespace_separated_str(src, preserve_quotes), false];
    return split_quoted_str(src, dlm, preserve_quotes);
}



module.exports.split_quoted_str = split_quoted_str;
module.exports.split_whitespace_separated_str = split_whitespace_separated_str;
module.exports.smart_split = smart_split;

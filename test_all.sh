#!/usr/bin/env bash

dir_name=$( basename "$PWD" )

if [ "$dir_name" != "RBQL" ] ; then
    echo "Error: This test must be run from RBQL dir. Exiting"
    exit 1
fi

die_if_error() {
    if [ $1 != 0 ]; then
        echo "One of the tests failed. Exiting"
        exit 1
    fi
}

cleanup_tmp_files() {
    rm tmp_out.csv 2> /dev/null
    rm random_tmp_table.txt 2> /dev/null
}

cleanup_tmp_files

python2 -m unittest test.test_csv_utils
die_if_error $?
python3 -m unittest test.test_csv_utils
die_if_error $?

python2 -m unittest test.test_rbql
die_if_error $?
python3 -m unittest test.test_rbql
die_if_error $?


PYTHONPATH=".:$PYTHONPATH" python test/test_csv_utils.py --create_random_csv_table random_tmp_table.txt

# FIXME test random_tmp_table.txt from js


py_rbql_version=$( python -m rbql --version )

has_node="yes"
node_version=$( node --version 2> /dev/null )
rc=$?
if [ "$rc" != 0 ] || [ -z "$node_version" ] ; then
    echo "WARNING! Node.js was not found. Skipping node unit tests"  1>&2
    has_node="no"
fi

if [ "$has_node" == "yes" ] ; then
    js_rbql_version=$( node rbql-js/cli_rbql.js --version )
    if [ "$py_rbql_version" != "$js_rbql_version" ] ; then
        echo "Error: version missmatch between rbql.py ($py_rbql_version) and rbql.js ($js_rbql_version)"  1>&2
        exit 1
    fi
    cd test

    node test_rbql.js --auto-rebuild-engine
    die_if_error $?

    node test_csv_utils.js --auto-rebuild-engine
    die_if_error $?

    cd ..
fi


md5sum_canonic=($( md5sum test/csv_files/canonic_result_4.tsv ))

md5sum_test=($(python -m rbql --delim TAB --query "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where 'Sci-Fi' in a7.split('|') and b2!='US' and int(a4) > 2010" < test/csv_files/movies.tsv | md5sum))
if [ "$md5sum_canonic" != "$md5sum_test" ] ; then
    echo "CLI Python test FAIL!"  1>&2
    exit 1
fi

printf "select select a1\nselect a1, nonexistent_func(a2)\nselect a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where 'Sci-Fi' in a7.split('|') and b2!='US' and int(a4) > 2010\n" | python -m rbql --delim '\t' --input test/csv_files/movies.tsv --output tmp_out.csv > /dev/null
md5sum_test=($(cat tmp_out.csv | md5sum))
if [ "$md5sum_canonic" != "$md5sum_test" ] ; then
    echo "Interactive CLI Python test FAIL!"  1>&2
    exit 1
fi

if [ "$has_node" == "yes" ] ; then
    md5sum_test=($( node ./rbql-js/cli_rbql.js --delim TAB --query "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where a7.split('|').includes('Sci-Fi') && b2!='US' && a4 > 2010" < test/csv_files/movies.tsv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ] ; then
        echo "CLI JS test FAIL!"  1>&2
        exit 1
    fi

    printf "select select a1\nselect a1, nonexistent_func(a2)\nselect a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where a7.split('|').includes('Sci-Fi') && b2!='US' && a4 > 2010\n" | node ./rbql-js/cli_rbql.js --input test/csv_files/movies.tsv --output tmp_out.csv --delim '\t' > /dev/null
    md5sum_test=($(cat tmp_out.csv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ] ; then
        echo "Interactive CLI JS test FAIL!"  1>&2
        exit 1
    fi
fi

cleanup_tmp_files

echo "Finished tests"

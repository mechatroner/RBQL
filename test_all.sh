#!/usr/bin/env bash

dir_name=$( basename "$PWD" )


if [ "$dir_name" != "RBQL" ] && [ "$dir_name" != "rbql_core" ]; then
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
    rm speed_test.csv 2> /dev/null
}


run_unit_tests="yes"
run_python_tests="yes"
run_node_tests="yes"
has_python2="yes"
has_python3="yes"


py2_version=$( python2 --version 2>&1 )
rc=$?
if [ "$rc" != 0 ] || [ -z "$py2_version" ]; then
    echo "WARNING! python2 was not found"  1>&2
    has_python2="no"
fi

py3_version=$( python3 --version 2> /dev/null )
rc=$?
if [ "$rc" != 0 ] || [ -z "$py3_version" ]; then
    echo "WARNING! python3 was not found"  1>&2
    has_python3="no"
fi

if [ $has_python2 = "yes" ] && [ $has_python3 = "yes" ]; then
    rand_val=$[ $RANDOM % 2 ]
    if [ $rand_val = 1 ]; then
        random_python_interpreter="python3"
    else
        random_python_interpreter="python2"
    fi
elif [ $has_python2 = "yes" ]; then
    random_python_interpreter="python2"
elif [ $has_python3 = "yes" ]; then
    random_python_interpreter="python3"
else
    echo "WARNING! python was not found. Skipping python tests"  1>&2
    run_python_tests="no"
fi

echo "Random python interpreter to use: $random_python_interpreter"


while [[ $# -gt 0 ]]; do
    key="$1"
    case "$key" in
        --skip_unit_tests)
        run_unit_tests="no"
        ;;
        --skip_node_tests)
        run_node_tests="no"
        ;;
        --skip_python_tests)
        run_python_tests="no"
        ;;
        *)
        echo "Unknown option '$key'"
        exit 1
        ;;
    esac
    shift
done


cleanup_tmp_files

py_rbql_version=$( python -m rbql --version )


if [ $run_node_tests == "yes" ]; then
    node_version=$( node --version 2> /dev/null )
    rc=$?
    if [ "$rc" != 0 ] || [ -z "$node_version" ]; then
        echo "WARNING! Node.js was not found. Skipping node unit tests"  1>&2
        run_node_tests="no"
    fi
fi


PYTHONPATH=".:$PYTHONPATH" python test/test_csv_utils.py --create_big_csv_table speed_test.csv


if [ $run_unit_tests == "yes" ]; then
    if [ "$run_python_tests" == "yes" ]; then
        python2 -m unittest test.test_csv_utils
        die_if_error $?
        python3 -m unittest test.test_csv_utils
        die_if_error $?

        python2 -m unittest test.test_rbql
        die_if_error $?
        python3 -m unittest test.test_rbql
        die_if_error $?

        python2 -m unittest test.test_mad_max
        die_if_error $?
        python3 -m unittest test.test_mad_max
        die_if_error $?
    fi

    PYTHONPATH=".:$PYTHONPATH" python test/test_csv_utils.py --create_random_csv_table random_tmp_table.txt

    if [ "$run_node_tests" == "yes" ]; then
        node rbql-js/build_engine.js
        js_rbql_version=$( node rbql-js/cli_rbql.js --version )
        if [ "$py_rbql_version" != "$js_rbql_version" ]; then
            echo "Error: version missmatch between rbql.py ($py_rbql_version) and rbql.js ($js_rbql_version)"  1>&2
            exit 1
        fi
        cd test

        node test_csv_utils.js --run-random-csv-mode ../random_tmp_table.txt
        die_if_error $?

        node test_rbql.js
        die_if_error $?

        node test_csv_utils.js
        die_if_error $?

        cd ..
    fi
fi


md5sum_canonic="bdb725416a7b17e64034e0a128c6bb96"
# Testing unicode separators
if [ "$run_python_tests" == "yes" ]; then
    md5sum_test=($(python3 -m rbql --query 'select a2, a1' --delim $(echo -e "\u2063") --policy simple --input test/csv_files/invisible_separator_u2063.txt --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "python3 unicode separator test FAIL!"  1>&2
        exit 1
    fi
    md5sum_test=($(python2 -m rbql --query 'select a2, a1' --delim $(echo -e "\u2063") --policy simple --input test/csv_files/invisible_separator_u2063.txt --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "python2 unicode separator test FAIL!"  1>&2
        exit 1
    fi
fi
if [ "$run_node_tests" == "yes" ]; then
    md5sum_test=($( node ./rbql-js/cli_rbql.js --query 'select a2, a1' --delim $(echo -e "\u2063") --policy simple --input test/csv_files/invisible_separator_u2063.txt --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "node unicode separator test FAIL!"  1>&2
        exit 1
    fi
fi


# Testing unicode queries
md5sum_canonic="e1fe4bd13b25b2696e3df2623cd0f134"
if [ "$run_python_tests" == "yes" ]; then
    md5sum_test=($(python3 -m rbql --query "select a2, '$(echo -e "\u041f\u0440\u0438\u0432\u0435\u0442")' + ' ' + a1" --delim TAB --policy simple --input test/csv_files/movies.tsv --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "python3 unicode query test FAIL!"  1>&2
        exit 1
    fi
    md5sum_test=($(python2 -m rbql --query "select a2, '$(echo -e "\u041f\u0440\u0438\u0432\u0435\u0442")' + ' ' + a1" --delim TAB --policy simple --input test/csv_files/movies.tsv --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "python3 unicode query test FAIL!"  1>&2
        exit 1
    fi
fi
if [ "$run_node_tests" == "yes" ]; then
    md5sum_test=($(node ./rbql-js/cli_rbql.js --query "select a2, '$(echo -e "\u041f\u0440\u0438\u0432\u0435\u0442")' + ' ' + a1" --delim TAB --policy simple --input test/csv_files/movies.tsv --encoding utf-8 | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "node unicode query test FAIL!"  1>&2
        exit 1
    fi
fi


if [ "$run_python_tests" == "yes" ]; then
    expected_warning="Warning: Number of fields in \"input\" table is not consistent: e.g. record 1 -> 8 fields, record 3 -> 6 fields"
    actual_warning=$( python3 -m rbql --input test/csv_files/movies_variable_width.tsv --delim TAB --policy simple --query 'select a1, a2' 2>&1 1> /dev/null )
    if [ "$expected_warning" != "$actual_warning" ]; then
        echo "expected_warning= '$expected_warning' != '$actual_warning' = actual_warning"  1>&2
        exit 1
    fi
fi

if [ "$run_node_tests" == "yes" ]; then
    expected_warning="Warning: Number of fields in \"input\" table is not consistent: e.g. record 1 -> 8 fields, record 3 -> 6 fields"
    actual_warning=$( node rbql-js/cli_rbql.js --input test/csv_files/movies_variable_width.tsv --delim TAB --policy simple --query 'select a1, a2' 2>&1 1> /dev/null )
    if [ "$expected_warning" != "$actual_warning" ]; then
        echo "expected_warning= '$expected_warning' != '$actual_warning' = actual_warning"  1>&2
        exit 1
    fi
fi


if [ "$run_python_tests" == "yes" ]; then
    expected_error="Error [query execution]: At record 1, Details: name 'unknown_func' is not defined"
    actual_error=$( python3 -m rbql --input test/csv_files/countries.csv --query 'select top 10 unknown_func(a1)' --delim , --policy quoted 2>&1 )
    if [ "$expected_error" != "$actual_error" ]; then
        echo "expected_error = '$expected_error' != '$actual_error' = actual_error"  1>&2
        exit 1
    fi
fi

if [ "$run_node_tests" == "yes" ]; then
    expected_error="Error [query execution]: At record 1, Details: unknown_func is not defined"
    actual_error=$( node rbql-js/cli_rbql.js --input test/csv_files/countries.csv --query 'select top 10 unknown_func(a1)' --delim , --policy quoted 2>&1 )
    if [ "$expected_error" != "$actual_error" ]; then
        echo "expected_error = '$expected_error' != '$actual_error' = actual_error"  1>&2
        exit 1
    fi
fi


# Testing skip-header / named columns in CLI
md5sum_canonic=($( md5sum test/csv_files/canonic_result_14.csv ))


if [ "$run_python_tests" == "yes" ]; then
    md5sum_test=($($random_python_interpreter -m rbql --input ~/wsl_share/rainbow_tables/countries.csv --query "select top 5 a.Country, a['GDP per capita'] order by int(a['GDP per capita']) desc" --delim , --skip-header | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "CLI Python skip-header test FAIL!"  1>&2
        exit 1
    fi
fi


if [ "$run_node_tests" == "yes" ]; then
    # Using `NaN || 1000 * 1000` trick below to return 1M on NaN and make sure that --skip-header works. Otherwise the header line would be the max
    md5sum_test=($( node ./rbql-js/cli_rbql.js --input ~/wsl_share/rainbow_tables/countries.csv --query "select top 5 a.Country, a['GDP per capita'] order by parseInt(a['GDP per capita']) || 1000 * 1000 desc" --delim , --skip-header | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "CLI JS skip-header test FAIL!"  1>&2
        exit 1
    fi
fi



# Testing CLI errors and warnings

output=$( $random_python_interpreter -m rbql --delim , --query "SELECT top 3 a1, foobarium(a2)" --input test/csv_files/countries.csv 2>&1 )
rc=$?
if [ $rc != 1 ] || [[ $output != *"name 'foobarium' is not defined"* ]]; then
    echo "rbql-py does not produce expected error. rc:$rc, output:$output "  1>&2
    exit 1
fi

output=$( $random_python_interpreter -m rbql --delim , --query "SELECT top 3 a1, None, a2" --input test/csv_files/countries.csv 2>&1 > /dev/null )
rc=$?
if [ $rc != 0 ] || [[ $output != *"Warning: None values in output were replaced by empty strings"* ]]; then
    echo "rbql-py does not produce expected warning. rc:$rc, output:$output "  1>&2
    exit 1
fi


output=$( node ./rbql-js/cli_rbql.js --delim , --query "SELECT top 3 a1, foobarium(a2)" --input test/csv_files/countries.csv 2>&1 )
rc=$?
if [ $rc != 1 ] || [[ $output != *"foobarium is not defined"* ]]; then
    echo "rbql-js does not produce expected error. rc:$rc, output:$output "  1>&2
    exit 1
fi

output=$( node ./rbql-js/cli_rbql.js --delim , --query "SELECT top 3 a1, null, a2" --input test/csv_files/countries.csv 2>&1 > /dev/null )
rc=$?
if [ $rc != 0 ] || [[ $output != *"Warning: null values in output were replaced by empty strings"* ]]; then
    echo "rbql-js does not produce expected warning. rc:$rc, output:$output "  1>&2
    exit 1
fi



# Testing performance


if [ "$run_python_tests" == "yes" ]; then
    start_tm=$(date +%s.%N)
    python3 -m rbql --input speed_test.csv --delim , --policy quoted --query 'select a2, a1, a2, NR where int(a1) % 2 == 0' > /dev/null
    end_tm=$(date +%s.%N)
    elapsed=$( echo "$start_tm,$end_tm" | python -m rbql --delim , --query 'select float(a2) - float(a1)' )
    echo "Python simple select query took $elapsed seconds. Reference value: 3 seconds"
fi

if [ "$run_node_tests" == "yes" ]; then
    start_tm=$(date +%s.%N)
    node ./rbql-js/cli_rbql.js --input speed_test.csv --delim , --policy quoted --query 'select a2, a1, a2, NR where parseInt(a1) % 2 == 0' > /dev/null
    end_tm=$(date +%s.%N)
    elapsed=$( echo "$start_tm,$end_tm" | python -m rbql --delim , --query 'select float(a2) - float(a1)' )
    echo "JS simple select query took $elapsed seconds. Reference value: 2.3 seconds"
fi

if [ "$run_python_tests" == "yes" ]; then
    start_tm=$(date +%s.%N)
    python3 -m rbql --input speed_test.csv --delim , --policy quoted --query 'select max(a1), count(*), a2 where int(a1) > 15 group by a2' > /dev/null
    end_tm=$(date +%s.%N)
    elapsed=$( echo "$start_tm,$end_tm" | python -m rbql --delim , --query 'select float(a2) - float(a1)' )
    echo "Python GROUP BY query took $elapsed seconds. Reference value: 2.6 seconds"
fi

if [ "$run_node_tests" == "yes" ]; then
    start_tm=$(date +%s.%N)
    node ./rbql-js/cli_rbql.js --input speed_test.csv --delim , --policy quoted --query 'select max(a1), count(*), a2 where parseInt(a1) > 15 group by a2' > /dev/null
    end_tm=$(date +%s.%N)
    elapsed=$( echo "$start_tm,$end_tm" | python -m rbql --delim , --query 'select float(a2) - float(a1)' )
    echo "JS GROUP BY query took $elapsed seconds. Reference value: 1.1 seconds"
fi



# Testing generic CLI
md5sum_canonic=($( md5sum test/csv_files/canonic_result_4.tsv ))

if [ "$run_python_tests" == "yes" ]; then
    md5sum_test=($($random_python_interpreter -m rbql --delim TAB --query "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where 'Sci-Fi' in a7.split('|') and b2!='US' and int(a4) > 2010" < test/csv_files/movies.tsv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "CLI Python test FAIL!"  1>&2
        exit 1
    fi

    # XXX theorethically this test can randomly fail because sleep timeout is not long enough
    (echo "select select a1" && sleep 0.5 && echo "select a1, nonexistent_func(a2)" && sleep 0.5 && echo "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where 'Sci-Fi' in a7.split('|') and b2!='US' and int(a4) > 2010") | $random_python_interpreter -m rbql --delim '\t' --input test/csv_files/movies.tsv --output tmp_out.csv > /dev/null
    md5sum_test=($(cat tmp_out.csv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "Interactive CLI Python test FAIL!"  1>&2
        exit 1
    fi
fi


if [ "$run_node_tests" == "yes" ]; then
    md5sum_test=($( node ./rbql-js/cli_rbql.js --delim TAB --query "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where a7.split('|').includes('Sci-Fi') && b2!='US' && a4 > 2010" < test/csv_files/movies.tsv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "CLI JS test FAIL!"  1>&2
        exit 1
    fi

    # XXX theorethically this test can randomly fail because sleep timeout is not long enough
    (echo "select select a1" && sleep 0.5 && echo "select a1, nonexistent_func(a2)" && sleep 0.5 && echo "select a1,a2,a7,b2,b3,b4 left join test/csv_files/countries.tsv on a2 == b1 where a7.split('|').includes('Sci-Fi') && b2!='US' && a4 > 2010") | node ./rbql-js/cli_rbql.js --input test/csv_files/movies.tsv --output tmp_out.csv --delim '\t' > /dev/null
    md5sum_test=($(cat tmp_out.csv | md5sum))
    if [ "$md5sum_canonic" != "$md5sum_test" ]; then
        echo "Interactive CLI JS test FAIL!"  1>&2
        exit 1
    fi
fi

cleanup_tmp_files

echo "Finished tests"

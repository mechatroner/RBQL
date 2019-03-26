#!/usr/bin/env bash

die_if_error() {
    if [ $1 != 0 ]; then
        echo "One of the tests failed. Exiting"
        exit 1
    fi
}


python2 -m unittest test.test_csv_utils
die_if_error $?

python3 -m unittest test.test_csv_utils
die_if_error $?

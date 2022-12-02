#!/usr/bin/env bash

# In order to publish rbql-py you need to run these two magical commands:

# TODO Make sure that versions in setup.py and rbql/_version.py are equal (To update version globally see DEV_README.md)!
# TODO Make sure that external README.md is in sync with python README.md in this dir.

git clean -fd
python3 setup.py sdist bdist_wheel
twine upload dist/*

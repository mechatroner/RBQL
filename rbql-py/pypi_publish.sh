#!/usr/bin/env bash

# In order to publish rbql-py you need to run these two magical commands:

# FIXME make sure that versions in setup.py and rbql/_version.py are equal!
git clean -fd
python3 setup.py sdist bdist_wheel
twine upload dist/*

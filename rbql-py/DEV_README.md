# Publishing

* Make sure that versions in setup.py and rbql/_version.py are equal (To update version globally see DEV_README.md)!
* Make sure that external README.md is in sync with python README.md in this dir.

1. Run `git clean -fd`
2. Run the 2 magic commands below:
```
python3 setup.py sdist bdist_wheel
twine upload dist/*
```

#### Note: 
Under WSL the pre-publishing command `python3 setup.py sdist bdist_wheel` may fail with a permission error. In this case follow steps described here:
https://github.com/pypa/packaging-problems/issues/258

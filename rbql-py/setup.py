"""setup.py: setuptools control."""
 
import re
from setuptools import setup
 
 
version = '0.27.0'
 
 
with open("README.md", "rb") as f:
    long_descr = f.read().decode("utf-8")
 
 
setup(
    name = "rbql",
    packages = ["rbql"],
    entry_points = {
        "console_scripts": ['rbql-py = rbql.rbql_main:main', 'rbql = rbql.rbql_main:main']
        },
    version = version,
    description = "Rainbow Query Language",
    long_description = long_descr,
    long_description_content_type = 'text/markdown',
    author = "Dmitry Ignatovich",
    author_email = "mechatroner@yandex.ru",
    url = "https://github.com/mechatroner/RBQL",
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Framework :: IPython",
    ],
    keywords = "SQL SQL-like transpiler RBQL library CLI command-line CSV TSV IPython Jupyter",
    include_package_data = True,
    )

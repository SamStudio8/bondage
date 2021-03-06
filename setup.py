#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

setuptools.setup(
    name="bondage",
    version="0.0.1",
    url="https://github.com/samstudio8/bondage",

    description="A command line tool for forcing files to stick together",
    long_description="",

    author="Sam Nicholls",
    author_email="sam@samnicholls.net",

    maintainer="Sam Nicholls",
    maintainer_email="sam@samnicholls.net",

    packages=setuptools.find_packages(),
    include_package_data=True,

    entry_points = {
        "console_scripts": [
            "bond=bondage:cli",
        ]
    },

    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: MIT License',
    ],

)

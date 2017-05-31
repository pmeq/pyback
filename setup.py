#!/usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup

setup(
    name='pyback',
    version='1.0',
    description='backup tools for hdfs',
    author='SunWenhui',
    license='No License',
    platforms='linux',
    py_modules=[
        'pyback.HdfsUtil',
        'pyback.SysUtil',
        'pyback.PyBack'
    ],
    scripts=[
        "script/pyback",
    ],
    install_requires=[
        'pyhdfs'
    ],
    data_files=[
        ('/export/servers/conf', ['config/hdfs.cfg', ]),
    ]
)

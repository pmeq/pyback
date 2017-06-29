#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from distutils.core import setup
from distutils.command.install import install

script_file = 'script/pyback'


class MyInstall(install):
    def run(self):
        self.change_path()
        install.run(self)

    def change_path(self):
        python_executable = sys.executable

        # change the python file path
        with open(script_file, 'r+') as fh:
            d = fh.read().replace('/usr/bin/env python', python_executable)
            fh.seek(0)
            fh.truncate()
            fh.write(d)

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
        script_file,
    ],
    install_requires=[
        'pyhdfs'
    ],
    data_files=[
        ('/export/servers/conf', ['config/hdfs.cfg', ]),
    ],
    cmdclass={
        'install': MyInstall,
    },
)

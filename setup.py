#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import glob
import os
import sys
from distutils.sysconfig import get_python_lib
from setuptools import setup, find_packages
from setuptools.command.develop import develop
from setuptools.command.install import install
from distutils.command.install_data import install_data
from subprocess import check_call

sys.path.insert(0, os.path.abspath('lib/'))

from ess import version  # noqa


def get_reqs_from_file(requirements_file):
    if os.path.exists(requirements_file):
        return open(requirements_file, 'r').read().split('\n')
    return []


def parse_requirements(requirements_files):
    requirements = []
    for requirements_file in requirements_files:
        for line in get_reqs_from_file(requirements_file):
            line = line.split('#')[0]
            line = line.strip()
            if len(line):
                requirements.append(line)
    return requirements


def replace_python_lib_path(conf_files, python_lib_path):
    for conf_file in conf_files:
        new_file = conf_file.replace('.template', '.temp')
        with open(conf_file, 'r') as f:
            template = f.read()
        template = template.format(python_site_packages_path=python_lib_path)
        with open(new_file, 'w') as f:
            f.write(template)


class PostDevelopCommand(develop):
    """Post-installation for development mode."""
    def run(self):
        # check_call("apt-get install this-package".split())
        develop.run(self)


class PostInstallCommand(install):
    """Post-installation for installation mode."""
    def run(self):
        # check_call("apt-get install this-package".split())
        install.run(self)


# get release version
release_version = version.release_version

install_lib_path = get_python_lib()
rest_conf_files = ['etc/ess/rest/aliases-py27.conf.template',
                   'etc/ess/rest/httpd-ess-443-py27-cc7.conf.template']
replace_python_lib_path(rest_conf_files, install_lib_path)

requirements_files = ['tools/venv/pip-requires']
install_requires = parse_requirements(requirements_files=requirements_files)
extras_requires = dict(mysql=['mysqlclient==1.4.1'],
                       dev=parse_requirements(requirements_files=['tools/venv/pip-requires-test']))
data_files = [
    # config and cron files
    ('etc/ess', glob.glob('etc/ess/*.template')),
    ('etc/ess/rest', glob.glob('etc/ess/rest/*.temp*')),
    ('etc/ess/tools', glob.glob('tools/*.py') + glob.glob('tools/*.sh')),
    ('etc/ess/tools/orm', glob.glob('tools/orm/*')),
    ('etc/ess/tools/venv', glob.glob('tools/venv/*')),
]
scripts = glob.glob('bin/*')

s = setup(
    name="ess",
    version=release_version,
    description='Event Stream Service Package',
    long_description='''This package contains Event Stream Service components''',
    license='GPL',
    author='Panda Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://github.com/PanDAWMS/ess/wiki',
    python_requires='>=2.7',
    packages=find_packages('lib/'),
    package_dir={'': 'lib'},
    install_requires=install_requires,
    extras_require=extras_requires,
    data_files=data_files,
    scripts=scripts,
    cmdclass={
        'develop': PostDevelopCommand,
        'install': PostInstallCommand,
    },
)

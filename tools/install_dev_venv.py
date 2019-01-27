#!/usr/bin/env python
# Copyright European Organization for Nuclear Research (CERN)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import errno
import optparse
import os
import subprocess
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
PIP_REQUIRES = os.path.join(ROOT, 'tools/git', 'pip-requires')
PIP_REQUIRES_TEST = os.path.join(ROOT, 'tools/git', 'pip-requires-test')
VENV = os.path.join(ROOT, '.venv')


def die(message, *args):
    print >> sys.stderr, message % args
    sys.exit(1)


def run_command(cmd, redirect_output=True, check_exit_code=True, shell=False):
    """
    Runs a command in an out-of-process shell, returning the
    output of that command.  Working directory is ROOT.
    """
    if redirect_output:
        stdout = subprocess.PIPE
    else:
        stdout = None

    proc = subprocess.Popen(cmd, cwd=ROOT, stdout=stdout, shell=shell)
    output = proc.communicate()[0]
    if check_exit_code and proc.returncode != 0:
        die('Command "%s" failed.\n%s', ' '.join(cmd), output)
    return output


HAS_VENV = bool(run_command(['which', 'virtualenv'], check_exit_code=False).strip())


def configure_git():
    """
    Configure git to add git hooks
    """
    print "Configure git"
    run_command("%s/tools/git/configure_git.sh" % ROOT, shell=True)


def create_virtualenv(venv=VENV):
    """
    Creates the virtual environment
    """
    if HAS_VENV:
        print 'Creating venv...'
        run_command(['virtualenv', '-q', '--no-site-packages', venv])
    else:
        die("ERROR: failed to create virtual environment, command virtualenv is not found.")


def install_dependencies(venv=VENV):
    """
    Install dependencies packages through pip
    """
    run_command(['%s/bin/pip' % (venv), 'install', '-r', PIP_REQUIRES], redirect_output=False)
    run_command(['%s/bin/pip' % (venv), 'install', '-r', PIP_REQUIRES_TEST], redirect_output=False)

    py_ver = _detect_python_version(venv)
    pthfile = os.path.join(venv, "lib", py_ver, "site-packages", "ess.pth")
    f = open(pthfile, 'w')
    f.write("%s/lib/\n" % ROOT)
    f.close()


def _detect_python_version(venv):
    lib_dir = os.path.join(venv, "lib")
    for pathname in os.listdir(lib_dir):
        if pathname.startswith('python'):
            return pathname
    raise Exception('Unable to detect Python version')


def print_help():
    help = """
ESS development environment setup is complete.

To enable ESS dev environment by running:

$ source tools/setup_dev.sh
"""
    print help


def main():
    print "Configuring git"
    configure_git()

    print "Installing venv"
    create_virtualenv()

    print "Installing dependencies via pip"
    install_dependencies()
    print_help()

if __name__ == "__main__":
    main()

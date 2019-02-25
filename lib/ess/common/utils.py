#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import datetime
import logging
import os
import subprocess
import sys

from ess.common.config import config_has_section, config_has_option, config_get


# RFC 1123
DATE_FORMAT = '%a, %d %b %Y %H:%M:%S UTC'


def setup_logging(name):
    """
    Setup logging
    """
    if config_has_section('common') and config_has_option('common', 'loglevel'):
        loglevel = getattr(logging, config_get('common', 'loglevel').upper())
    else:
        loglevel = logging.INFO

    if config_has_section('common') and config_has_option('common', 'logdir'):
        logging.basicConfig(filename=os.path.join(config_get('common', 'logdir'), name),
                            level=loglevel,
                            format='%(asctime)s\t%(processName)s\t%(process)d\t%(levelname)s\t%(message)s')
    else:
        logging.basicConfig(stream=sys.stdout, level=loglevel,
                            format='%(asctime)s\t%(processName)s\t%(process)d\t%(levelname)s\t%(message)s')


def str_to_date(string):
    """
    Converts a string to the corresponding datetime value.

    :param string: the string to convert to datetime value.
    """
    return datetime.datetime.strptime(string, DATE_FORMAT) if string else None


def date_to_str(date):
    """
    Converts a datetime value to a string.

    :param date: the datetime value to convert.
    """
    return datetime.datetime.strftime(date, DATE_FORMAT) if date else None


def check_rest_host():
    """
    Function to check whether rest host is defined in config.
    To be used to decide whether to skip some test functions.

    :returns True: if rest host is available. Otherwise False.
    """
    if config_has_option('rest', 'host'):
        host = config_get('rest', 'host')
        if host:
            return True
    return False


def get_rest_host():
    """
    Function to get rest host
    """
    return config_get('rest', 'host')


def run_process(cmd, stdout=None, stderr=None):
    """
    Runs a command in an out-of-process shell.
    """
    if stdout and stderr:
        process = subprocess.Popen(cmd, shell=True, stdout=stdout, stderr=stderr, preexec_fn=os.setsid)
    else:
        process = subprocess.Popen(cmd, shell=True)
    return process

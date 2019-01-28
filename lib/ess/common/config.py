#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019

"""
Configurations.

configuration file looking for path:
    1. $ESS_HOME/etc/ess.cfg
    2. /etc/ess/ess.cfg
    3. $VIRTUAL_ENV/etc/ess.cfg
"""


import logging
import os

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser


def config_get(section, option):
    """
    Return the string value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the configuration value.
    """
    return __CONFIG.get(section, option)


def config_get_int(section, option):
    """
    Return the integer value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the integer configuration value.
    """
    return __CONFIG.getint(section, option)


def config_get_float(section, option):
    """
    Return the float value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the float configuration value.
    """
    return __CONFIG.getfloat(section, option)


def config_get_bool(section, option, raise_exception=True, default=None):
    """
    Return the boolean value for a given option in a section
    :param section: the named section.
    :param option: the named option.
.
    :returns: the boolean configuration value.
    """
    return __CONFIG.getboolean(section, option)


__CONFIG = ConfigParser.SafeConfigParser(os.environ)

__HAS_CONFIG = False
if os.environ.get('ESS_CONFIG', None):
    configfile = os.environ['ESS_CONFIG']
    if not __CONFIG.read(configfile) == [configfile]:
        raise Exception('ESS_CONFIG is defined as %s, ' % configfile,
                        'but could not load configurations from it.')
    __HAS_CONFIG = True
else:
    configfiles = ['%s/etc/ess/ess.cfg' % os.environ['ESS_HOME'],
                   '/etc/ess/ess.cfg',
                   '%s/etc/ess/ess.cfg' % os.environ['VIRTUAL_ENV']]

    for configfile in configfiles:
        if __CONFIG.read(configfile) == [configfile]:
            __HAS_CONFIG = True
            logging.info("Configuration file %s is used" % configfile)
            break

if not __HAS_CONFIG:
    raise Exception("Could not load configuration file."
                    "ESS looks for a configuration file, in order:"
                    "\n\t${ESS_CONFIG}"
                    "\n\t${ESS_HOME}/etc/ess/ess.cfg"
                    "\n\t/etc/ess/ess.cfg"
                    "\n\t${VIRTUAL_ENV}/etc/ess/ess.cfg")

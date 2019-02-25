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
Main start entry point for ESS service
"""


import logging

from ess.common.constants import Sections
from ess.common.config import config_has_section, config_has_option, config_list_options, config_get
from ess.common.utils import setup_logging


setup_logging(__name__)


DAEMONS = {
    'basedaemon': ['ess.daemons.common.basedaemon.BaseDaemon', Sections.BaseDaemon],
    'resourcemanager': ['ess.daemons.resourcemanager.daemon.ResourceManager', Sections.ResourceManager],
    'broker': ['ess.daemons.broker.daemon.Broker', Sections.Broker],
    'assigner': ['ess.daemons.assigner.daemon.Assigner', Sections.Assigner],
    'precacher': ['ess.daemons.precacher.daemon.PreCacher', Sections.PreCacher],
    'splitter': ['ess.daemons.splitter.daemon.Splitter', Sections.Splitter]
}
RUNNING_DAEMONS = []


def load_config_daemons():
    if config_has_section(Sections.Main) and config_has_option(Sections.Main, 'daemons'):
        daemons = config_get(Sections.Main, 'daemons')
        daemons = daemons.split(',')
        daemons = [d.strip() for d in daemons]
        return daemons
    return []


def load_daemon_attrs(section):
    """
    Load daemon attributes
    """
    attrs = {}
    logging.info("Loading config for section: %s" % section)
    if config_has_section(section):
        options = config_list_options(section)
        for option, value in options:
            if not option.startswith('plugin.'):
                if isinstance(value, str) and value.lower() == 'true':
                    value = True
                if isinstance(value, str) and value.lower() == 'false':
                    value = False
                attrs[option] = value
    return attrs


def load_daemon(daemon):
    if daemon not in DAEMONS.keys():
        logging.critical("Configured daemon %s is not supported." % daemon)
        raise Exception("Configured daemon %s is not supported." % daemon)

    daemon_cls, daemon_section = DAEMONS[daemon]
    attrs = load_daemon_attrs(daemon_section)
    logging.info("Loading daemon %s with class %s and attributes %s" % (daemon, daemon_cls, str(attrs)))

    k = daemon_cls.rfind('.')
    daemon_modules = daemon_cls[:k]
    daemon_class = daemon_cls[k + 1:]
    module = __import__(daemon_modules, fromlist=[None])
    cls = getattr(module, daemon_class)
    impl = cls(**attrs)
    return impl


def run_daemons():
    global RUNNING_DAEMONS

    daemons = load_config_daemons()
    logging.info("Configured to run daemons: %s" % str(daemons))
    for daemon in daemons:
        daemon_thr = load_daemon(daemon)
        RUNNING_DAEMONS.append(daemon_thr)

    for daemon in RUNNING_DAEMONS:
        daemon.start()

    while len(RUNNING_DAEMONS):
        RUNNING_DAEMONS = [thr.join(timeout=3.14) for thr in RUNNING_DAEMONS if thr and thr.is_alive()]


def stop():
    global RUNNING_DAEMONS

    [thr.terminate() for thr in RUNNING_DAEMONS if thr and thr.is_alive()]


if __name__ == '__main__':
    try:
        run_daemons()
    except KeyboardInterrupt:
        stop()

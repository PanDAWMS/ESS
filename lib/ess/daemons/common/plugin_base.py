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
Class of base plugin
"""

import logging


class PluginBase(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(key, kwargs[key])

        self.logger = None
        self.setup_logger()

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.__class__.__name__)

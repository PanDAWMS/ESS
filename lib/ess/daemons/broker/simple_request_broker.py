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
Simple request broker plugin
"""


from ess.daemons.common.plugin_base import PluginBase


class SimpleRequestBroker(PluginBase):
    def __init__(self, **kwargs):
        super(SimpleRequestBroker, self).__init__(**kwargs)

        self.setup_logger()

    def broker_request(self, req, collection, edges):
        """
        Find an edge server to broker the request.
        """
        return edges[0]

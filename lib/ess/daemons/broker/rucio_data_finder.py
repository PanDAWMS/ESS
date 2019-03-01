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
Rucio data finder plugin
"""


from rucio.client.client import Client

from ess.daemons.common.plugin_base import PluginBase


class RucioDataFinder(PluginBase):
    def __init__(self, **kwargs):
        super(RucioDataFinder, self).__init__(**kwargs)

        self.setup_logger()

    def find_dataset(self, scope, name):
        """
        Find the dataset/container with scope:name.
        """
        d = Client()
        info = d.get_did(scope, name)
        ret = {'collection_type': info['type'],
               'size': info['bytes'],
               'total_files': info['length']}

        if info['type'] in ['FILE']:
            ret['status'] = 'AVAILABLE'
        else:
            ret['status'] = 'OPEN' if info['open'] else 'CLOSED'

        return ret

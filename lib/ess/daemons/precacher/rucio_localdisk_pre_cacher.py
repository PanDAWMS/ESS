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
Rucio pre-cacher plugin
"""


from rucio.client.client import Client
from rucio.client.downloadclient import DownloadClient

from ess.daemons.common.plugin_base import PluginBase
from ess.orm.constants import ContentStatus


class RucioPreCacher(PluginBase):
    def __init__(self, **kwargs):
        super(RucioPreCacher, self).__init__(**kwargs)

        self.setup_logger()

        if not hasattr(self, 'cache_path'):
            self.cache_path = None
        if not hasattr(self, 'no_subdir'):
            self.no_subdir = False
        if not hasattr(self, 'transfer_timeout'):
            self.transfer_timeout = None
        if not hasattr(self, 'rse'):
            self.rse = None
        if not hasattr(self, 'num_threads'):
            self.num_threads = 1

    def pre_cache(self, scope, name):
        """
        Pre cache the dataset to this edge.
        The edge should define a path or a storage for pre caching.
        """
        item = {'did': '%s:%s' % (scope, name),
                'base_dir': self.cache_path,
                'no_subdir': self.no_subdir,
                'transfer_timeout': self.transfer_timeout}
        if self.rse:
            item['rse'] = self.rse

        client = Client()
        all_files = client.list_files(scope, name)

        download_client = DownloadClient(client=client)
        downloaded_files = download_client.download_dids([item], num_threads=self.num_threads)

        self.logger.info('Downloaded files: %s' % downloaded_files)

        ret_files = []
        for file in all_files:
            downloaded_file = None
            for d_file in downloaded_files:
                if d_file['scope'] == file['scope'] and d_file['name'] == file['name']:
                    downloaded_file = d_file
                    break

            ret_file = {'scope': file['scope'],
                        'name': file['name'],
                        'min_id': 1,
                        'max_id': file['events'],
                        'status': ContentStatus.AVAILABLE if downloaded_file and downloaded_file['clientState'] == 'ALREADY_DONE' else ContentStatus.NEW,
                        'size': file['bytes'],
                        'md5': downloaded_file['md5'] if downloaded_file else None,
                        'adler32': downloaded_file['adler32'] if downloaded_file else None,
                        'pfn': downloaded_file['dest_file_path'] if downloaded_file else None
                        }
            ret_files.append(ret_file)
        return ret_files

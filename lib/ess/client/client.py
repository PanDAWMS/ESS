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
Main client class for ESS Rest callings.
"""


import os
import urllib
import warnings

from ess.common import exceptions
from ess.client.catalogclient import CatalogClient
from ess.client.edgeclient import EdgeClient
from ess.client.requestclient import RequestClient


warnings.filterwarnings("ignore")


class Client(CatalogClient, EdgeClient, RequestClient):

    """Main client class for ESS rest callings."""

    def __init__(self, host=None, timeout=600):
        """
        Constructor for the ESS main client class.

        :param host: the host of the ESS system.
        :param timeout: the timeout of the request (in seconds).
        """

        client_proxy = self.get_user_proxy()
        super(Client, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def get_user_proxy(sellf):
        """
        Get the user proxy.

        :returns: the path of the user proxy.
        """

        if 'X509_USER_PROXY' in os.environ:
            client_proxy = os.environ['X509_USER_PROXY']
        else:
            client_proxy = '/tmp/x509up_u%d' % os.geteuid()

        if not os.path.exists(client_proxy):
            raise exceptions.RestException("Cannot find a valid x509 proxy.")

    def download(self, scope, name, min_id, max_id, dest_dir=None):
        """
        To download a file or a partial file.
        """

        content = self.get_content(scope, name, min_id, max_id, status='AVAILABLE')
        if content:
            if content['content_type'] == 'FILE':
                filename = content['name']
            else:
                filename = '%s.%s-%s-%s-%s' % (content['name'], content['coll_id'], content['content_id'], content['min_id'], content['max_id'])

            if dest_dir:
                filename = os.path.join(dest_dir, filename)
            local_filename, ret_msg = urllib.urlretrieve(content['pfn'], filename)
            if os.path.getsize(local_filename) == content['pfn_size']:
                return {'status': 0, 'message': 'successfully downloded.',
                        'metadata': {'name': local_filename,
                                     'coll_id': content['coll_id'],
                                     'content_id': content['content_id'],
                                     'min_id': content['min_id'],
                                     'max_id': content['max_id']
                                     }
                        }
            else:
                os.remove(local_filename)
                return {'status': -1, 'message': 'File size mismatch, clean.'}
        else:
            return {'status': -1, 'message': 'No matched files.'}

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
Catalog Rest client to access ESS system.
"""

import os

from ess.client.base import BaseRestClient


class CatalogClient(BaseRestClient):

    """catalog Rest client"""

    CATALOG_BASEURL = 'catalog'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the ESS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(CatalogClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def get_collection(self, scope, name):
        """
        Get a collection from the Head service.

        :param scope: The scope of the collection.
        :param name: The name of the collection.

        :raise exceptions if it's not got successfully.
        """
        path = self.CATALOG_BASEURL

        url = self.build_url(self.host, path=os.path.join(path, 'collection/%s/%s' % (scope, name)))

        r = self.get_request_response(url, type='GET')
        return r

    def get_content(self, scope, name, min_id=None, max_id=None, status=None):
        """
        Get content of a file or a partial file from the Head service.

        :param scope: The scope of the file.
        :param name: The name of the file.
        :min_id: The min_id of the file.
        :max_id: The max_id of the file.

        :raise exceptions if it's not got successfully.
        """
        path = self.CATALOG_BASEURL

        params = {}
        if min_id:
            params['min_id'] = min_id
        if max_id:
            params['max_id'] = max_id
        if status:
            params['status'] = status

        url = self.build_url(self.host, path=os.path.join(path, 'content/%s/%s' % (scope, name)), params=params)

        r = self.get_request_response(url, type='GET')
        return r

    def add_contents(self, collection_scope, collection_name, edge_name, files):
        """
        Synchronize the contents to the Head service.

        :param collection_scope: The scope of the collection.
        :param collection_name: The name of the collection.
        :param edge_name: The edge name.

        :raise exceptions if it's not got successfully.
        """
        path = self.CATALOG_BASEURL

        url = self.build_url(self.host, path=os.path.join(path, 'contents/%s/%s/%s' % (collection_scope, collection_name, edge_name)))

        r = self.get_request_response(url, type='POST', data=files)
        return r

    def get_contents(self, collection_scope, collection_name, edge_name):
        """
        Synchronize the contents from the Head service.

        :param collection_scope: The scope of the collection.
        :param collection_name: The name of the collection.
        :param edge_name: The edge name.

        :raise exceptions if it's not got successfully.
        """
        path = self.CATALOG_BASEURL

        url = self.build_url(self.host, path=os.path.join(path, 'contents/%s/%s/%s' % (collection_scope, collection_name, edge_name)))

        r = self.get_request_response(url, type='GET')
        return r

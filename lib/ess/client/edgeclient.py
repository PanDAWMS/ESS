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
Edge Rest client to access ESS system.
"""

import os

from ess.client.base import BaseRestClient


class EdgeClient(BaseRestClient):

    """Edge Rest client"""

    EDGE_BASEURL = 'edges'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the ESS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(EdgeClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def register_edge(self, edge_name, **kwargs):
        """
        Register Edge to the Head service.

        :param edge_name: the edge name.
        :param kwargs: other attributes of the edge.

        :raise exceptions if it's not registerred successfully.
        """
        path = self.EDGE_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, edge_name))

        data = kwargs
        data['edge_name'] = edge_name

        r = self.get_request_response(url, type='POST', data=data)
        return r

    def update_edge(self, edge_name, **kwargs):
        """
        Update Edge to the Head service.

        :param edge_name: the edge name.
        :param kwargs: other attributes of the edge.

        :raise exceptions if it's not updated successfully.
        """
        path = self.EDGE_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, edge_name))

        data = kwargs
        data['edge_name'] = edge_name

        r = self.get_request_response(url, type='PUT', data=data)
        return r

    def get_edge(self, edge_name):
        """
        Get Edge from the Head service.

        :param edge_name: the edge name.

        :raise exceptions if it's not got successfully.
        """
        path = self.EDGE_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, edge_name))

        r = self.get_request_response(url, type='GET')
        return r

    def delete_edge(self, edge_name):
        """
        Delete Edge from the Head service.

        :param edge_name: the edge name.

        :raise exceptions if it's not deleted successfully.
        """
        path = self.EDGE_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, edge_name))

        r = self.get_request_response(url, type='DEL')
        return r

    def list_edges(self, **kwargs):
        """
        List edges.

        :raise exceptions if it's not successful.
        """
        path = self.EDGE_BASEURL
        params = kwargs
        url = self.build_url(self.host, path=path + '/', params=params)

        r = self.get_request_response(url, type='GET')
        return r

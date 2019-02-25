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
Request Rest client to access ESS system.
"""

import os

from ess.client.base import BaseRestClient


class RequestClient(BaseRestClient):

    """Request Rest client"""

    REQUEST_BASEURL = 'requests'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the ESS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(RequestClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

    def add_request(self, **kwargs):
        """
        Add request to the Head service.

        :param kwargs: attributes of the request.

        :raise exceptions if it's not registerred successfully.
        """
        path = self.REQUEST_BASEURL
        url = self.build_url(self.host, path=path + '/')

        data = kwargs

        r = self.get_request_response(url, type='POST', data=data)
        return r['request_id']

    def update_request(self, request_id, **kwargs):
        """
        Update Request to the Head service.

        :param request_id: the request.
        :param kwargs: other attributes of the request.

        :raise exceptions if it's not updated successfully.
        """
        path = self.REQUEST_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))

        data = kwargs
        data['request_id'] = request_id

        r = self.get_request_response(url, type='PUT', data=data)
        return r

    def get_request(self, request_id):
        """
        Get request from the Head service.

        :param request_id: the request id.

        :raise exceptions if it's not got successfully.
        """
        path = self.REQUEST_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))

        r = self.get_request_response(url, type='GET')
        return r

    def delete_request(self, request_id):
        """
        Delete request from the Head service.

        :param request_id: the request id.

        :raise exceptions if it's not deleted successfully.
        """
        path = self.REQUEST_BASEURL
        url = self.build_url(self.host, path=os.path.join(path, str(request_id)))

        r = self.get_request_response(url, type='DEL')
        return r

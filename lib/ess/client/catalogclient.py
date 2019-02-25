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


from ess.client.base import BaseRestClient


class CatalogClient(BaseRestClient):

    """Edge Rest client"""

    EDGE_BASEURL = 'edges'

    def __init__(self, host=None, client_proxy=None, timeout=None):
        """
        Constructor of the BaseRestClient.

        :param host: the address of the ESS server.
        :param client_proxy: the client certificate proxy.
        :param timeout: timeout in seconds.
        """
        super(CatalogClient, self).__init__(host=host, client_proxy=client_proxy, timeout=timeout)

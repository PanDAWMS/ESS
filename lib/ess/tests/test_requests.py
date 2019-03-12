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
Test Request.
"""

import json

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises

from ess.client.client import Client
from ess.common import exceptions
from ess.common.utils import check_rest_host, get_rest_host, check_database, check_user_proxy, has_config
from ess.core.requests import add_request, get_request, update_request, delete_request
from ess.orm.types import GUID


class TestRequest(unittest.TestCase):

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_database(), "Database is not defined")
    def test_create_and_check_for_request_core(self):
        """ Request (CORE): Test the creation, query, and deletion of a Request """

        properties = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'data_type': 'DATASET',
            'granularity_type': 'FILE',
            'granularity_level': 1,
            'priority': 99,
            'edge_id': None,
            'status': 'NEW',
            'request_meta': {'taskid': 975, 'job_id': 864},
            'processing_meta': None,
            'errors': None,
        }
        request_id = add_request(**properties)
        # request = get_request(properties['scope'], properties['name'], request_metadata=properties['request_meta'])
        # assert_equal(request_id, request.request_id)

        request = get_request(request_id=request_id)
        assert_equal(request_id, request.request_id)

        json.dumps(request.to_dict())

        assert_equal(request.scope, properties['scope'])
        assert_equal(request.name, properties['name'])
        assert_equal(str(request.data_type), properties['data_type'])
        assert_equal(str(request.granularity_type), properties['granularity_type'])
        assert_equal(request.granularity_level, properties['granularity_level'])
        assert_equal(request.priority, properties['priority'])
        assert_equal(request.edge_id, properties['edge_id'])
        assert_equal(str(request.status), properties['status'])
        assert_equal(str(request.request_meta['taskid']), str(properties['request_meta']['taskid']))
        assert_equal(str(request.request_meta['job_id']), str(properties['request_meta']['job_id']))
        # assert_equal(str(request.processing_meta), str(properties['processing_meta']))
        assert_equal(request.errors, properties['errors'])

        request_id1 = add_request(**properties)
        delete_request(request_id1)

        update_request(request_id, parameters={'status': 'ERROR'})
        request = get_request(request_id=request_id)
        assert_equal(str(request.status), 'ERROR')

        with assert_raises(exceptions.NoObject):
            get_request(request_id=999999)

        delete_request(request_id)

        with assert_raises(exceptions.NoObject):
            get_request(request_id=request_id)

    @unittest.skipIf(not has_config(), "No config file")
    @unittest.skipIf(not check_user_proxy(), "No user proxy to access REST")
    @unittest.skipIf(not check_rest_host(), "REST host is not defined")
    def test_create_and_check_for_request_rest(self):
        """ Request (REST): Test the creation, query, and deletion of a Request """
        host = get_rest_host()

        properties = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'data_type': 'DATASET',
            'granularity_type': 'FILE',
            'granularity_level': 1,
            'priority': 99,
            'edge_id': None,
            'status': 'NEW',
            'request_meta': {'taskid': 975, 'job_id': 864},
            'processing_meta': None,
            'errors': None,
        }

        client = Client(host=host)

        request_id = client.add_request(**properties)

        request = client.get_request(request_id=request_id)
        assert_equal(request_id, request['request_id'])

        assert_equal(request['scope'], properties['scope'])
        assert_equal(request['name'], properties['name'])
        assert_equal(str(request['data_type']), properties['data_type'])
        assert_equal(str(request['granularity_type']), properties['granularity_type'])
        assert_equal(request['granularity_level'], properties['granularity_level'])
        assert_equal(request['priority'], properties['priority'])
        assert_equal(request['edge_id'], properties['edge_id'])
        assert_equal(str(request['status']), properties['status'])
        # assert_equal(json.dumps(request['request_meta']), json.dumps(properties['request_meta']))
        assert_equal(str(request['processing_meta']), str(properties['processing_meta']))
        assert_equal(request['errors'], properties['errors'])

        client.update_request(request_id, status='ERROR')
        request = client.get_request(request_id=request_id)
        assert_equal(str(request['status']), 'ERROR')

        with assert_raises(exceptions.NoObject):
            client.get_request(request_id=GUID().generate_uuid())

        client.delete_request(request_id)

        with assert_raises(exceptions.NoObject):
            client.get_request(request_id=request_id)

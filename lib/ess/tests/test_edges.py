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
Test Edge.
"""

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises, assert_true

from ess.client.client import Client
from ess.common import exceptions
from ess.common.utils import check_rest_host, get_rest_host
from ess.core.edges import register_edge, get_edge, get_edge_id, update_edge, delete_edge, get_edges
from ess.core.utils import render_json


class TestEdge(unittest.TestCase):

    def test_create_and_check_for_edge_core(self):
        """ Edge (CORE): Test the creation, query, and deletion of a Edge """
        edge_name = 'test_edge_%s' % str(uuid())
        edge_name = edge_name[:29]
        properties = {
            'edge_type': 'EDGE',
            'status': 'ACTIVE',
            'is_independent': True,
            'continent': 'US',
            'country_name': 'US',
            'region_code': 'US',
            'city': 'Madison',
            'longitude': '111111',
            'latitude': '2222222',
            'total_space': 0,
            'used_space': 0,
            'num_files': 0
        }
        register_edge(edge_name, **properties)
        edge = get_edge(edge_name)

        assert_equal(edge.edge_name, edge_name)
        assert_equal(str(edge.edge_type), properties['edge_type'])
        assert_equal(str(edge.status), properties['status'])
        assert_equal(edge.is_independent, properties['is_independent'])
        assert_equal(edge.continent, properties['continent'])
        assert_equal(edge.country_name, properties['country_name'])
        assert_equal(edge.region_code, properties['region_code'])
        assert_equal(edge.city, properties['city'])
        assert_equal(edge.longitude, properties['longitude'])
        assert_equal(edge.latitude, properties['latitude'])
        assert_equal(edge.total_space, properties['total_space'])
        assert_equal(edge.used_space, properties['used_space'])
        assert_equal(edge.num_files, properties['num_files'])

        assert_equal(edge.edge_id, get_edge_id(edge_name))

        with assert_raises(exceptions.NoObject):
            get_edge_id('not_exist_edge')

        with assert_raises(exceptions.NoObject):
            get_edge('not_exist_edge')

        with assert_raises(exceptions.DuplicatedObject):
            register_edge(edge_name)

        update_edge(edge_name, parameters={'status': 'LOSTHEARTBEAT'})
        edge = get_edge(edge_name)
        assert_equal(str(edge.status), 'LOSTHEARTBEAT')

        edges = get_edges()
        assert_true(len(edges) >= 1)
        render_json(**edges[0])
        edges[0].to_dict()

        edges = get_edges(status='LOSTHEARTBEAT')
        assert_true(len(edges) >= 1)

        delete_edge(edge_name)

        with assert_raises(exceptions.NoObject):
            get_edge(edge_name)

    @unittest.skipIf(not check_rest_host(), "REST host is not defined")
    def test_create_and_check_for_edge_rest(self):
        """ Edge (REST): Test the creation, query, and deletion of a Edge """
        host = get_rest_host()

        edge_name = 'test_edge_%s' % str(uuid())
        edge_name = edge_name[:29]
        properties = {
            'edge_type': 'EDGE',
            'status': 'ACTIVE',
            'is_independent': True,
            'continent': 'US',
            'country_name': 'US',
            'region_code': 'US',
            'city': 'Madison',
            'longitude': '111111',
            'latitude': '2222222',
            'total_space': 0,
            'used_space': 0,
            'num_files': 0
        }

        client = Client(host=host)
        client.register_edge(edge_name, **properties)

        edge = client.get_edge(edge_name)

        assert_equal(edge['edge_name'], edge_name)
        assert_equal(str(edge['edge_type']), properties['edge_type'])
        assert_equal(str(edge['status']), properties['status'])
        assert_equal(edge['is_independent'], properties['is_independent'])
        assert_equal(edge['continent'], properties['continent'])
        assert_equal(edge['country_name'], properties['country_name'])
        assert_equal(edge['region_code'], properties['region_code'])
        assert_equal(edge['city'], properties['city'])
        assert_equal(edge['longitude'], properties['longitude'])
        assert_equal(edge['latitude'], properties['latitude'])
        assert_equal(edge['total_space'], properties['total_space'])
        assert_equal(edge['used_space'], properties['used_space'])
        assert_equal(edge['num_files'], properties['num_files'])

        with assert_raises(exceptions.NoObject):
            client.get_edge('not_exist_edge')

        with assert_raises(exceptions.DuplicatedObject):
            client.register_edge(edge_name, **properties)

        client.update_edge(edge_name, status='LOSTHEARTBEAT')
        edge = client.get_edge(edge_name)
        assert_equal(str(edge['status']), 'LOSTHEARTBEAT')

        edges = client.list_edges()
        assert_true(len(edges) >= 1)

        client.delete_edge(edge_name)

        with assert_raises(exceptions.NoObject):
            client.get_edge(edge_name)

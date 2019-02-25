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
Test Catalog.
"""

import json

import unittest2 as unittest
from uuid import uuid4 as uuid
from nose.tools import assert_equal, assert_raises

from ess.common import exceptions
from ess.core.edges import register_edge, delete_edge
from ess.core.catalog import (add_collection, get_collection, update_collection, delete_collection,
                              add_content, update_content, get_content, delete_content,
                              get_content_best_match, add_collection_replicas, get_collection_replicas,
                              update_collection_replicas, delete_collection_replicas)


class TestCatalogCore(unittest.TestCase):

    def test_create_and_check_for_collection(self):
        """ Catalog (CORE): Test the creation, query, and deletion of a Collection """

        edge_name = 'test_rse_%s' % str(uuid())
        edge_name = edge_name[:29]

        properties_edge = {
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
        register_edge(edge_name, **properties_edge)

        properties = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'collection_type': 'DATASET',
            'coll_size': 100,
            'global_status': 'NEW',
            'total_files': 100,
            'num_replicas': 10,
            'coll_metadata': {'attr1': 'abd', 'attr2': 1}
        }
        collection_id = add_collection(**properties)
        collection = get_collection(properties['scope'], properties['name'])
        assert_equal(collection_id, collection.coll_id)

        json.dumps(collection.to_dict())

        assert_equal(collection.scope, properties['scope'])
        assert_equal(collection.name, properties['name'])
        assert_equal(str(collection.collection_type), properties['collection_type'])
        assert_equal(str(collection.global_status), properties['global_status'])
        assert_equal(collection.coll_size, properties['coll_size'])
        assert_equal(collection.total_files, properties['total_files'])
        assert_equal(collection.num_replicas, properties['num_replicas'])
        # assert_equal(collection.coll_metadata, properties['coll_metadata'])

        with assert_raises(exceptions.DuplicatedObject):
            add_collection(**properties)

        with assert_raises(exceptions.NoObject):
            get_collection(properties['scope'], 'not_exist_name')

        update_collection(scope=properties['scope'],
                          name=properties['name'],
                          parameters={'global_status': 'UNAVAILABLE'})
        collection = get_collection(properties['scope'], properties['name'], coll_id=collection.coll_id)
        assert_equal(str(collection.global_status), 'UNAVAILABLE')

        delete_collection(scope=properties['scope'], name=properties['name'], coll_id=collection.coll_id)

        with assert_raises(exceptions.NoObject):
            get_collection(scope=properties['scope'], name=properties['name'], coll_id=collection.coll_id)

        delete_edge(edge_name)

    def test_create_and_check_for_collection_replicas(self):
        """ Catalog (CORE): Test the creation, query, and deletion of a Collection replicas"""

        edge_name = 'test_rse_%s' % str(uuid())
        edge_name = edge_name[:29]

        properties_edge = {
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
        edge_id = register_edge(edge_name, **properties_edge)

        properties_coll = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'collection_type': 'DATASET',
            'coll_size': 100,
            'global_status': 'NEW',
            'total_files': 100,
            'num_replicas': 10,
            'coll_metadata': {'attr1': 'abd', 'attr2': 1},
        }
        collection_id = add_collection(**properties_coll)

        properties = {
            'status': 'NEW',
            'transferring_files': 0,
            'replicated_files': 0,
            'num_active_requests': 1,
            'retries': 0
        }

        add_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name, **properties)
        coll_replicas = get_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name)

        assert_equal(collection_id, coll_replicas.coll_id)
        assert_equal(edge_id, coll_replicas.edge_id)

        assert_equal(str(coll_replicas.status), properties['status'])
        assert_equal(coll_replicas.transferring_files, properties['transferring_files'])
        assert_equal(coll_replicas.replicated_files, properties['replicated_files'])
        assert_equal(coll_replicas.num_active_requests, properties['num_active_requests'])
        assert_equal(coll_replicas.retries, properties['retries'])

        with assert_raises(exceptions.DuplicatedObject):
            add_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name, **properties)

        with assert_raises(exceptions.NoObject):
            get_collection_replicas(properties_coll['scope'], 'not_exist_name', edge_name)

        coll_replicas1 = get_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name, edge_id=edge_id)
        assert_equal(coll_replicas1.coll_id, coll_replicas.coll_id)
        assert_equal(coll_replicas1.edge_id, coll_replicas.edge_id)

        coll_replicas1 = get_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name,
                                                 coll_id=coll_replicas.coll_id, edge_id=edge_id)
        assert_equal(coll_replicas1.coll_id, coll_replicas.coll_id)
        assert_equal(coll_replicas1.edge_id, coll_replicas.edge_id)

        update_collection_replicas(scope=properties_coll['scope'],
                                   name=properties_coll['name'],
                                   edge_name=edge_name,
                                   parameters={'status': 'UNAVAILABLE'})
        coll_replicas = get_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name)
        assert_equal(str(coll_replicas.status), 'UNAVAILABLE')

        delete_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name)

        with assert_raises(exceptions.NoObject):
            get_collection_replicas(properties_coll['scope'], properties_coll['name'], edge_name)

        delete_edge(edge_name)

    def test_create_and_check_for_content(self):
        """ Catalog (CORE): Test the creation, query, and deletion of a Content """
        edge_name = 'test_rse_%s' % str(uuid())
        edge_name = edge_name[:29]

        properties_edge = {
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
        edge_id = register_edge(edge_name, **properties_edge)

        properties_collection = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'collection_type': 'DATASET',
            'coll_size': 100,
            'global_status': 'NEW',
            'total_files': 100,
            'num_replicas': 10,
            'coll_metadata': {'attr1': 'abd', 'attr2': 1},
        }

        collection_id = add_collection(**properties_collection)

        properties = {
            'scope': 'test_scope',
            'name': 'test_name_%s' % str(uuid()),
            'min_id': 0,
            'max_id': 7,
            'coll_id': collection_id,
            'content_type': 'PARTIAL',
            'edge_id': edge_id,
            'status': 'NEW',
            'priority': 100,
            'num_success': 1,
            'num_failure': 0,
            'last_failed_at': None,
            'pfn_size': 1,
            'pfn': 'adss',
            'object_metadata': {'size': 1, 'md5': '1322232', 'adler32': '12345678'}
        }

        content_id = add_content(**properties)
        content = get_content(properties['scope'],
                              properties['name'],
                              properties['min_id'],
                              properties['max_id'],
                              edge_name=edge_name)
        assert_equal(content_id, content.content_id)

        json.dumps(content.to_dict())

        content = get_content(properties['scope'],
                              properties['name'],
                              properties['min_id'],
                              properties['max_id'],
                              edge_id=edge_id)
        assert_equal(content_id, content.content_id)

        assert_equal(content.scope, properties['scope'])
        assert_equal(content.name, properties['name'])
        assert_equal(content.min_id, properties['min_id'])
        assert_equal(content.max_id, properties['max_id'])
        assert_equal(content.coll_id, properties['coll_id'])
        assert_equal(str(content.content_type), properties['content_type'])
        assert_equal(content.edge_id, properties['edge_id'])
        assert_equal(str(content.status), properties['status'])
        assert_equal(content.priority, properties['priority'])
        assert_equal(content.num_success, properties['num_success'])
        assert_equal(content.num_failure, properties['num_failure'])
        assert_equal(content.last_failed_at, properties['last_failed_at'])
        assert_equal(content.pfn_size, properties['pfn_size'])
        assert_equal(content.pfn, properties['pfn'])

        with assert_raises(exceptions.DuplicatedObject):
            add_content(**properties)

        with assert_raises(exceptions.NoObject):
            get_content(properties['scope'],
                        properties['name'],
                        properties['min_id'],
                        properties['max_id'],
                        edge_name='Not_exist_edge')

        with assert_raises(exceptions.NoObject):
            get_content(properties['scope'],
                        properties['name'],
                        properties['min_id'] + 1,
                        properties['max_id'] + 1,
                        edge_name='Not_exist_edge')

        with assert_raises(exceptions.NoObject):
            get_content(properties['scope'],
                        properties['name'],
                        edge_name=edge_name)

        update_content(scope=properties['scope'],
                       name=properties['name'],
                       min_id=properties['min_id'],
                       max_id=properties['max_id'],
                       edge_id=edge_id,
                       parameters={'status': 'UNAVAILABLE'})

        content = get_content(properties['scope'],
                              properties['name'],
                              content_id=content_id)
        assert_equal(str(content.status), 'UNAVAILABLE')

        content = get_content_best_match(properties['scope'],
                                         properties['name'],
                                         properties['min_id'] + 1,
                                         properties['max_id'] - 1,
                                         edge_id=edge_id)
        assert_equal(content_id, content.content_id)

        delete_content(scope=properties['scope'], name=properties['name'], content_id=content_id)

        with assert_raises(exceptions.NoObject):
            get_content(scope=properties['scope'], name=properties['name'], content_id=content_id)

        delete_collection(scope=properties_collection['scope'], name=properties_collection['name'], coll_id=collection_id)
        delete_edge(edge_name)

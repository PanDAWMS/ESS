#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from ess.common import exceptions
from ess.client.client import Client

edge_properties = {
    'edge_type': 'EDGE',
    'status': 'ACTIVE',
    'is_independent': True,
    'continent': 'US',
    'country_name': 'US',
    'region_code': 'US',
    'city': 'Madison',
    'time_zone': 'timeZone',
    'longitude': '111111',
    'latitude': '2222222',
    'total_space': 0,
    'used_space': 0,
    'num_files': 0
}

client = Client(host='https://aipanda180.cern.ch:8443')

try:
    client.register_edge('my_edge_name2', **edge_properties)
except exceptions.DuplicatedObject as ex:
    print(ex)

edge = client.get_edge('my_edge_name2')
print('get edge:')
print(edge)

edges = client.list_edges()
print('list edges:')
print(edges)

status = client.update_edge('my_edge_name2', city='MyCity')
print('update edge:')
print(status)

edge = client.get_edge('my_edge_name2')
print('updated edge:')
print(edge)

status = client.delete_edge('my_edge_name2')
print('delete edge:')
print(status)

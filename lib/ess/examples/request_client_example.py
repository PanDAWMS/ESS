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

properties = {
    'scope': 'mc16_13TeV',
    'name': 'mc16_13TeV.450525.MadGraphPythia8EvtGen_A14NNPDF23LO_X2000tohh_bbtautau_hadhad.merge.EVNT.e7244_e5984_tid16986378_00',
    'data_type': 'DATASET',
    'granularity_type': 'PARTIAL',
    'granularity_level': 10,
    'priority': 99,
    'edge_id': None,
    'status': 'NEW',
    'request_meta': {'jeditaskid': 16986388, 'site': 'CERN'},
}

client = Client(host='https://aipanda182.cern.ch:8443')

try:
    request_id = client.add_request(**properties)
except exceptions.DuplicatedObject as ex:
    print(ex)

req = client.get_request(request_id=request_id)
print req

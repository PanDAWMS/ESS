#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from ess.client.client import Client

scope = 'mc16_13TeV'
name = 'EVNT.16986378._000012.pool.root.1'

client = Client(host='https://aipanda182.cern.ch:8443')


ret = client.download(scope=scope, name=name, min_id=1, max_id=10)
print ret

ret = client.download(scope=scope, name=name, min_id=1, max_id=10, dest_dir='..')
print ret

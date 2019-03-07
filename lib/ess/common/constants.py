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
Constants.
"""


class Sections:
    Main = 'main'
    Assigner = 'assigner'
    BaseDaemon = 'basedaemon'
    Broker = 'broker'
    Finisher = 'finisher'
    PreCacher = 'precacher'
    ResourceManager = 'resourcemanager'
    Splitter = 'splitter'
    Stager = 'stager'


class HTTP_STATUS_CODE:
    OK = 200
    Created = 201
    Accepted = 202

    # Client Errors
    BadRequest = 400
    Unauthorized = 401
    Forbidden = 403
    NotFound = 404
    NoMethod = 405
    Conflict = 409

    # Server Errors
    InternalError = 500

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
SQLALchemy constant values for ESS models
"""

from ess.orm.enum import DeclEnum


SCOPE_LENGTH = 25
NAME_LENGTH = 255


class EdgeType(DeclEnum):
    HEAD = 'H', 'HEAD'
    EDGE = 'E', 'EDGE'
    COMPOSITE = 'C', 'COMPOSITE'


class EdgeStatus(DeclEnum):
    ACTIVE = 'A', 'ACTIVE'
    LOSTHEARTBEAT = 'L', 'LOSTHEARTBEAT'
    FAILED = 'F', 'FAILED'


class CollectionType(DeclEnum):
    DATASET = 'D', 'DATASET'
    CONTAINER = 'C', 'CONTAINER'
    FILE = 'F', 'FILE'


class CollectionStatus(DeclEnum):
    NEW = 'N', 'NEW'
    REPLICATING = 'R', 'REPLICATING'
    AVAILABLE = 'A', 'AVAILABLE'
    PARTLYAVAILABLE = 'P', 'PARTLYAVAILABLE'
    UNAVAILABLE = 'U', 'UNAVAILABLE'
    BAD = 'B', 'BAD'
    REMOVABLE = 'R', 'REMOVABLE'
    REMOVING = 'I', 'REMOVING'
    REMOVED = 'D', 'REMOVED'


class CollectionReplicasStatus(DeclEnum):
    NEW = 'N', 'NEW'
    REPLICATING = 'R', 'REPLICATING'
    AVAILABLE = 'A', 'AVAILABLE'
    PARTLYAVAILABLE = 'P', 'PARTLYAVAILABLE'
    UNAVAILABLE = 'U', 'UNAVAILABLE'
    BAD = 'B', 'BAD'
    REMOVABLE = 'R', 'REMOVABLE'
    REMOVING = 'I', 'REMOVING'
    REMOVED = 'D', 'REMOVED'


class ContentType(DeclEnum):
    FILE = 'F', 'FILE'
    PARTIAL = 'P', 'PARTIAL'


class ContentStatus(DeclEnum):
    NEW = 'N', 'NEW'
    # REPLICATING = 'R', 'REPLICATING'
    AVAILABLE = 'A', 'AVAILABLE'
    UNAVAILABLE = 'U', 'UNAVAILABLE'
    BAD = 'B', 'BAD'
    PRECACHED = 'C', 'PRECACHED'
    TOSPLIT = 'T', 'TOSPLIT'
    SPLITTING = 'S', 'SPLITTING'
    SPLITTED = 'L', 'SPLITTED'
    TOSTAGEDOUT = 'O', 'TOSTAGEDOUT'
    STAGINGOUT = 'G', 'STAGINGOUT'
    REMOVABLE = 'R', 'REMOVABLE'
    REMOVING = 'I', 'REMOVING'
    REMOVED = 'D', 'REMOVED'


class DataType(DeclEnum):
    DATASET = 'D', 'DATASET'
    CONTAINER = 'C', 'CONTAINER'
    FILE = 'F', 'FILE'


class GranularityType(DeclEnum):
    FILE = 'F', 'FILE'
    PARTIAL = 'P', 'PARTIAL'


class RequestStatus(DeclEnum):
    NEW = 'N', 'NEW'
    REPLICATING = 'R', 'REPLICATING'
    AVAILABLE = 'A', 'AVAILABLE'
    ERROR = 'E', 'ERROR'
    BROKERING = 'B', 'BROKERING'
    WAITING = 'W', 'WAITING'
    ASSIGNING = 'I', 'ASSIGNING'
    ASSIGNINGFAILED = 'F', 'ASSIGNINGFAILED'
    ASSIGNED = 'D', 'ASSIGNED'
    PRECACHING = 'P', 'PRECACHING'
    PRECACHED = 'C', 'PRECACHED'
    TOSPLITTING = 'T', 'TOSPLITTING'
    SPLITTING = 'S', 'SPLITTING'
    SPLITTED = 'L', 'SPLITTED'

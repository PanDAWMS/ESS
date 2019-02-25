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
ESS Exceptions.

error codes:
    The fist the number is one of the main catagories.
    The second number is one of the subcatagories.
    The third number and numbers after third one are local defined for every subcatagory.

Catagories:
 1. ORM related exception
    1. request table related exception
    2. collection table related exception
    3. collection_content table related exception
    4. collection_replicas table related exception
    5. replicas table related exception
    6. edges table related exception
    7. messages table related exception
 2. core related exception
 3. Rest related exception
    1. bad request
    2. connection error
 4. broker exception
    1. NoRquestedData
    2. No suitable Edges
    3. Broker plugin exception
"""


class ESSException(Exception):
    """
    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.
    """

    def __init__(self, *args, **kwargs):
        super(ESSException, self).__init__()
        self._message = "An unknown ESS exception occurred."
        self.args = args
        self.kwargs = kwargs
        self.error_code = 1
        self._error_string = None

    def construct_error_string(self):
        try:
            self._error_string = "%s: %s" % (self._message, self.kwargs)
        except Exception:
            # at least get the core message out if something happened
            self._error_string = self._message
        if len(self.args) > 0:
            # Convert all arguments into their string representations...
            args = ["%s" % arg for arg in self.args if arg]
            self._error_string = (self._error_string + "\nDetails: %s" % '\n'.join(args))
        return self._error_string.strip()

    def __str__(self):
        self.construct_error_string()
        return self._error_string.strip()

    def get_detail(self):
        self.construct_error_string()
        return self._error_string.strip() + "\nStacktrace: %s" % self._stack_trace


class DatabaseException(ESSException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(DatabaseException, self).__init__(*args, **kwargs)
        self._message = "Database exception."
        self.error_code = 100


class InvalidDatabaseType(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(InvalidDatabaseType, self).__init__(*args, **kwargs)
        self._message = "Invalid Database type exception."
        self.error_code = 101


class NoObject(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(NoObject, self).__init__(*args, **kwargs)
        self._message = "No object  exception."
        self.error_code = 102


class DuplicatedObject(DatabaseException):
    """
    RucioException
    """
    def __init__(self, *args, **kwargs):
        super(DuplicatedObject, self).__init__(*args, **kwargs)
        self._message = "Duplicated object exception."
        self.error_code = 103


class NoReplica(DatabaseException):
    """
    NoReplica
    """
    def __init__(self, *args, **kwargs):
        super(NoReplica, self).__init__(*args, **kwargs)
        self._message = "No replica"
        self.error_code = 150


class CoreException(ESSException):
    """
    CoreException
    """
    def __init__(self, *args, **kwargs):
        super(CoreException, self).__init__(*args, **kwargs)
        self._message = "Core exception."
        self.error_code = 200


class RestException(ESSException):
    """
    RestException
    """
    def __init__(self, *args, **kwargs):
        super(RestException, self).__init__(*args, **kwargs)
        self._message = "Rest exception."
        self.error_code = 300


class BadRequest(RestException):
    """
    BadRequest
    """
    def __init__(self, *args, **kwargs):
        super(BadRequest, self).__init__(*args, **kwargs)
        self._message = "Bad request exception."
        self.error_code = 301


class ConnectionException(RestException):
    """
    ConnectionException
    """
    def __init__(self, *args, **kwargs):
        super(ConnectionException, self).__init__(*args, **kwargs)
        self._message = "Connection exception."
        self.error_code = 302


class BrokerException(ESSException):
    """
    BrokerException
    """
    def __init__(self, *args, **kwargs):
        super(BrokerException, self).__init__(*args, **kwargs)
        self._message = "Broker exception."
        self.error_code = 400


class NoRequestedData(BrokerException):
    """
    NoRequestedData Exception
    """
    def __init__(self, *args, **kwargs):
        super(NoRequestedData, self).__init__(*args, **kwargs)
        self._message = "No requested data exception."
        self.error_code = 401


class NoSuitableEdges(BrokerException):
    """
    NoSuitableEdges exception
    """
    def __init__(self, *args, **kwargs):
        super(NoSuitableEdges, self).__init__(*args, **kwargs)
        self._message = "No suitable edges exception."
        self.error_code = 402


class BrokerPluginError(BrokerException):
    """
    BrokerPluginError exception
    """
    def __init__(self, *args, **kwargs):
        super(BrokerPluginError, self).__init__(*args, **kwargs)
        self._message = "Broker plugin exception."
        self.error_code = 403

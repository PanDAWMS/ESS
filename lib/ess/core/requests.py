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
operations related to Requests.
"""

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.exc import DatabaseError, IntegrityError

from ess.common import exceptions
from ess.core.edges import get_edge_id
from ess.orm import models
from ess.orm.constants import DataType, RequestStatus, GranularityType
from ess.orm.session import read_session, transactional_session


@transactional_session
def add_request(scope, name, data_type=DataType.DATASET, granularity_type=GranularityType.FILE,
                granularity_level=None, priority=0, edge_id=None, status=RequestStatus.NEW,
                request_meta=None, processing_meta=None, errors=None, session=None):
    """
    Add a request.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param data_type: The type of the request data.
    :param granularity_type: The granularity type, for example File or Partial.
    :param granularity_level: The granularity level, for example, number of events.
    :param priority: The priority as an integer.
    :param edge_id: The id of the assigned edge.
    :param status: The status of the request.
    :param request_meta: The metadata of the request, as Json.
    :param processing_meta: The processing metadata, as Json.
    :param errors: The processing errors, as Json.
    :param session: The database session in use.

    :raises DuplicatedObject: If an request with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: request id.
    """
    if isinstance(data_type, str) or isinstance(data_type, unicode):
        data_type = DataType.from_sym(str(data_type))

    if isinstance(granularity_type, str) or isinstance(granularity_type, unicode):
        granularity_type = GranularityType.from_sym(str(granularity_type))

    if isinstance(status, str) or isinstance(status, unicode):
        status = RequestStatus.from_sym(str(status))

    new_request = models.Request(scope=scope, name=name, data_type=data_type, granularity_type=granularity_type,
                                 granularity_level=granularity_level, priority=priority, edge_id=edge_id, status=status,
                                 request_meta=request_meta, processing_meta=processing_meta, errors=errors)

    try:
        new_request.save(session=session)
    except IntegrityError:
        raise exceptions.DuplicatedObject('Request %s:%s already exists!' % (scope, name))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return new_request.request_id


@transactional_session
def update_request(request_id, parameters, session=None):
    """
    update an request.

    :param request_id: the request id.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    :raises DatabaseException: If there is a database error.

    :returns: request id.
    """
    try:
        if 'data_type' in parameters and \
           (isinstance(parameters['data_type'], str) or isinstance(parameters['data_type'], unicode)):
            parameters['data_type'] = DataType.from_sym(str(parameters['data_type']))

        if 'granularity_type' in parameters and \
           (isinstance(parameters['granularity_type'], str) or isinstance(parameters['granularity_type'], unicode)):
            parameters['granularity_type'] = GranularityType.from_sym(str(parameters['granularity_type']))

        if 'status' in parameters and \
           (isinstance(parameters['status'], str) or isinstance(parameters['status'], unicode)):
            parameters['status'] = RequestStatus.from_sym(str(parameters['status']))

        request = session.query(models.Request).filter_by(request_id=request_id).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Request %s cannot be found' % request_id)

    try:
        request.update(parameters)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return request.request_id


@read_session
def get_request(scope=None, name=None, request_id=None, request_meta=None, session=None):
    """
    Get a request or raise a NoObject exception.

    :param scope: The scope of the request data.
    :param name: The name of the request data.
    :param request_id: The id of the request.
    :param request_meta: The metadata of the request, as Json.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request model.
    """

    try:
        if request_id:
            request = session.query(models.Request).filter_by(request_id=request_id).one()
        else:
            request = session.query(models.Request).filter_by(scope=scope, name=name).filter(request_meta.like(request_meta)).one()

        request['data_type'] = request.data_type
        request['granularity_type'] = request.granularity_type
        request['status'] = request.status
        return request
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('request %s:%s(id:%s,meta:%s) cannot be found' % (scope, name, request_id, request_meta))


@read_session
def get_requests_by_edge(edge_name, edge_id=None, session=None):
    """
    Get requests by edge.

    :param edge_name: The name of the edge.
    :param edge_id: The id of the edge
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: list of Request model.
    """

    try:
        if not edge_id:
            edge_id = get_edge_id(edge_name)

        requests = session.query(models.Request).filter_by(edge_id=edge_id).all()

        for request in requests:
            request['data_type'] = request.data_type
            request['granularity_type'] = request.granularity_type
            request['status'] = request.status
        return requests
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('No requests at %s' % (edge_name))


@read_session
def get_requests(status=None, edge_name=None, edge_id=None, session=None):
    """
    Get requests.

    :param status: The status of the request data.
    :param edge_name: The name of the edge.
    :param edge_id: The id of the edge
    :param session: The database session in use.

    :raises NoObject: If no request is founded.

    :returns: Request models.
    """

    try:
        if edge_name and not edge_id:
            edge_id = get_edge_id(edge_name)

        if status:
            if (isinstance(status, str) or isinstance(status, unicode)):
                status = RequestStatus.from_sym(status)

            if edge_id:
                requests = session.query(models.Request).filter_by(status=status, edge_id=edge_id).all()
            else:
                requests = session.query(models.Request).filter_by(status=status).all()
        else:
            if edge_id:
                requests = session.query(models.Request).filter_by(edge_id=edge_id).all()
            else:
                requests = session.query(models.Request).all()

        for request in requests:
            request['data_type'] = request.data_type
            request['granularity_type'] = request.granularity_type
            request['status'] = request.status
        return requests
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Cannot find request with status: %s' % (status))


@transactional_session
def delete_request(request_id, session=None):
    """
    Delete a request or raise a NoObject exception.

    :param request_id: The request id.
    :param session: The database session in use.

    :raises NoObject: If no request is founded.
    """

    try:
        session.query(models.Request).filter_by(request_id=request_id).delete()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Request %s cannot be found' % request_id)

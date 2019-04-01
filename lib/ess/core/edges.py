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
operations related to Edges.
"""

import datetime
import sqlalchemy
import sqlalchemy.orm

from sqlalchemy.exc import DatabaseError, IntegrityError

from ess.common import exceptions
from ess.orm import models
from ess.orm.constants import EdgeType, EdgeStatus
from ess.orm.session import read_session, transactional_session


@transactional_session
def register_edge(edge_name, edge_type=EdgeType.EDGE, status=EdgeStatus.ACTIVE, is_independent=True,
                  continent=None, country_name=None, region_code=None, city=None, longitude=None,
                  latitude=None, total_space=0, used_space=0, reserved_space=0, num_files=0,
                  session=None):
    """
    Register an edge.

    :param edge_name: the name of the edge.
    :param edge_type: The type of the edge.
    :param status: Status of the edge.
    :param is_independent: Whether it's using current db or independent db.
    :param continent: The continent of the edge.
    :param country_name: The country of the edge.
    :param region_code: The region code for the edge.
    :param city: The city for the edge.
    :param latitude: The latitude coordinate of edge.
    :param longitude: The longitude coordinate of edge.
    :param total_space: Total space of the edge.
    :param used_space: Used space of the edge.
    :param reserved_space: Reserved space of the edge.
    :param num_files: Number of cached files in the edge.
    :param session: The database session in use.

    :raises DuplicatedObject: If an edge with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: edge id.
    """
    if isinstance(edge_type, str) or isinstance(edge_type, unicode):
        edge_type = EdgeType.from_sym(str(edge_type))

    if isinstance(status, str) or isinstance(status, unicode):
        status = EdgeStatus.from_sym(str(status))

    new_edge = models.Edge(edge_name=edge_name, edge_type=edge_type, status=status,
                           is_independent=is_independent, continent=continent, country_name=country_name,
                           region_code=region_code, city=city, longitude=longitude, latitude=latitude,
                           total_space=total_space, used_space=used_space, reserved_space=reserved_space,
                           num_files=num_files)

    try:
        new_edge.save(session=session)
    except IntegrityError as error:
        raise exceptions.DuplicatedObject('Edge %s already exists!: %s' % (edge_name, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)

    return new_edge.edge_id


@transactional_session
def update_edge(edge_name, parameters, session=None):
    """
    update an edge.

    :param edge_name: the name of the edge.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    :raises DatabaseException: If there is a database error.

    :returns: edge id.
    """
    try:
        if 'edge_type' in parameters and \
           (isinstance(parameters['edge_type'], str) or isinstance(parameters['edge_type'], unicode)):
            parameters['edge_type'] = EdgeType.from_sym(str(parameters['edge_type']))

        if 'status' in parameters and \
           (isinstance(parameters['status'], str) or isinstance(parameters['status'], unicode)):
            parameters['status'] = EdgeStatus.from_sym(str(parameters['status']))

        edge = session.query(models.Edge).filter_by(edge_name=edge_name).one()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Edge %s cannot be found: %s' % (edge_name, error))

    try:
        edge.update(parameters)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error)

    return edge.edge_id


@read_session
def get_edge(edge_name, edge_id=None, session=None):
    """
    Get an Edge or raise a NoObject exception.

    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Edge model.
    """

    try:
        if edge_id:
            edge = session.query(models.Edge).filter_by(edge_id=edge_id).one()
        else:
            edge = session.query(models.Edge).filter_by(edge_name=edge_name).one()

        edge['edge_type'] = edge.edge_type
        return edge
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Edge %s cannot be found: %s' % (edge_name, error))


@read_session
def get_edge_id(edge_name, session=None):
    """
    Get an Edge id or raise a NoObject exception.

    :param edge_name: The name of the edge.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Edge id.
    """

    try:
        return session.query(models.Edge.edge_id).filter_by(edge_name=edge_name).one()[0]
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Edge %s cannot be found: %s' % (edge_name, error))


@transactional_session
def delete_edge(edge_name, session=None):
    """
    Delete an Edge or raise a NoObject exception.

    :param edge_name: The name of the edge.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    """

    try:
        session.query(models.Edge).filter_by(edge_name=edge_name).delete()
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Edge %s cannot be found: %s' % (edge_name, error))


@transactional_session
def clean_old_edges(seconds, session=None):
    """
    Delete edges where are not updated for <seconds> seconds.

    :param seconds: Number of seconds.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    """

    try:
        session.query(models.Edge).filter(models.Edge.updated_at < datetime.datetime.utcnow() - datetime.timedelta(seconds=seconds)).update({'status': EdgeStatus.LOSTHEARTBEAT})
    except Exception as error:
        raise exceptions.DatabaseException('Failed to update old edges to LOSTHeARTBEAT: %s' % error)


@read_session
def get_edges(status=None, session=None):
    """
    Get an Edge or raise a NoObject exception.

    :param status: The status of the edge.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Edge models.
    """

    try:
        if status:
            if (isinstance(status, str) or isinstance(status, unicode)):
                status = EdgeStatus.from_sym(status)
            edges = session.query(models.Edge).filter_by(status=status).all()
        else:
            edges = session.query(models.Edge).all()

        for edge in edges:
            edge['edge_type'] = edge.edge_type
        return edges
    except sqlalchemy.orm.exc.NoResultFound as error:
        raise exceptions.NoObject('Cannot find edges with status: %s, %s' % (status, error))

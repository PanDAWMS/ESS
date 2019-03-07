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
operations related to collections and collection content.
"""

import sqlalchemy
import sqlalchemy.orm

from sqlalchemy import and_, func
from sqlalchemy.exc import DatabaseError, IntegrityError

from ess.common import exceptions
from ess.core.edges import get_edge_id
from ess.orm import models
from ess.orm.constants import CollectionType, CollectionStatus, ContentType, ContentStatus, CollectionReplicasStatus
from ess.orm.models import CollectionContent
from ess.orm.session import read_session, transactional_session


@transactional_session
def add_collection(scope, name, collection_type=CollectionType.DATASET, coll_size=0, global_status=CollectionStatus.NEW,
                   total_files=0, num_replicas=0, coll_metadata=None, session=None):
    """
    Add a collection.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param collection_type: The type of the collection data.
    :param coll_size: The space size of the collection as an integer.
    :param global_status: The status of the collection.
    :param total_files: Total number of files of the collection.
    :param num_replicas: Number of replicas.
    :param coll_metadata: Collection metadata in json.
    :param session: The database session in use.

    :raises DuplicatedObject: If an edge with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    if isinstance(collection_type, str) or isinstance(collection_type, unicode):
        collection_type = CollectionType.from_sym(str(collection_type))

    if isinstance(global_status, str) or isinstance(global_status, unicode):
        global_status = CollectionStatus.from_sym(str(global_status))

    new_collection = models.Collection(scope=scope, name=name, collection_type=collection_type, coll_size=coll_size, global_status=global_status,
                                       total_files=total_files, num_replicas=num_replicas, coll_metadata=coll_metadata)

    try:
        new_collection.save(session=session)
    except IntegrityError:
        raise exceptions.DuplicatedObject('Collection %s:%s already exists!' % (scope, name))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return new_collection.coll_id


@transactional_session
def update_collection(scope, name, parameters=None, coll_id=None, session=None):
    """
    update a collection.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param parameters: A dictionary of parameters.
    :param coll_id: Collection id.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    :raises DatabaseException: If there is a database error.

    :returns: collection id.
    """
    try:
        if coll_id:
            collection = session.query(models.Collection).filter_by(coll_id=coll_id).one()
        else:
            collection = session.query(models.Collection).filter_by(scope=scope, name=name).one()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s cannot be found' % (scope, name))

    try:
        if 'collection_type' in parameters and \
           (isinstance(parameters['collection_type'], str) or isinstance(parameters['collection_type'], unicode)):
            parameters['collection_type'] = CollectionType.from_sym(str(parameters['collection_type']))

        if 'global_status' in parameters and \
           (isinstance(parameters['global_status'], str) or isinstance(parameters['global_status'], unicode)):
            parameters['global_status'] = CollectionStatus.from_sym(str(parameters['global_status']))

        collection.update(parameters)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return collection.coll_id


@read_session
def get_collection(scope, name, coll_id=None, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param coll_id: Collection id.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Collection model.
    """

    try:
        if coll_id:
            collection = session.query(models.Collection).filter_by(coll_id=coll_id).one()
        else:
            collection = session.query(models.Collection).filter_by(scope=scope, name=name).one()

        collection['collection_type'] = collection.collection_type
        collection['global_status'] = collection.global_status
        return collection
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s cannot be found' % (scope, name))


@read_session
def get_collection_id(scope, name, session=None):
    """
    Get a collection or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Collection model.
    """

    try:
        collection_id = session.query(models.Collection.coll_id).filter_by(scope=scope, name=name).one()[0]

        return collection_id
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s cannot be found' % (scope, name))


@transactional_session
def delete_collection(scope, name, coll_id=None, session=None):
    """
    Delete a collection or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param coll_id: Collection id.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    """

    try:
        if coll_id:
            session.query(models.Collection).filter_by(coll_id=coll_id).delete()
        else:
            session.query(models.Collection).filter_by(scope=scope, name=name).delete()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s cannot be found' % (scope, name))


@transactional_session
def add_collection_replicas(scope, name, edge_name, coll_id=None, edge_id=None, status=CollectionReplicasStatus.NEW,
                            transferring_files=0, replicated_files=0, num_active_requests=1, retries=0, session=None):
    """
    Add a collection replicas.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param edge_name: The name of the replicating edge.
    :param edge_id: The id of the replicating edge.
    :param coll_id: Collection id.
    :param status: The status of the collection replicas.
    :param transferring_files: Number of transferring files.
    :param replicated_files: Number of replicated files.
    :param num_active_requests: Number of active requests.
    :param retries: Number of retries.
    :param session: The database session in use.

    :raises DuplicatedObject: If an edge with the same name exists.
    :raises DatabaseException: If there is a database error.
    """
    if not coll_id:
        coll_id = get_collection_id(scope=scope, name=name, session=session)
    if not edge_id:
        edge_id = get_edge_id(edge_name=edge_name, session=session)

    try:
        if isinstance(status, str) or isinstance(status, unicode):
            status = CollectionReplicasStatus.from_sym(str(status))

        new_collection_replicas = models.CollectionReplicas(coll_id=coll_id, edge_id=edge_id, status=status,
                                                            transferring_files=transferring_files,
                                                            replicated_files=replicated_files,
                                                            num_active_requests=num_active_requests,
                                                            retries=retries)
        new_collection_replicas.save(session=session)
    except IntegrityError as error:
        if 'a foreign key constraint fails' in str(error):
            raise exceptions.DatabaseException("Failed to add new collection %s:%s(at edge %s): %s" % (scope, name, edge_name, error))
        else:
            raise exceptions.DuplicatedObject('Collection %s:%s(at edge %s) already exists! (%s)' % (scope, name, edge_name, error))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)


@read_session
def get_collection_replicas(scope, name, edge_name, coll_id=None, edge_id=None, session=None):
    """
    Get a collection replicas or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param coll_id: Collection id.
    :param edge_name: The name of the replicating edge.
    :param edge_id: The id of the replicating edge.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: CollectionReplicas model.
    """

    try:
        if not coll_id:
            coll_id = get_collection_id(scope=scope, name=name, session=session)
        if not edge_id:
            edge_id = get_edge_id(edge_name=edge_name, session=session)

        collection_replicas = session.query(models.CollectionReplicas).filter_by(coll_id=coll_id, edge_id=edge_id).one()

        collection_replicas['status'] = collection_replicas.status
        return collection_replicas
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection replicas %s:%s(at %s) cannot be found' % (scope, name, edge_name))


@transactional_session
def update_collection_replicas(scope, name, edge_name, coll_id=None, edge_id=None, parameters=None, session=None):
    """
    update a collection replicas.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param coll_id: Collection id.
    :param edge_name: The name of the replicating edge.
    :param edge_id: The id of the replicating edge.
    :param parameters: A dictionary of parameters.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    :raises DatabaseException: If there is a database error.

    """
    try:
        collection_replicas = get_collection_replicas(scope, name, edge_name, coll_id=coll_id, edge_id=edge_id, session=session)
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s cannot be found' % (scope, name))

    try:
        if 'status' in parameters and \
           (isinstance(parameters['status'], str) or isinstance(parameters['status'], unicode)):
            parameters['status'] = CollectionReplicasStatus.from_sym(str(parameters['status']))

        collection_replicas.update(parameters)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)


@transactional_session
def delete_collection_replicas(scope, name, edge_name, coll_id=None, edge_id=None, session=None):
    """
    Delete a collection replicas or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param coll_id: Collection id.
    :param edge_name: The name of the replicating edge.
    :param edge_id: The id of the replicating edge.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    """

    try:
        if not coll_id:
            coll_id = get_collection_id(scope=scope, name=name, session=session)
        if not edge_id:
            edge_id = get_edge_id(edge_name=edge_name, session=session)

        session.query(models.CollectionReplicas).filter_by(coll_id=coll_id, edge_id=edge_id).delete()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Collection %s:%s(at %s) cannot be found' % (scope, name, edge_name))


@transactional_session
def add_content(scope, name, min_id=None, max_id=None, coll_id=None, content_type=ContentType.FILE,
                status=ContentStatus.NEW, priority=0, edge_name=None, edge_id=None, num_success=0,
                num_failure=0, last_failed_at=None, pfn_size=0, pfn=None, object_metadata=None, session=None):
    """
    Add a collection content.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param min_id: The minimum id of the partial file, related to the whole file.
    :param max_id: The maximum id of the partial file, related to the whole file.
    :param coll_id: The collection it belongs to.
    :param content_type: The type of the cotent data.
    :param status: The status of the content.
    :param priority: The priority as an integer.
    :param edge_name: The name of the replicating edge.
    :param edge_id: The id of the replicating edge.
    :param num_success: The number of successful access.
    :param num_failure; The number of failed access
    :param last_failed_at: The time of last failure.
    :param pfn_size: The size of physical file name.
    :param pfn: Physical file name.

    :raises DuplicatedObject: If an edge with the same name exists.
    :raises DatabaseException: If there is a database error.

    :returns: collection content id.
    """
    if isinstance(content_type, str) or isinstance(content_type, unicode):
        content_type = ContentType.from_sym(str(content_type))

    if isinstance(status, str) or isinstance(status, unicode):
        status = ContentStatus.from_sym(str(status))

    if not edge_id:
        edge_id = get_edge_id(edge_name=edge_name, session=session)

    new_content = models.CollectionContent(scope=scope, name=name, min_id=min_id, max_id=max_id, coll_id=coll_id,
                                           content_type=content_type, status=status, priority=priority,
                                           edge_id=edge_id, num_success=num_success, num_failure=num_failure,
                                           last_failed_at=last_failed_at, pfn_size=pfn_size, pfn=pfn,
                                           object_metadata=object_metadata)

    try:
        new_content.save(session=session)
    except IntegrityError:
        raise exceptions.DuplicatedObject('Content %s:%s[%s:%s](at edge %s) already exists!' % (scope, name, min_id, max_id, edge_name))
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return new_content.content_id


@transactional_session
def add_contents(collection_scope, collection_name, edge_name, files, session=None):
    """
    Add a collection contents.

    :param collection_scope: The scope of the collection.
    :param collection_name: The name of the collection.
    :param edge_name: The edge name.
    :param files: list of files.
    """
    coll_id = get_collection_id(scope=collection_scope, name=collection_name, session=session)
    edge_id = get_edge_id(edge_name=edge_name, session=session)

    for file in files:
        try:
            add_content(scope=file['scope'], name=file['name'], min_id=file['min_id'],
                        max_id=file['max_id'], coll_id=coll_id, content_type=file['content_type'],
                        status=file['status'], priority=file['priority'], edge_id=edge_id,
                        pfn_size=file['pfn_size'] if 'pfn_size' in file else 0,
                        pfn=file['pfn'] if 'pfn' in file else None,
                        object_metadata=file['object_metadata'] if 'object_metadata' in file else None,
                        session=session)
        except exceptions.DuplicatedObject:
            pass


@transactional_session
def update_content(scope, name, min_id=None, max_id=None, edge_name=None, edge_id=None, content_id=None, parameters=None, session=None):
    """
    update a collection content.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param min_id: The minimum id of the partial file, related to the whole file.
    :param max_id: The maximum id of the partial file, related to the whole file.
    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.

    :returns: content id.
    """
    content = get_content(scope, name, min_id=min_id, max_id=max_id,
                          edge_name=edge_name, edge_id=edge_id, session=session)

    try:
        content = get_content(scope, name, min_id=min_id, max_id=max_id, content_id=content_id,
                              edge_name=edge_name, edge_id=edge_id, session=session)

        if 'content_type' in parameters and \
           (isinstance(parameters['content_type'], str) or isinstance(parameters['content_type'], unicode)):
            parameters['content_type'] = ContentType.from_sym(str(parameters['content_type']))

        if 'status' in parameters and \
           (isinstance(parameters['status'], str) or isinstance(parameters['status'], unicode)):
            parameters['status'] = ContentStatus.from_sym(str(parameters['status']))

        content.update(parameters)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)

    return content.content_id


@transactional_session
def update_contents(files, session=None):
    """
    update a collection content.

    :param files: The list of content files.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """

    try:
        for file in files:
            parameters = {'status': file.status,
                          'pfn_size': file.pfn_size,
                          'pfn': file.pfn}
            update_content(cope=file.scope, name=file.name, min_id=file.min_id, max_id=file.max_id, edge_id=file.edge_id,
                           content_id=file.content_id,
                           parameters=parameters, session=session)
    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)


@transactional_session
def update_contents_by_id(files, session=None):
    """
    update a collection content.

    :param files: The list of content files.
    :param session: The database session in use.

    :raises NoObject: If no content is founded.
    :raises DatabaseException: If there is a database error.
    """

    try:
        for id in files:
            parameters = files[id]

            if 'status' in parameters and \
               (isinstance(parameters['status'], str) or isinstance(parameters['status'], unicode)):
                parameters['status'] = ContentStatus.from_sym(str(parameters['status']))

            session.query(models.CollectionContent).filter_by(content_id=id).update(parameters)

    except DatabaseError as error:
        raise exceptions.DatabaseException(error.args)


@read_session
def get_content(scope, name, min_id=None, max_id=None, edge_name=None, edge_id=None, content_id=None, session=None):
    """
    Get a collection content or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param min_id: The minimum id of the partial file, related to the whole file.
    :param max_id: The maximum id of the partial file, related to the whole file.
    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Content model.
    """

    try:
        if content_id:
            content = session.query(models.CollectionContent).filter_by(content_id=content_id).one()
        else:
            if not edge_id:
                edge_id = get_edge_id(edge_name=edge_name, session=session)

            if min_id is not None and max_id is not None:
                content = session.query(models.CollectionContent).filter_by(scope=scope, name=name, edge_id=edge_id,
                                                                            min_id=min_id, max_id=max_id).one()
            else:
                content_type = ContentType.FILE
                content = session.query(models.CollectionContent).filter_by(scope=scope, name=name, content_type=content_type, edge_id=edge_id).one()

        content['content_type'] = content.content_type
        content['status'] = content.status
        return content
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Content %s:%s[%s:%s](at edge %s) cannot be found' % (scope, name, min_id, max_id, edge_id))


@read_session
def get_content_best_match(scope, name, min_id=None, max_id=None, edge_name=None, edge_id=None, status=None, session=None):
    """
    Get a collection content or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param min_id: The minimum id of the partial file, related to the whole file.
    :param max_id: The maximum id of the partial file, related to the whole file.
    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Content model.
    """

    try:
        if not edge_id and edge_name:
            edge_id = get_edge_id(edge_name=edge_name, session=session)

        if status and (isinstance(status, str) or isinstance(status, unicode)):
            status = ContentStatus.from_sym(str(status))

        query = session.query(models.CollectionContent).filter_by(scope=scope, name=name)
        if status:
            query = query.filter_by(status=status)
        if edge_id:
            query = query.filter_by(edge_id=edge_id)

        if min_id is not None and max_id is not None:
            contents = query.filter(and_(CollectionContent.min_id <= min_id, CollectionContent.max_id >= max_id)).all()
            content = None
            for row in contents:
                if (not content) or (content['max_id'] - content['min_id'] > row['max_id'] - row['min_id']):
                    content = row
            if content is None:
                raise sqlalchemy.orm.exc.NoResultFound()
        else:
            content = query.filter_by(content_type=ContentType.FILE).one()

        content['content_type'] = content.content_type
        content['status'] = content.status
        return content
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Content %s:%s[%s:%s](at edge %s) cannot be found' % (scope, name, min_id, max_id, edge_id))


@read_session
def get_contents_by_edge(edge_name, edge_id=None, status=None, coll_id=None, content_type=None,
                         collection_scope=None, collection_name=None, limit=None, session=None):
    """
    Get a collection content or raise a NoObject exception.

    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param status: The status of the content.
    :param coll_id: The collection id.
    :param content_type: The tyep of the content.
    :param limit: Number to return limited items.
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.

    :returns: Content model.
    """

    try:
        if not edge_id:
            edge_id = get_edge_id(edge_name=edge_name, session=session)
        if not coll_id and (collection_scope and collection_name):
            coll_id = get_collection_id(collection_scope, collection_name)

        query = session.query(models.CollectionContent).filter_by(edge_id=edge_id)
        if status:
            if isinstance(status, str) or isinstance(status, unicode):
                status = ContentStatus.from_sym(str(status))
            query = query.filter_by(status=status)
        if coll_id:
            query = query.filter_by(coll_id=coll_id)
        if content_type:
            query = query.filter_by(content_type=content_type)
        if limit:
            query = query.limit(limit)

        contents = query.all()

        for content in contents:
            content['content_type'] = content.content_type
            content['status'] = content.status
        return contents
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('No contents at edge %s with status %s' % (edge_name, status))


@read_session
def get_contents_statistics(edge_name, edge_id=None, coll_id=None, status=None, content_type=None, session=None):
    """
    Get content statistics.

    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param coll_id: The collection id.
    :param status: The status of the content.
    :param content_type: The tyep of the content.
    :param session: The database session in use.

    :returns: dict.
    """

    try:
        if not edge_id and edge_name:
            edge_id = get_edge_id(edge_name=edge_name, session=session)

        query = session.query(models.CollectionContent.edge_id, models.CollectionContent.coll_id,
                              models.CollectionContent.content_type, models.CollectionContent.status,
                              func.count(1).label('counter'))
        if edge_id:
            query = query.filter_by(edge_id=edge_id)
        if coll_id:
            query = query.filter_by(coll_id=coll_id)
        if status:
            if isinstance(status, str) or isinstance(status, unicode):
                status = ContentStatus.from_sym(str(status))
            query = query.filter_by(status=status)
        if content_type:
            if isinstance(content_type, str) or isinstance(content_type, unicode):
                content_type = ContentType.from_sym(str(content_type))
            query = query.filter_by(content_type=content_type)

        query = query.group_by(models.CollectionContent.edge_id, models.CollectionContent.coll_id,
                               models.CollectionContent.content_type, models.CollectionContent.status)
        statistics = query.all()

        return statistics
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Failed to get statistics for edge %s and collection %s' % (edge_id, coll_id))


@transactional_session
def delete_content(scope, name, edge_name=None, edge_id=None, content_id=None, session=None):
    """
    Delete a collection content or raise a NoObject exception.

    :param scope: The scope of the collection data.
    :param name: The name of the collection data.
    :param edge_name: The name of the edge.
    :param edge_id: The id of the Edge
    :param session: The database session in use.

    :raises NoObject: If no edge is founded.
    """

    try:
        if content_id:
            session.query(models.CollectionContent).filter_by(content_id=content_id).delete()
        else:
            if not edge_id:
                edge_id = get_edge_id(edge_name=edge_name, session=session)

            session.query(models.CollectionContent).filter_by(scope=scope, name=name, edge_id=edge_id).delete()
    except sqlalchemy.orm.exc.NoResultFound:
        raise exceptions.NoObject('Contents %s:%s(at edge %s) cannot be found' % (scope, name, edge_id))

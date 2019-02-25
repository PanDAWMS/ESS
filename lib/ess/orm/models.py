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
SQLAlchemy models for ess relational data
"""

import datetime

from sqlalchemy import BigInteger, Boolean, Column, DateTime, Integer, String as _String, UniqueConstraint, event, DDL
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import backref, object_mapper, relationship
from sqlalchemy.schema import CheckConstraint, ForeignKeyConstraint, Index, PrimaryKeyConstraint, Sequence, Table

from ess.common.utils import date_to_str
from ess.orm.enum import EnumSymbol
from ess.orm.types import JSON
from ess.orm.session import BASE
from ess.orm.constants import (SCOPE_LENGTH, NAME_LENGTH,
                               EdgeType, EdgeStatus, CollectionType, CollectionStatus, CollectionReplicasStatus,
                               ContentType, ContentStatus, DataType, GranularityType, RequestStatus)


# Recipe to for str instead if unicode
# https://groups.google.com/forum/#!msg/sqlalchemy/8Xn31vBfGKU/bAGLNKapvSMJ
def String(*arg, **kw):
    kw['convert_unicode'] = 'force'
    return _String(*arg, **kw)


@compiles(Boolean, "oracle")
def compile_binary_oracle(type_, compiler, **kw):
    return "NUMBER(1)"


@event.listens_for(Table, "after_create")
def _psql_autoincrement(target, connection, **kw):
    if connection.dialect.name == 'mysql' and target.name == 'ess_coll':
        DDL("alter table ess_coll modify coll_id bigint(20) not null unique auto_increment")


class ModelBase(object):
    """Base class for ESS Models"""

    @declared_attr
    def __table_args__(cls):  # pylint: disable=no-self-argument
        # pylint: disable=maybe-no-member
        return cls._table_args + (CheckConstraint('CREATED_AT IS NOT NULL', name=cls.__tablename__.upper() + '_CREATED_NN'),
                                  CheckConstraint('UPDATED_AT IS NOT NULL', name=cls.__tablename__.upper() + '_UPDATED_NN'))

    @declared_attr
    def created_at(cls):  # pylint: disable=no-self-argument
        return Column("created_at", DateTime, default=datetime.datetime.utcnow)

    @declared_attr
    def updated_at(cls):  # pylint: disable=no-self-argument
        return Column("updated_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    @declared_attr
    def accessed_at(cls):  # pylint: disable=no-self-argument
        return Column("accessed_at", DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def save(self, flush=True, session=None):
        """Save this object"""
        session.add(self)
        if flush:
            session.flush()

    def delete(self, flush=True, session=None):
        """Delete this object"""
        session.delete(self)
        if flush:
            session.flush()

    def update(self, values, flush=True, session=None):
        """dict.update() behaviour."""
        for k, v in values.iteritems():
            self[k] = v
        if session and flush:
            session.flush()

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __iter__(self):
        self._i = iter(object_mapper(self).columns)
        return self

    def next(self):
        n = self._i.next().name
        return n, getattr(self, n)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def to_dict(self):
        return {key: self._expand_item(value) for key, value
                in self.__dict__.items() if not key.startswith('_')}

    @classmethod
    def _expand_item(cls, obj):
        """
        Return a valid representation of `obj` depending on its type.
        """
        if isinstance(obj, datetime.datetime):
            return date_to_str(obj)
        elif isinstance(obj, (datetime.time, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return obj.days * 24 * 60 * 60 + obj.seconds
        elif isinstance(obj, EnumSymbol):
            return obj.description

        return obj


class Edge(BASE, ModelBase):
    """Represents an ESS EDGE"""
    __tablename__ = 'ess_edges'
    edge_id = Column(Integer, Sequence('ESS_EDGE_ID_SEQ'))
    edge_name = Column(String(30))
    edge_type = Column(EdgeType.db_type(name='ESS_EDGES_TYPE_CHK'), default=EdgeType.EDGE)
    status = Column(EdgeStatus.db_type(name='ESS_EDGES_STATUS_CHK'), default=EdgeStatus.ACTIVE)
    is_independent = Column(Boolean(name='ESS_EDGES_INDEPENDENT_CHK'), default=False)
    continent = Column(String(2))
    country_name = Column(String(30))
    region_code = Column(String(2))
    city = Column(String(30))
    longitude = Column(String(25))
    latitude = Column(String(25))
    total_space = Column(BigInteger)
    used_space = Column(BigInteger)
    reserved_space = Column(BigInteger)
    num_files = Column(BigInteger)
    _table_args = (PrimaryKeyConstraint('edge_id', name='ESS_EDGES_PK'),
                   UniqueConstraint('edge_name', name='ESS_EDGES_EDGENAME_UQ'),
                   CheckConstraint('edge_name IS NOT NULL', name='ESS_EDGE_NAME_NN'),
                   CheckConstraint('edge_type IS NOT NULL', name='ESS_EDGE_TYPE_NN'),
                   Index('ESS_EDGE_NAME_IDX', 'edge_id', 'edge_name', 'status'))


class Collection(BASE, ModelBase):
    """Represents collections of files"""
    __tablename__ = 'ess_coll'
    coll_id = Column(BigInteger, Sequence('ESS_COLL_ID_SEQ'), nullable=False, unique=True, autoincrement=True)
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    collection_type = Column(CollectionType.db_type(name='ESS_COLL_TYPE'), default=CollectionType.DATASET)
    coll_size = Column(BigInteger)
    num_replicas = Column(Integer)
    global_status = Column(CollectionStatus.db_type(name='ESS_COLL_GLOBAL_STATUS'), default=CollectionStatus.NEW)
    total_files = Column(BigInteger)
    coll_metadata = Column(JSON())

    _table_args = (PrimaryKeyConstraint('coll_id', name='ESS_COLL_PK'),
                   # PrimaryKeyConstraint('scope', 'name', 'coll_id', name='ESS_COLL_PK'),
                   CheckConstraint('status IS NOT NULL', name='ESS_COLL_GLOBAL_STATUS_NN'),
                   UniqueConstraint('scope', 'name', name='ESS_COLL_UQ'),
                   Index('ESS_COLL_SCOPE_NAEM_IDX', 'scope', 'name'),
                   Index('ESS_COLL_IDX', 'coll_id'))


class CollectionReplicas(BASE, ModelBase):
    """Represents collection replicas of files"""
    __tablename__ = 'ess_coll2edge'
    coll_id = Column(BigInteger)
    edge_id = Column(Integer)
    status = Column(CollectionReplicasStatus.db_type(name='ESS_COLL2EDGE_STATUS'), default=CollectionReplicasStatus.NEW)
    transferring_files = Column(BigInteger)
    replicated_files = Column(BigInteger)
    num_active_requests = Column(Integer())
    retries = Column(Integer())
    edge = relationship("Edge", backref=backref('ess_coll2edge', order_by="Edge.edge_id"))
    collection = relationship("Collection", backref=backref('ess_coll2edge', order_by="Collection.coll_id"))
    _table_args = (PrimaryKeyConstraint('coll_id', 'edge_id', name='ESS_COLL2EDGES_PK'),
                   ForeignKeyConstraint(['coll_id'], ['ess_coll.coll_id'], name='ESS_COLL2EDGES_COLL_ID_FK'),
                   ForeignKeyConstraint(['edge_id'], ['ess_edges.edge_id'], name='ESS_COLL2EDGES_EDGE_ID_FK'))


class CollectionContent(BASE, ModelBase):
    """Represents files"""
    __tablename__ = 'ess_coll_content'
    content_id = Column(BigInteger, Sequence('ESS_CONTENT_ID_SEQ'))
    coll_id = Column(BigInteger)
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    min_id = Column(BigInteger)
    max_id = Column(BigInteger)
    content_type = Column(ContentType.db_type(name='ESS_CONTENT_TYPE'), default=ContentType.FILE)
    # size = Column(BigInteger)
    # md5 = Column(String(32))
    # adler32 = Column(String(8))
    edge_id = Column(Integer)
    status = Column(ContentStatus.db_type(name='ESS_CONTENT_STATUS'), default=ContentStatus.NEW)
    priority = Column(Integer())
    num_success = Column(Integer())
    num_failure = Column(Integer())
    last_failed_at = Column(DateTime)
    pfn_size = Column(BigInteger)
    pfn = Column(String(1024))
    object_metadata = Column(JSON())
    _table_args = (PrimaryKeyConstraint('content_id', name='ESS_COLL_CONTENT_PK'),
                   # PrimaryKeyConstraint('scope', 'name', 'coll_id', 'content_type', 'min_id', 'max_id', 'edge_id', 'content_id', name='ESS_COLL_CONTENT_PK'),
                   ForeignKeyConstraint(['edge_id'], ['ess_edges.edge_id'], name='ESS_CONTENT_EDGE_ID_FK'),
                   ForeignKeyConstraint(['coll_id'], ['ess_coll.coll_id'], name='ESS_CONTENT_COLL_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='ESS_CONTENT_STATUS_NN'),
                   UniqueConstraint('scope', 'name', 'coll_id', 'content_type', 'min_id', 'max_id', 'edge_id', name='ESS_CONTENT_UQ'),
                   Index('ESS_CONTENT_SCOPE_NAME_IDX', 'scope', 'name', 'edge_id', 'status'),
                   Index('ESS_CONTENT_SCOPE_NAME_MINMAX_IDX', 'scope', 'name', 'content_type', 'min_id', 'max_id', 'edge_id', 'status'),
                   Index('ESS_CONTENT_COLLECTION_ID_IDX', 'coll_id', 'status'),
                   Index('ESS_CONTENT_EDGE_ID_IDX', 'edge_id', 'status'),
                   Index('ESS_CONTENT_STATUS_PRIORITY_IDX', 'status', 'priority'))


class Request(BASE, ModelBase):
    """Represents a pre-cache request from other service"""
    __tablename__ = 'ess_requests'
    request_id = Column(BigInteger, Sequence('ESS_REQUEST_ID_SEQ'))
    scope = Column(String(SCOPE_LENGTH))
    name = Column(String(NAME_LENGTH))
    data_type = Column(DataType.db_type(name='ESS_REQUESTS_DATA_TYPE'), default=DataType.DATASET)
    granularity_type = Column(GranularityType.db_type(name='ESS_REQUESTS_GRANULARITY_TYPE'), default=GranularityType.PARTIAL)
    granularity_level = Column(Integer())
    priority = Column(Integer())
    edge_id = Column(Integer)
    status = Column(RequestStatus.db_type(name='ESS_REQUESTS_STATUS'), default=RequestStatus.NEW)
    request_meta = Column(JSON())  # task id, job id, pandq queues inside
    processing_meta = Column(JSON())  # collection_id or file_id inside
    errors = Column(JSON())
    _table_args = (PrimaryKeyConstraint('request_id', name='ESS_REQUESTS_PK'),
                   ForeignKeyConstraint(['edge_id'], ['ess_edges.edge_id'], name='ESS_REQUESTS_EDGE_ID_FK'),
                   CheckConstraint('status IS NOT NULL', name='ESS_REQ_STATUS_ID_NN'),
                   Index('ESS_REQUESTS_SCOPE_NAME_IDX', 'scope', 'name', 'data_type', 'request_id'),
                   Index('ESS_REQUESTS_STATUS_PRIORITY_IDX', 'status', 'priority', 'request_id'))


def register_models(engine):
    """
    Creates database tables for all models with the given engine
    """

    models = (Edge,
              Collection,
              CollectionReplicas,
              CollectionContent,
              Request)

    for model in models:
        model.metadata.create_all(engine)   # pylint: disable=maybe-no-member


def unregister_models(engine):
    """
    Drops database tables for all models with the given engine
    """

    models = (Edge,
              Collection,
              CollectionReplicas,
              CollectionContent,
              Request)

    for model in models:
        model.metadata.drop_all(engine)   # pylint: disable=maybe-no-member

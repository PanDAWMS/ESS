#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import traceback

from ess.common.constants import Sections
from ess.common.exceptions import NoObject, NoRequestedData, NoSuitableEdges, NoPluginException, DaemonPluginError
from ess.common.utils import setup_logging
from ess.core.catalog import add_collection, get_collection
from ess.core.edges import get_edges
from ess.core.requests import get_requests, update_request
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import EdgeStatus, RequestStatus, CollectionType, CollectionStatus

setup_logging(__name__)


class Broker(BaseDaemon):
    """
    The Broker daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Broker, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.Broker

        self.setup_logger()

        self.edges = None

    def get_resources(self):
        self.edges = get_edges(status=EdgeStatus.ACTIVE)

    def get_tasks(self):
        """
        Get tasks to process
        """

        requests = get_requests(status=RequestStatus.NEW)

        self.logger.info("Main thread get %s tasks" % len(requests))
        for req in requests:
            req.errors = None
            self.tasks.put(req)
            update_request(request_id=req.request_id, parameters={'status': RequestStatus.BROKERING})
        if requests:
            self.get_resources()

    def get_request_data_info(self, req):
        """
        Based on the request's scope and name, find the requested dataset and the size of requested dataset.
        """
        try:
            collection = get_collection(req.scope, req.name)
        except NoObject as error:
            self.logger.error("Request dataset(%s:%s) is not registered in ESS, will look for it for data management system" %
                              (req.scope, req.name))

            if 'datafinder' in self.plugins:
                try:
                    data = self.plugins['datafinder'].find_dataset(req.scope, req.name)
                    collection = {'scope': req.scope,
                                  'name': req.name,
                                  'collection_type': data.get('collection_type', CollectionType.DATASET),
                                  'coll_size': data.get('size', 0),
                                  'global_status': CollectionStatus.NEW,
                                  'total_files': data.get('total_files', 0)}
                except Exception as error:
                    self.logger.critical("Request dataset(%s:%s) cannot be found from data management system: %s, %s" %
                                         (req.scope, req.name, error, traceback.format_exc()))
                    raise DaemonPluginError("Request dataset cannot be found from data management system")

                collection_id = add_collection(**collection)
                collection = get_collection(req.scope, req.name, coll_id=collection_id)
            else:
                self.logger.critical("No available data finder plugins")
                raise NoPluginException("No available data finder plugins")

        return collection

    def broker_request(self, req, collection):
        """
        Broker the req to one edge service which will serve this request.
        """
        edges_canditates = []
        for edge in self.edges:
            free_space = edge.total_space - edge.used_space - edge.reserved_space
            if free_space > collection.coll_size:
                edges_canditates.append(edge)
        if not edges_canditates:
            self.logger.info("No edges available with enough space for this request.")
            req.status = RequestStatus.WAITING
            req.errors = {'message': 'No edges available with enough space for this request'}
            raise NoSuitableEdges('No edges available with enough space for this request')

        if 'requestbroker' in self.plugins:
            try:
                edge = self.plugins['requestbroker'].broker_request(req, collection, edges_canditates)
            except Exception as error:
                self.logger.error("Broker plugin throws an exception: %s" % (str(error), traceback.format_exc()))
                raise DaemonPluginError("Broker plugin throws an exception: %s" % (str(error), traceback.format_exc()))
        else:
            self.logger.critical("No available request broker plugins")
            raise NoPluginException("No available request broker plugins")
        return edge

    def process_task(self, req):
        """
        Process request
        """
        try:
            collection = self.get_request_data_info(req)
        except NoRequestedData as error:
            req.status = RequestStatus.ERROR
            req.errors = {'message': str(error)}
            return req

        try:
            edge = self.broker_request(req, collection)
            req.status = RequestStatus.ASSIGNING
            req.edge_id = edge.edge_id

            req.processing_meta = {}
            req.processing_meta['coll_id'] = collection.coll_id
            req.processing_meta['collection_type'] = collection.collection_type
            req.processing_meta['size'] = collection.coll_size
            req.processing_meta['total_files'] = collection.total_files
            req.processing_meta['collection_status'] = collection.global_status

        except NoSuitableEdges as error:
            req.status = RequestStatus.WAITING
            req.errors = {'message': str(error)}
            return req
        except DaemonPluginError as error:
            req.status = RequestStatus.ERROR
            req.errors = {'message': str(error)}
            return req
        except Exception as error:
            self.logger.error("Broker plugin throws an unknown exception: %s, %s" % (str(error), traceback.format_exc()))
            req.status = RequestStatus.ERROR
            req.errors = {'message': str(error)}
            return req
        return req

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        while not self.finished_tasks.empty():
            req = self.finished_tasks.get()
            self.logger.info("Main thread finishing task: %s" % req)
            try:
                parameters = {'status': req.status, 'edge_id': req.edge_id}
                parameters['errors'] = req.errors
                if req.processing_meta:
                    parameters['processing_meta'] = req.processing_meta
                self.logger.info("Updating request %s: %s" % (req.request_id, parameters))
                update_request(request_id=req.request_id, parameters=parameters)
            except Exception as error:
                self.logger.critical("Failed to update request %s: %s, %s" % (req, error, traceback.format_exc()))


if __name__ == '__main__':
    daemon = Broker()
    daemon.run()

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
from ess.common.constants import Sections
from ess.common.exceptions import DuplicatedObject, NoObject, ESSException
from ess.common.utils import setup_logging
from ess.core.catalog import add_collection, get_collection_id
from ess.core.edges import get_edge_id
from ess.core.requests import add_request, get_requests, update_request
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import RequestStatus

setup_logging(__name__)


class Assigner(BaseDaemon):
    """
    The Assigner daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Assigner, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.Assigner

        self.setup_logger()

        self.resource_name = self.get_resouce_name()
        head_service = self.get_head_service()
        if head_service:
            self.head_client = Client(head_service)
        else:
            self.head_client = None

    def assign_local_requests(self):
        reqs = get_requests(edge_name=self.resource_name, status=RequestStatus.ASSIGNING)
        for req in reqs:
            req.status = RequestStatus.ASSIGNED
            update_request(req.request_id, {'status': req.status})

    def assign_remote_requests(self):
        if not self.head_client:
            return

        reqs = []
        try:
            reqs = self.head_client.get_requests(edge_name=self.resource_name, status=str(RequestStatus.ASSIGNING))
            if not reqs:
                reqs = []
        except NoObject as error:
            self.logger.info("Got no requests: %s" % str(error))
        except ESSException as error:
            self.logger.info("Caught exception when get requests from the head service: %s" % str(error))

        for req in reqs:
            coll = self.head_client.get_collection(req['scope'], req['name'])
            collection = {'scope': coll['scope'],
                          'name': coll['name'],
                          'collection_type': coll['collection_type'],
                          'coll_size': coll['coll_size'],
                          'global_status': coll['global_status'],
                          'total_files': coll['total_files'],
                          'num_replicas': coll['num_replicas'],
                          'coll_metadata': coll['coll_metadata']}
            try:
                coll_id = add_collection(**collection)
            except DuplicatedObject as error:
                self.logger.info("Collection is already registered: %s, %s" % (collection, error))
                coll_id = get_collection_id(scope=coll['scope'], name=coll['name'])

            req['status'] = RequestStatus.ASSIGNED
            processing_meta = req['processing_meta']
            processing_meta['original_request_id'] = req['request_id']
            processing_meta['coll_id'] = coll_id
            request = {'scope': req['scope'],
                       'name': req['name'],
                       'data_type': req['data_type'],
                       'granularity_type': req['granularity_type'],
                       'granularity_level': req['granularity_level'],
                       'priority': req['priority'],
                       'edge_id': get_edge_id(self.resource_name),
                       'status': req['status'],
                       'request_meta': req['request_meta'],
                       'processing_meta': processing_meta,
                       'errors': req['errors']}
            try:
                add_request(**request)
            except DuplicatedObject as error:
                self.logger.info("Request is already registered: %s, %s" % (request, error))

            self.head_client.update_request(req['request_id'], status=str(req['status']))

    def get_tasks(self):
        """
        Get tasks to process
        """

        tasks = [{'name': 'assign_local_requests'}, {'name': 'assign_remote_requests'}]

        self.logger.info("Main thread get %s tasks" % len(tasks))
        for task in tasks:
            self.tasks.put(task)

    def process_task(self, req):
        """
        Process task
        """
        if req['name'] == 'assign_local_requests':
            self.assign_local_requests()
        if req['name'] == 'assign_remote_requests':
            self.assign_remote_requests()

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        self.graceful_stop.wait(30)


if __name__ == '__main__':
    daemon = Assigner()
    daemon.run()

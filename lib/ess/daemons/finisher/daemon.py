#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import datetime
import json
import time
import traceback

from ess.client.client import Client
from ess.common.constants import Sections
from ess.common.exceptions import ESSException
from ess.common.utils import setup_logging, date_to_str
from ess.core.catalog import get_contents_statistics, get_contents_by_edge
from ess.core.requests import get_requests, update_request
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import RequestStatus, ContentType, ContentStatus, GranularityType

setup_logging(__name__)


class Finisher(BaseDaemon):
    """
    The Finisher daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Finisher, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.Finisher

        self.setup_logger()

        self.resource_name = self.get_resouce_name()
        head_service = self.get_head_service()
        if head_service:
            self.head_client = Client(head_service)
        else:
            self.head_client = None

        if hasattr(self, 'send_messaging') and self.send_messaging:
            self.send_messaging = True
        else:
            self.send_messaging = False

    def sync_contents(self, collection_scope, collection_name, edge_name, edge_id, coll_id):
        """
        Synchronize the content to the head service.
        """
        if not self.head_client:
            return

        contents = get_contents_by_edge(edge_name=edge_name,
                                        edge_id=edge_id,
                                        coll_id=coll_id)

        contents_list = []
        for content in contents:
            cont = {'scope': content.scope,
                    'name': content.name,
                    'min_id': content.min_id,
                    'max_id': content.max_id,
                    'content_type': str(content.content_type),
                    'status': str(content.status),
                    'priority': content.priority,
                    'num_success': content.num_success,
                    'num_failure': content.num_failure,
                    'last_failed_at': content.last_failed_at,
                    'pfn_size': content.pfn_size,
                    'pfn': content.pfn,
                    'object_metadata': content.object_metadata}
            contents_list.append(cont)
        self.head_client.add_contents(collection_scope, collection_name, edge_name, contents_list)

    def finish_local_requests(self):
        reqs = get_requests(edge_name=self.resource_name, status=RequestStatus.SPLITTING)
        for req in reqs:
            if req.granularity_type == GranularityType.FILE:
                statistics = get_contents_statistics(edge_name=self.resource_name,
                                                     edge_id=req.edge_id,
                                                     coll_id=req.processing_meta['coll_id'],
                                                     content_type=ContentType.FILE)

                items = {}
                for item in statistics:
                    items[item.status] = items.counter
                if len(items.keys()) == 1 and items.keys()[0] == ContentStatus.AVAILABLE and items.values()[0] > 0:
                    self.logger.info('All files are available for request(%s): %s' % (req.request_id, items))

                    # To sync content info to the head service
                    self.sync_contents(collection_scope=req.scope,
                                       collection_name=req.name,
                                       edge_name=self.resource_name,
                                       edge_id=req.edge_id,
                                       coll_id=req.processing_meta['coll_id'])

                    req.status = RequestStatus.AVAILABLE
                    self.logger.info("Updating request %s to status %s" % (req.request_id, req.status))
                    update_request(req.request_id, {'status': req.status})

                    if self.send_messaging:
                        msg = {'event_type': 'REQUEST_DONE',
                               'payload': {'scope': req.scope,
                                           'name': req.name,
                                           'metadata': req.request_metadata},
                               'created_at': date_to_str(datetime.datetime.utcnow())}
                        self.logger.info("Sending a message to message broker: %s" % json.dumps(msg))
                        self.messaging_queue.put(msg)
                else:
                    self.logger.info('Not all files are available for request(%s): %s' % (req.request_id, items))
            if req.granularity_type == GranularityType.PARTIAL:
                statistics = get_contents_statistics(edge_name=self.resource_name,
                                                     edge_id=req.edge_id,
                                                     coll_id=req.processing_meta['coll_id'],
                                                     content_type=ContentType.PARTIAL)
                items = {}
                for item in statistics:
                    items[item.status] = item.counter
                if len(items.keys()) == 1 and items.keys()[0] == ContentStatus.AVAILABLE and items.values()[0] > 0:
                    self.logger.info('All partial files are available for request(%s): %s' % (req.request_id, items))

                    # To sync content info to the head service
                    self.sync_contents(collection_scope=req.scope,
                                       collection_name=req.name,
                                       edge_name=self.resource_name,
                                       edge_id=req.edge_id,
                                       coll_id=req.processing_meta['coll_id'])

                    req.status = RequestStatus.AVAILABLE
                    self.logger.info("Updating request %s to status %s" % (req.request_id, req.status))
                    update_request(req.request_id, {'status': req.status})

                    if self.send_messaging:
                        msg = {'event_type': 'REQUEST_DONE',
                               'payload': {'scope': req.scope,
                                           'name': req.name,
                                           'metadata': req.request_meta},
                               'created_at': date_to_str(datetime.datetime.utcnow())}
                        self.logger.info("Sending a message to message broker: %s" % json.dumps(msg))
                        self.messaging_queue.put(msg)
                else:
                    self.logger.info('Not all partial files are available for request(%s): %s' % (req.request_id, items))

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            if self.send_messaging:
                self.start_messaging_broker()

            while not self.graceful_stop.is_set():
                try:
                    self.finish_local_requests()

                    for i in range(5):
                        time.sleep(1)
                except ESSException as error:
                    self.logger.error("Main thread ESSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()
        except Exception as error:
            self.logger.error("Main thread ESSException: %s, %s" % (str(error), traceback.format_exc()))

        if self.send_messaging:
            self.stop_messaging_broker()


if __name__ == '__main__':
    daemon = Finisher()
    daemon.run()

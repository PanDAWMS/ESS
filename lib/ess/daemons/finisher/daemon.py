#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from ess.common.constants import Sections
from ess.common.utils import setup_logging
from ess.core.catalog import get_contents_statistics
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

    def finish_local_requests(self):
        reqs = get_requests(edge_name=self.resource_name, status=RequestStatus.PRECACHED)
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
                    req.status = RequestStatus.AVAILABLE
                    update_request(req.request_id, {'status': req.status})
                else:
                    self.logger.info('Not all files are available for request(%s): %s' % (req.request_id, items))
            if req.granularity_type == GranularityType.PARTIAL:
                statistics = get_contents_statistics(edge_name=self.resource_name,
                                                     edge_id=req.edge_id,
                                                     coll_id=req.processing_meta['coll_id'],
                                                     content_type=ContentType.PARTIAL)
                items = {}
                for item in statistics:
                    items[item.status] = items.counter
                if len(items.keys()) == 1 and items.keys()[0] == ContentStatus.AVAILABLE and items.values()[0] > 0:
                    self.logger.info('All partial files are available for request(%s): %s' % (req.request_id, items))
                    req.status = RequestStatus.AVAILABLE
                    update_request(req.request_id, {'status': req.status})
                else:
                    self.logger.info('Not all partial files are available for request(%s): %s' % (req.request_id, items))

    def finish_remote_requests(self):
        pass

    def get_tasks(self):
        """
        Get tasks to process
        """

        tasks = [{'name': 'finish_local_requests'}, {'name': 'finish_remote_requests'}]

        self.logger.info("Main thread get %s tasks" % len(tasks))
        for task in tasks:
            self.tasks.put(task)

    def process_task(self, req):
        """
        Process task
        """
        if req['name'] == 'finish_local_requests':
            self.finish_local_requests()
        if req['name'] == 'finish_remote_requests':
            self.finish_remote_requests()

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        self.graceful_stop.wait(30)


if __name__ == '__main__':
    daemon = Finisher()
    daemon.run()

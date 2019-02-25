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
from ess.core.requests import get_requests_by_edge, update_request
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

    def assign_local_requests(self):
        reqs = get_requests_by_edge(self.resource_name)
        for req in reqs:
            req.status = RequestStatus.ASSIGNED
            update_request(req.request_id, {'status': req.status})

    def assign_remote_requests(self):
        pass

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
        self.graceful_stop.wait(600)


if __name__ == '__main__':
    daemon = Assigner()
    daemon.run()

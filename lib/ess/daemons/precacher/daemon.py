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
from ess.common.exceptions import NoRequestedData, NoPluginException, DaemonPluginError
from ess.common.utils import setup_logging
from ess.core.requests import get_requests, update_request
from ess.core.catalog import add_contents
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import ContentType, RequestStatus


setup_logging(__name__)


class PreCacher(BaseDaemon):
    """
    The PreCacher daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(PreCacher, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.PreCacher

        self.setup_logger()

        self.resource_name = self.get_resouce_name()

    def get_tasks(self):
        """
        Get tasks to process
        """
        requests = get_requests(status=RequestStatus.ASSIGNED, edge_name=self.resource_name)

        self.logger.info("Main thread get %s tasks" % len(requests))
        for req in requests:
            self.tasks.put(req)
            update_request(request_id=req.request_id, parameters={'status': RequestStatus.PRECACHING})

    def pre_cache(self, scope, name):
        if 'precache' in self.plugins:
            try:
                files = self.plugins['precache'].pre_cache(scope, name)
                return files
            except Exception as error:
                self.logger.error("Precacher plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Precacher plugin throws an exception: %s" % (error))
        else:
            self.logger.critical("No available pre-cache plugins")
            raise NoPluginException("No available pre-cache plugins")

    def process_task(self, req):
        """
        Process task
        """
        try:
            files = self.pre_cache(req.scope, req.name)
            ret_files = []
            for file in files:
                ret_file = {'scope': file['scope'],
                            'name': file['name'],
                            'min_id': file['min_id'],
                            'max_id': file['max_id'],
                            'content_type': ContentType.FILE,
                            'status': file['status'],
                            'priority': req.priority,
                            'pfn_size': file['size'],
                            'pfn': file['pfn'],
                            'object_metadata': {'md5': file['md5'], 'adler32': file['adler32']}}
                ret_files.append(ret_file)
            add_contents(req.scope, req.name, self.resource_name, ret_files)
            req.status = RequestStatus.PRECACHED
            req.processing_meta['collection_status'] = str(RequestStatus.PRECACHED)
            return req
        except NoRequestedData as error:
            req.status = RequestStatus.ERROR
            req.errors = {'message': str(error)}
            return req

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        while not self.finished_tasks.empty():
            req = self.finished_tasks.get()
            self.logger.info("Main thread finishing task: %s" % req)
            try:
                parameters = {'status': req.status}
                parameters['errors'] = req.errors
                if req.processing_meta:
                    parameters['processing_meta'] = req.processing_meta
                self.logger.info("Updating request %s: %s" % (req.request_id, parameters))
                update_request(request_id=req.request_id, parameters=parameters)
            except Exception as error:
                self.logger.critical("Failed to update request %s: %s, %s" % (req, error, traceback.format_exc()))

        self.graceful_stop.wait(10)


if __name__ == '__main__':
    daemon = PreCacher()
    daemon.run()

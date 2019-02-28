#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import signal
import time
import traceback
import Queue

from ess.common.constants import Sections
from ess.common.exceptions import ESSException, NoPluginException, DaemonPluginError
from ess.common.utils import setup_logging
from ess.core.catalog import add_contents, get_contents_by_edge, update_contents_by_id
from ess.core.requests import get_requests, update_request
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import ContentType, ContentStatus, RequestStatus, GranularityType

setup_logging(__name__)


class Splitter(BaseDaemon):
    """
    The Splitter daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Splitter, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.Splitter
        self.output_queue = Queue.Queue()

        self.setup_logger()

    def start_splitter_process(self):
        if 'splitter' in self.plugins:
            try:
                self.logger.info("Starting splitter plugin %s" % self.plugins['splitter'])
                self.plugins['splitter'].start()
                self.logger.info("Splitter plugin %s started" % self.plugins['splitter'])
            except Exception as error:
                self.logger.error("Splitter plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Splitter plugin throws an exception: %s" % (error))
        else:
            self.logger.critical("No available splitter plugins")
            raise NoPluginException("No available splitter plugins")

    def start_stagers(self):
        if 'stager' in self.plugins:
            try:
                self.logger.info("Starting stager plugin %s" % self.plugins['stager'])
                self.plugins['stager'].set_output_queue(self.output_queue)
                self.plugins['stager'].start()
                self.logger.info("Stager plugin %s started" % self.plugins['stager'])
            except Exception as error:
                self.logger.error("Stager plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Stager plugin throws an exception: %s" % (error))

    def prepare_split_request_task(self):
        """
        Prepare split request
        """
        requests = get_requests(status=RequestStatus.PRECACHED, edge_name=self.resource_name)

        if requests:
            self.logger.info("Main thread get %s split requests" % len(requests))

        for req in requests:
            self.logger.info("Prepare to_split files for request %s" % req.request_id)
            update_request(request_id=req.request_id, parameters={'status': RequestStatus.TOSPLITTING})
            self.prepare_to_split_files(req)
            update_request(request_id=req.request_id, parameters={'status': RequestStatus.SPLITTING})

    def prepare_to_split_files(self, req):
        if req.granularity_type == GranularityType.PARTIAL:
            coll_id = req.processing_meta['coll_id']
            files = get_contents_by_edge(edge_name=self.resource_name, coll_id=coll_id, content_type=ContentType.FILE)

            sub_files = []
            for file in files:
                for i in range(file.min_id, file.max_id + 1, req.granularity_level):
                    new_min_id = i
                    new_max_id = i + req.granularity_level - 1
                    if new_max_id > file.max_id:
                        new_max_id = file.max_id
                    new_file = {'coll_id': file.coll_id,
                                'scope': file.scope,
                                'name': file.name,
                                'min_id': new_min_id,
                                'max_id': new_max_id,
                                'content_type': ContentType.PARTIAL,
                                'edge_id': file.edge_id,
                                'status': ContentStatus.TOSPLIT,
                                'pfn': file.pfn,
                                'priority': file.priority}

                    sub_files.append(new_file)

            self.logger.debug("Creating new splitting files: %s" % sub_files)
            self.logger.info("Creating %s new splitting files" % len(sub_files))

            add_contents(req.scope, req.name, self.resource_name, sub_files)

    def get_splitter_tasks(self):
        """
        Get tasks to splitter
        """
        files = get_contents_by_edge(edge_name=self.resource_name, status=ContentStatus.TOSPLIT,
                                     content_type=ContentType.PARTIAL)

        # self.logger.debug("Main thread get %s files to split" % len(files))

        update_files = {}
        for file in files:
            update_files[file.content_id] = {'status': ContentStatus.SPLITTING}
        update_contents_by_id(update_files)

        return files

    def stage_out_outputs(self, outputs):
        """
        Stage out outputs
        """
        if 'stager' in self.plugins:
            try:
                self.logger.info("Staging out outputs: %s" % outputs)
                self.plugins['stager'].stage_out_outputs(outputs)
            except Exception as error:
                self.logger.error("Stager plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Stager plugin throws an exception: %s" % (error))
        else:
            self.logger.info("No stager plugin, will directly register the outputs")
            for output in outputs:
                self.output_queue.put(output)

    def finish_splitter_tasks(self, files):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        update_files = {}
        for file in files:
            update_files[file['content_id']] = {'status': ContentStatus.AVAILABLE,
                                                'pfn_size': file['size'],
                                                'pfn': file['pfn']}
        update_contents_by_id(update_files)

    def run(self):
        """
        Main run function.
        """
        signal.signal(signal.SIGTERM, self.stop)

        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.start_splitter_process()
            self.start_stagers()

            while not self.graceful_stop.is_set():
                try:
                    self.prepare_split_request_task()

                    if self.plugins['splitter'].need_more_requests():
                        self.logger.info("Splitter plugin needs more events")

                        files = self.get_splitter_tasks()
                        if files:
                            self.logger.info('Got %s files to split' % len(files))
                            self.plugins['splitter'].send_requests(files)

                    if self.plugins['splitter'].has_outputs():
                        self.logger.info("Splitter plugin has outputs")

                        outputs = self.plugins['splitter'].get_outputs()
                        self.logger.info('Got %s splitted outputs' % len(outputs))

                        self.stage_out_outputs(outputs)

                    if self.output_queue.qsize() > 0:
                        self.logger.info("Output stager has outputs")

                        staged_outputs = []
                        while not self.output_queue.empty():
                            staged_outputs.append(self.output_queue.get())
                        self.logger.info('Got %s staged outputs' % len(staged_outputs))

                        self.finish_splitter_tasks(staged_outputs)

                    time.sleep(5)
                except ESSException as error:
                    self.logger.error("Main thread ESSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    daemon = Splitter()
    daemon.run()

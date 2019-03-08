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
import Queue

from ess.common.constants import Sections
from ess.common.exceptions import ESSException, NoPluginException, DaemonPluginError
from ess.common.utils import setup_logging, date_to_str
from ess.core.catalog import get_contents_by_edge, update_contents_by_id
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import ContentStatus

setup_logging(__name__)


class Stager(BaseDaemon):
    """
    The Stager daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(Stager, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.Stager
        self.request_queue = Queue.Queue()
        self.finished_queue = Queue.Queue()

        self.setup_logger()

        if hasattr(self, 'send_messaging') and self.send_messaging:
            self.send_messaging = True
        else:
            self.send_messaging = False
        self.logger.info("Send messaging is defined as: %s" % self.send_messaging)

    def start_stagers(self):
        if 'stager' in self.plugins:
            try:
                self.logger.info("Starting stager plugin %s" % self.plugins['stager'])
                self.plugins['stager'].set_queues(self.request_queue, self.finished_queue)
                self.plugins['stager'].start()
                self.logger.info("Stager plugin %s started" % self.plugins['stager'])
            except Exception as error:
                self.logger.error("Stager plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Stager plugin throws an exception: %s" % (error))
        else:
            self.logger.critical("No available stager plugins")
            raise NoPluginException("No available stager plugins")

    def stop_stagers(self):
        if 'stager' in self.plugins:
            try:
                self.logger.info("Stopping stager plugin %s" % self.plugins['stager'])
                self.plugins['stager'].stop()
            except Exception as error:
                self.logger.error("Stager plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Stager plugin throws an exception: %s" % (error))

    def is_stagers_alive(self):
        if 'stager' in self.plugins:
            try:
                self.plugins['stager'].is_alive()
            except Exception as error:
                self.logger.error("Stager plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Stager plugin throws an exception: %s" % (error))
        return False

    def get_stager_tasks(self):
        """
        Get tasks to stage out.
        """
        files = get_contents_by_edge(edge_name=self.resource_name, status=ContentStatus.TOSTAGEDOUT, content_type=None)

        update_files = {}
        for file in files:
            update_files[file.content_id] = {'status': ContentStatus.STAGINGOUT}
        update_contents_by_id(update_files)

        for file in files:
            to_stageout = {'content_id': file.content_id,
                           'coll_id': file.coll_id,
                           'pfn_size': file.pfn_size,
                           'scope': file.scope,
                           'name': file.name,
                           'min_id': file.min_id,
                           'max_id': file.max_id,
                           'pfn': file.pfn}
            self.request_queue.put(to_stageout)

    def finish_stager_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """

        update_files = {}
        messages = []
        while not self.finished_queue.empty():
            file = self.finished_queue.get()
            update_files[file['content_id']] = {'status': ContentStatus.AVAILABLE,
                                                'pfn_size': file['pfn_size'],
                                                'pfn': file['pfn']}
            msg = {'event_type': 'FILE_AVAILABLE',
                   'payload': {'scope': file['scope'],
                               'name': file['name'],
                               'startEvent': file['min_id'],
                               'lastEvent': file['max_id'],
                               'pfn': file['pfn']},
                   'created_at': date_to_str(datetime.datetime.utcnow())}
            messages.append(msg)

        self.logger.info('Got %s staged outputs' % len(update_files))
        update_contents_by_id(update_files)

        if self.send_messaging:
            for msg in messages:
                self.logger.info("Sending a message to message broker: %s" % json.dumps(msg))
                self.messaging_queue.put(msg)

    def run(self):
        """
        Main run function.
        """
        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            self.start_stagers()
            if self.send_messaging:
                self.start_messaging_broker()

            while not self.graceful_stop.is_set():
                try:
                    if self.request_queue.qsize() < 1:
                        self.get_stager_tasks()

                    if self.finished_queue.qsize() > 0:
                        self.logger.info("Output stager has outputs")
                        self.finish_stager_tasks()

                    time.sleep(5)
                except ESSException as error:
                    self.logger.error("Main thread ESSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()
        except Exception as error:
            self.logger.error("Main thread ESSException: %s, %s" % (str(error), traceback.format_exc()))

        self.stop_stagers()
        while(self.is_stagers_alive()):
            time.sleep(1)
        if self.send_messaging:
            self.stop_messaging_broker()


if __name__ == '__main__':
    daemon = Stager()
    daemon.run()

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
Splitter plugin based ATLAS AthenaMP prefetcher
"""

import datetime
import json
import logging
import os
import threading
import time
import traceback
import yampl
import Queue

from ess.common.utils import run_process
from ess.daemons.common.plugin_base import PluginBase


class AtlasPrefetcher(threading.Thread):

    class MessageThread(threading.Thread):
        def __init__(self, messageQ, socketname, context='local', logger=None, **kwds):
            threading.Thread.__init__(self, **kwds)
            if logger:
                self.logger = logger
            else:
                self.logger = logging.getLogger(self.__class__.__name__)
            self.messageQ = messageQ
            self.num_messages_required = 0
            self._stop = threading.Event()
            try:
                self.__messageSrv = yampl.ServerSocket(socketname, context)
            except:  # noqa: B901
                self.logger.debug("Exception: failed to start yampl server socket: %s" % traceback.format_exc())

        def send(self, message):
            try:
                self.__messageSrv.send_raw(message)
                self.num_messages_required -= 1
            except:  # noqa: B901
                self.logger.debug("Exception: failed to send yampl message: %s" % traceback.format_exc())

        def stop(self):
            self._stop.set()

        def stopped(self):
            return self._stop.isSet()

        def __del__(self):
            if self.__messageSrv:
                del self.__messageSrv
                self.__messageSrv = None

        def run(self):
            try:
                while True:
                    if self.stopped():
                        if self.__messageSrv:
                            del self.__messageSrv
                            self.__messageSrv = None
                        break
                    size, buf = self.__messageSrv.try_recv_raw()
                    if size == -1:
                        time.sleep(0.00001)
                    else:
                        self.logger.info("Received message: %s" % buf)
                        if 'Ready for events' in buf:
                            self.num_messages_required += 1
                        else:
                            self.messageQ.put(buf)
            except:  # noqa: B901
                self.logger.debug("Exception: Message Thread failed: %s" % traceback.format_exc())
                if self.__messageSrv:
                    del self.__messageSrv
                    self.__messageSrv = None

    def __init__(self, request_queue, output_queue, process_name, process_cmd, stdout, stderr, num_threads):
        threading.Thread.__init__(self)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.graceful_stop = threading.Event()

        self.process_name = process_name
        self.process_cmd = process_cmd
        self.num_threads = num_threads
        self.stdout = stdout
        self.stderr = stderr
        self.process = None
        self.message_thread = None

        self.request_queue = request_queue
        self.output_queue = output_queue

    def start_prefetcher_process(self):
        """
        Start AthenaMP prefetcher process.
        """
        self.logger.info("Starting process(stdout: %s, stderr: %s): %s" % (self.stdout, self.stderr, self.process_cmd))
        self.process = run_process(self.process_cmd, stdout=self.stdout, stderr=self.stderr)

        self.logger.info("Starting messaging thread.")
        self.message_thread = AtlasPrefetcher.MessageThread(self.output_queue, socketname=self.process_name, logger=self.logger)
        self.message_thread.start()

    def stop(self):
        self.graceful_stop.set()

    def inject_request(self):
        if not self.request_queue.empty():
            req = self.request_queue.get(False)
            if req:
                # req = str([req])
                req = json.dumps([req])
                self.logger.info("Inject a message to prefetcher: %s" % req)
                self.message_thread.send(req)

    def run(self):
        self.start_prefetcher_process()
        time_logging = time.time()
        while not self.graceful_stop.is_set():
            ret_code = self.process.poll()
            if ret_code is None:
                if time.time() - time_logging > 60:
                    self.logger.info("Prefetcher process is still running.")
                    time_logging = time.time()
            else:
                self.logger.info("Prefecther process finished with %s" % ret_code)
                break

            while (self.message_thread.num_messages_required > 0 and not self.request_queue.empty()):
                self.inject_request()
            time.sleep(0.1)

        self.logger.info("Stopping messaging thread.")
        self.message_thread.stop()

        self.logger.info("Stopping prefetcher process.")
        self.process.terminate()
        while self.process.poll() is None:
            self.logger.info("Prefetcher is still running")
            time.sleep(5)
        self.logger.info("Prefetcher terminated")


class AtlasPrefetcherSplitter(PluginBase, threading.Thread):
    def __init__(self, **kwargs):
        threading.Thread.__init__(self)
        super(AtlasPrefetcherSplitter, self).__init__(**kwargs)
        self.setup_logger()
        self.graceful_stop = threading.Event()

        if not hasattr(self, 'num_threads'):
            self.num_threads = 1
        else:
            self.num_threads = int(self.num_threads)

        if not hasattr(self, 'output_prefix'):
            self.output_prefix = 'splitted_eventranges.pool.root'

        if not hasattr(self, 'splitter_template'):
            raise Exception('splitter_template is required but not defined.')
        if not hasattr(self, 'default_input'):
            raise Exception('default_input is required but not defined.')
        if not hasattr(self, 'log_dir'):
            raise Exception('log_dir is required but not defined')

        if not hasattr(self, 'stop_delay'):
            self.stop_delay = 180
        else:
            self.stop_delay = int(self.stop_delay)

        self.request_queue = Queue.Queue()
        self.output_queue = Queue.Queue()

        self.processes = []

    def get_splitter_process_name(self):
        if hasattr(self, 'splitter_name'):
            return self.splitter_name.replace('"', '').replace("'", "")
        return "EventRange_Prefetcher"

    def start_splitter_processes_old(self):
        """
        Start splitter process with multiple AthenaMP instances(one process per instance).
        """
        name = self.get_splitter_process_name()

        with open(self.splitter_template, 'r') as f:
            template = f.read()

        for i in range(self.num_threads):
            process_name = name + '_%s' % i
            process_cmd = template.format(process_name=process_name, num_processes=1, output_prefix=self.output_prefix,
                                          work_dir=self.log_dir, default_input=self.default_input)
            today_str = datetime.date.today().strftime("%Y-%m-%d")
            stdout = self.log_dir + '/' + process_name + '.stdout.' + today_str
            stderr = self.log_dir + '/' + process_name + '.stderr.' + today_str
            stdout = open(stdout, 'a+')
            stderr = open(stderr, 'a+')
            process = AtlasPrefetcher(self.request_queue, self.output_queue, process_name, process_cmd, stdout, stderr, self.num_threads)
            self.processes.append(process)

    def start_splitter_processes(self):
        """
        Start splitter process with one AthenaMP instance(multiple processes per instance).
        """
        name = self.get_splitter_process_name()

        with open(self.splitter_template, 'r') as f:
            template = f.read()

        work_dir = os.path.join(self.log_dir, datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S"))
        if not os.path.exists(work_dir):
            os.makedirs(work_dir)
        process_name = name
        process_cmd = template.format(process_name=process_name, num_processes=self.num_threads,
                                      output_prefix=self.output_prefix, work_dir=work_dir,
                                      default_input=self.default_input)

        # self.logger.info("Starting ATLAS prefetcher %s with command %s" % (process_name, process_cmd))

        stdout = work_dir + '/' + process_name + '.stdout'
        stderr = work_dir + '/' + process_name + '.stderr'
        stdout = open(stdout, 'a+')
        stderr = open(stderr, 'a+')
        process = AtlasPrefetcher(self.request_queue, self.output_queue, process_name, process_cmd, stdout, stderr, self.num_threads)
        process.start()
        self.processes.append(process)

    def send_requests(self, reqs):
        """
        Send splitting requests
        """
        self.logger.info("Sending %s requests to Atlas Prefetcher" % len(reqs))
        for req in reqs:
            event_range = {'eventRangeID': '%s-%s-%s-%s' % (req.coll_id, req.content_id, req.min_id, req.max_id),
                           'scope': req.scope,
                           'LFN': req.name,
                           'startEvent': req.min_id,
                           'lastEvent': req.max_id,
                           'GUID': '4857B452-50F4-A34E-A181-94CB673CEB63',
                           'PFN': req.pfn}
            self.request_queue.put(event_range)

    def get_queued_requests_num(self):
        self.request_queue.qsize()

    def need_more_requests(self):
        if self.request_queue.qsize() < int(self.num_threads):
            return True
        else:
            return False

    def has_outputs(self):
        return self.output_queue.qsize() > 0

    def get_outputs(self):
        """
        Get splitted outputs
        """
        ret = []
        while not self.output_queue.empty():
            output = self.output_queue.get()
            self.logger.debug("Got output message: %s" % output)
            pfn, id, cpu, wall = output.split(',')
            coll_id, content_id, min_id, max_id = id.split('-')
            coll_id = coll_id.replace('ID:', '')
            output_ret = {'content_id': int(content_id),
                          'coll_id': int(coll_id),
                          'size': os.path.getsize(pfn),
                          'pfn': pfn}
            ret.append(output_ret)
        return ret

    def stop(self):
        self.graceful_stop.set()

    def run(self):

        self.start_splitter_processes()
        while not self.graceful_stop.is_set():
            time.sleep(1)

        for process in self.processes:
            process.stop()

        while(len(self.processes)):
            self.processes = [p for p in self.processes if p.is_alive()]
            time.sleep(1)

#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import logging
import signal
import threading
import traceback
import Queue

from concurrent import futures
# from multiprocessing import Process
from threading import Thread

from ess.common.constants import Sections
from ess.common.config import config_has_section, config_list_options, config_get
from ess.common.exceptions import ESSException, DaemonPluginError
from ess.common.utils import setup_logging


setup_logging(__name__)


class BaseDaemon(Thread):
    """
    The base ESS daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(BaseDaemon, self).__init__()

        self.num_threads = num_threads
        self.graceful_stop = threading.Event()
        self.executors = futures.ThreadPoolExecutor(max_workers=num_threads)
        self.tasks = Queue.Queue()
        self.finished_tasks = Queue.Queue()

        self.config_section = Sections.BaseDaemon

        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.plugins = {}

        self.logger = None
        self.setup_logger()

        self.resource_name = self.get_resouce_name()

        self.messaging_queue = Queue.Queue()

    def setup_logger(self):
        """
        Setup logger
        """
        self.logger = logging.getLogger(self.__class__.__name__)

    def stop(self, signum=None, frame=None):
        """
        Graceful exit.
        """
        self.graceful_stop.set()

    def get_resouce_name(self):
        return config_get(Sections.ResourceManager, 'resource_name')

    def load_plugin_attributes(self, name, plugin):
        """
        Load plugin attributes
        """
        attrs = {}
        if config_has_section(self.config_section):
            options = config_list_options(self.config_section)
            for option, value in options:
                plugin_prefix = 'plugin.%s.' % name
                if option.startswith(plugin_prefix):
                    attr_name = option.replace(plugin_prefix, '')
                    if isinstance(value, str) and value.lower() == 'true':
                        value = True
                    if isinstance(value, str) and value.lower() == 'false':
                        value = False
                    attrs[attr_name] = value
        return attrs

    def load_plugin(self, name, plugin):
        """
        Load plugin attributes
        """
        attrs = self.load_plugin_attributes(name, plugin)
        self.logger.info("Loading plugin %s with attributes: %s" % (name, attrs))
        k = plugin.rfind('.')
        plugin_modules = plugin[:k]
        plugin_class = plugin[k + 1:]
        module = __import__(plugin_modules, fromlist=[None])
        cls = getattr(module, plugin_class)
        impl = cls(**attrs)
        return impl

    def load_plugins(self):
        """
        Load plugins
        """
        if config_has_section(self.config_section):
            options = config_list_options(self.config_section)
            for option, value in options:
                if option.startswith('plugin.'):
                    if option.count('.') == 1:
                        plugin_name = option.replace('plugin.', '').strip()
                        self.logger.info("Loading plugin %s with %s" % (plugin_name, value))
                        self.plugins[plugin_name] = self.load_plugin(plugin_name, value)

    def start_messaging_broker(self):
        if 'messaging' in self.plugins:
            try:
                self.logger.info("Starting messaging broker plugin %s" % self.plugins['messaging'])
                self.plugins['messaging'].set_request_queues(self.messaging_queue)
                self.plugins['messaging'].start()
                self.logger.info("Messaging broker plugin %s started" % self.plugins['stager'])
            except Exception as error:
                self.logger.error("Messaging broker plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Messaging broker plugin throws an exception: %s" % (error))

    def stop_messaging_broker(self):
        if 'messaging' in self.plugins:
            try:
                self.logger.info("Stopping messaging broker plugin %s" % self.plugins['messaging'])
                self.plugins['messaging'].stop()
            except Exception as error:
                self.logger.error("Messaging broker plugin throws an exception: %s, %s" % (error, traceback.format_exc()))
                raise DaemonPluginError("Messaging broker plugin throws an exception: %s" % (error))

    def get_tasks(self):
        """
        Get tasks to process
        """
        tasks = []
        self.logger.info("Main thread get %s tasks" % len(tasks))
        for task in tasks:
            self.tasks.put(task)

    def process_task(self, task):
        """
        Process task
        """
        task = self.plugin.process_task(task)
        return task

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        while not self.finished_tasks.empty():
            task = self.finished_tasks.get()
            self.logger.info("Main thread finishing task: %s" % task)

    def run_tasks(self, thread_id):
        log_prefix = "[Thread %s]: " % thread_id
        self.logger.info(log_prefix + "Starting worker thread")

        while not self.graceful_stop.is_set():
            try:
                if not self.tasks.empty():
                    task = self.tasks.get()
                    self.logger.info(log_prefix + "Got task: %s" % task)

                    try:
                        self.logger.info(log_prefix + "Processing task: %s" % task)
                        task = self.process_task(task)
                    except ESSException as error:
                        self.logger.error(log_prefix + "Caught an ESSException: %s" % str(error))
                    except Exception as error:
                        self.logger.critical(log_prefix + "Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

                    self.logger.info(log_prefix + "Put task to finished queue: %s" % task)
                    self.finished_tasks.put(task)
                else:
                    self.graceful_stop.wait(1)
            except Exception as error:
                self.logger.critical(log_prefix + "Caught an exception: %s\n%s" % (str(error), traceback.format_exc()))

    def sleep_for_tasks(self):
        """
        Sleep for tasks
        """
        if self.finished_tasks.empty() and self.tasks.empty():
            self.logger.info("Main thread will sleep 4 seconds")
            self.graceful_stop.wait(4)
        else:
            self.logger.info("Main thread will sleep 2 seconds")
            self.graceful_stop.wait(2)

    def run(self):
        """
        Main run function.
        """
        signal.signal(signal.SIGTERM, self.stop)

        try:
            self.logger.info("Starting main thread")

            self.load_plugins()

            for i in range(self.num_threads):
                self.executors.submit(self.run_tasks, i)

            while not self.graceful_stop.is_set():
                try:
                    self.get_tasks()
                    self.finish_tasks()
                    self.sleep_for_tasks()
                except ESSException as error:
                    self.logger.error("Main thread ESSException: %s" % str(error))
                except Exception as error:
                    self.logger.critical("Main thread exception: %s\n%s" % (str(error), traceback.format_exc()))
        except KeyboardInterrupt:
            self.stop()


if __name__ == '__main__':
    daemon = BaseDaemon()
    daemon.run()

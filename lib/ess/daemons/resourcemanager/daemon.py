#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import time
import traceback

from ess.client.client import Client
from ess.common.constants import Sections
from ess.common.exceptions import NoObject
from ess.common.utils import setup_logging, get_space_from_string
from ess.core.edges import register_edge, update_edge, clean_old_edges
from ess.daemons.common.basedaemon import BaseDaemon
from ess.orm.constants import EdgeStatus

setup_logging(__name__)


class ResourceManager(BaseDaemon):
    """
    The ResourceManager daemon class
    """

    def __init__(self, num_threads=1, **kwargs):
        super(ResourceManager, self).__init__(num_threads, **kwargs)

        self.config_section = Sections.ResourceManager

        self.setup_logger()

        if not hasattr(self, 'total_space'):
            self.total_space = 0
        else:
            self.total_space = get_space_from_string(self.total_space)
        if not hasattr(self, 'resource_check_period'):
            self.resource_check_period = 600
        else:
            self.resource_check_period = int(self.resource_check_period)

        if not hasattr(self, 'clean_edges_period'):
            self.clean_edges_period = 1200
        else:
            self.clean_edges_period = int(self.clean_edges_period)

        if self.clean_edges_period < self.resource_check_period * 2:
            self.logger.warning("clean_edges_period should be bigger thant resource_check_period*2, replace it with resource_check_period*2")
            self.clean_edges_period = self.resource_check_period * 2
        self.used_space = None
        self.num_files = 0
        self.sched_tasks = [{'name': 'resource_check', 'execute_time': time.time()},
                            {'name': 'clean_edges', 'execute_time': time.time()}]

    def get_tasks(self):
        """
        Get tasks to process
        """

        # if self.used_space is not None:
        #     self.graceful_stop.wait(1800)

        ret_tasks = []
        for task in self.sched_tasks:
            if task['execute_time'] < time.time():
                self.sched_tasks.remove(task)
                ret_tasks.append(task)
                if task['name'] == 'resource_check':
                    new_task = task.copy()
                    new_task['execute_time'] = time.time() + self.resource_check_period
                    self.sched_tasks.append(new_task)
                if task['name'] == 'clean_edges':
                    new_task = task.copy()
                    new_task['execute_time'] = time.time() + self.clean_edges_period
                    self.sched_tasks.append(new_task)

        self.logger.info("Main thread get %s tasks" % len(ret_tasks))
        for task in ret_tasks:
            self.tasks.put(task)

    def process_task(self, task):
        """
        Process task
        """
        if task['name'] == 'resource_check':
            if 'resourcechecker' in self.plugins:
                try:
                    self.used_space = self.plugins['resourcechecker'].resource_check(task)
                except Exception as error:
                    self.logger.error("Resource check plugin throws an exception: %s, %s" % (str(error), traceback.format_exc()))
                    self.used_space = 0
            else:
                self.logger.warn("No resource checker plugin. Used space will be set to 0.")
                self.used_space = 0
            return task

        if task['name'] == 'clean_edges':
            clean_old_edges(self.clean_edges_period)

    def finish_tasks(self):
        """
        Finish processing the finished tasks, for example, update db status.
        """
        while not self.finished_tasks.empty():
            task = self.finished_tasks.get()
            self.logger.info("Main thread finishing task: %s" % task)
            try:
                parameters = {'edge_type': self.edge_type, 'status': EdgeStatus.ACTIVE, 'is_independent': self.is_independent,
                              'continent': self.continent, 'country_name': self.country_name, 'region_code': self.region_code,
                              'city': self.city, 'longitude': self.longitude, 'latitude': self.latitude,
                              'total_space': self.total_space, 'used_space': self.used_space, 'num_files': self.num_files}
                self.logger.info("Updating edge %s with parameters: %s" % (self.get_resouce_name(), parameters))
                update_edge(edge_name=self.get_resouce_name(), parameters=parameters)
            except NoObject as error:
                self.logger.info("Edge %s doesn't exist(%s), will register it" % (self.get_resouce_name(), error))
                register_edge(self.get_resouce_name(), edge_type=self.edge_type, status=EdgeStatus.ACTIVE,
                              is_independent=self.is_independent, continent=self.continent, country_name=self.country_name,
                              region_code=self.region_code, city=self.city, longitude=self.longitude, latitude=self.latitude,
                              total_space=self.total_space, used_space=self.used_space, num_files=self.num_files)

            try:
                head_service = self.get_head_service()
                if head_service:
                    try:
                        client = Client(host=self.get_head_service())
                        client.update_edge(self.get_resouce_name(), edge_type=self.edge_type, status=str(EdgeStatus.ACTIVE),
                                           is_independent=self.is_independent, continent=self.continent,
                                           country_name=self.country_name, region_code=self.region_code, city=self.city,
                                           longitude=self.longitude, latitude=self.latitude, total_space=self.total_space,
                                           used_space=self.used_space, num_files=self.num_files)
                    except NoObject as error:
                        self.logger.info("Edge %s doesn't exist(%s) on head %s, will register it" % (self.get_resouce_name(),
                                                                                                     error,
                                                                                                     self.get_head_service()))

                        edge_properties = {'edge_type': self.edge_type,
                                           'status': str(EdgeStatus.ACTIVE),
                                           'is_independent': self.is_independent,
                                           'continent': self.continent,
                                           'country_name': self.country_name,
                                           'region_code': self.region_code,
                                           'city': self.city,
                                           'longitude': self.longitude,
                                           'latitude': self.latitude,
                                           'total_space': self.total_space,
                                           'used_space': self.used_space,
                                           'num_files': self.num_files}
                        client = Client(host=self.get_head_service())
                        client.register_edge(self.get_resouce_name(), **edge_properties)
            except Exception as error:
                self.logger.info("Failed to register edge %s to head service %s: %s" % (self.get_resouce_name(),
                                                                                        self.get_head_service(),
                                                                                        error))


if __name__ == '__main__':
    daemon = ResourceManager()
    daemon.run()

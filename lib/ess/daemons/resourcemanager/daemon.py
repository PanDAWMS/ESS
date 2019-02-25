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
from ess.common.exceptions import NoObject
from ess.common.utils import setup_logging, get_space_from_string
from ess.core.edges import register_edge, update_edge
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
        self.used_space = None
        self.num_files = 0

    def get_tasks(self):
        """
        Get tasks to process
        """

        # if self.used_space is not None:
        #     self.graceful_stop.wait(1800)

        tasks = [{'name': 'resource_check'}]

        self.logger.info("Main thread get %s tasks" % len(tasks))
        for task in tasks:
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
                self.logger.info("Updating edge %s with parameters: %s" % (self.name, parameters))
                update_edge(edge_name=self.name, parameters=parameters)
            except NoObject as error:
                self.logger.info("Edge %s doesn't exist, will register it: %s" % (self.name, error))
                register_edge(self.name, edge_type=self.edge_type, status=EdgeStatus.ACTIVE, is_independent=self.is_independent,
                              continent=self.continent, country_name=self.country_name, region_code=self.region_code,
                              city=self.city, longitude=self.longitude, latitude=self.latitude,
                              total_space=self.total_space, used_space=self.used_space, num_files=self.num_files)


if __name__ == '__main__':
    daemon = ResourceManager()
    daemon.run()

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

import logging
import os
import threading
import time
import traceback
import Queue

import boto
import boto.s3.connection
from boto.s3.key import Key

from ess.daemons.common.plugin_base import PluginBase


class Stager(threading.Thread):

    def __init__(self, request_queue, output_queue, logger=None, bucket_name=None,
                 access_key=None, secret_key=None, hostname=None, port=None, is_secure=None,
                 signed_url=True, lifetime=3600):

        threading.Thread.__init__(self)
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(self.__class__.__name__)
        self.graceful_stop = threading.Event()

        self.request_queue = request_queue
        self.output_queue = output_queue

        self.access_key = access_key
        self.secret_key = secret_key
        self.hostname = hostname
        self.port = port
        self.is_secure = is_secure
        self.bucket_name = bucket_name

        self.signed_url = signed_url
        self.lifetime = lifetime

        self.conn = self.connect_s3()
        self.bucket = None

    def stop(self):
        self.graceful_stop.set()

    def connect_s3(self):
        return boto.connect_s3(aws_access_key_id=self.access_key,
                               aws_secret_access_key=self.secret_key,
                               host=self.hostname,
                               port=self.port,
                               is_secure=self.is_secure,
                               calling_format=boto.s3.connection.OrdinaryCallingFormat())

    def get_bucket_name(self, coll_id):
        bucket_name = '%s-%s' % (self.bucket_name, coll_id)
        return bucket_name

    def get_bucket(self, coll_id):
        self.conn = self.connect_s3()
        try:
            bucket = self.conn.get_bucket(self.get_bucket_name(coll_id))
        except boto.exception.S3ResponseError as error:
            self.logger.error("Failed to get bucket %s: %s" % (self.get_bucket_name(coll_id), error))
            bucket = self.conn.create_bucket(self.get_bucket_name(coll_id))
        return bucket

    def stage_out(self, req):
        try:
            try:
                if self.bucket is None:
                    self.bucket = self.get_bucket(req['coll_id'])

                key = Key(self.bucket, os.path.basename(req['pfn']))
                key.set_contents_from_filename(req['pfn'])
            except boto.exception.S3ResponseError as error:
                self.bucket = self.get_bucket(req['coll_id'])
                key = Key(self.bucket, os.path.basename(req['pfn']))
                key.set_contents_from_filename(req['pfn'])

            if key.size == req['size']:
                self.logger.debug("Successfully staged out %s" % req['pfn'])
                if self.signed_url:
                    req['pfn'] = key.generate_url(self.lifetime, method='GET')
                else:
                    req['pfn'] = 's3://%s:%s/%s/%s' % (self.hostname,
                                                       self.port,
                                                       self.get_bucket_name(req['coll_id']),
                                                       os.path.basename(req['pfn']))
                return req
            else:
                self.logger.debug("Failed to stage out %s: size mismatch(local size: %s, remote size: %s)" % (
                                  req['pfn'], req['size'], key.size))
        except Exception as error:
            self.logger.error("Failed to stageout request(%s): %s, %s" % (req, error, traceback.format_exc()))

    def run(self):
        while not self.graceful_stop.is_set():
            try:
                if not self.request_queue.empty():
                    req = self.request_queue.get(False)
                    if req:
                        self.logger.debug("Staging out %s" % req)
                        output = self.stage_out(req)
                        if output:
                            self.logger.debug("Successfully staged out: %s" % output)
                            self.output_queue.put(output)
                else:
                    time.sleep(1)
            except Exception as error:
                self.logger.error("Stager throws an exception: %s, %s" % (error, traceback.format_exc()))


class ObjectStoreStager(PluginBase):
    def __init__(self, **kwargs):
        super(ObjectStoreStager, self).__init__(**kwargs)
        self.setup_logger()
        self.graceful_stop = threading.Event()

        if not hasattr(self, 'num_threads'):
            self.num_threads = 1
        else:
            self.num_threads = int(self.num_threads)

        if not hasattr(self, 'access_key'):
            raise Exception('access_key is required but not defined.')
        if not hasattr(self, 'secret_key'):
            raise Exception('secret_key is required but not defined.')
        if not hasattr(self, 'hostname'):
            raise Exception('hostname is required but not defined.')
        if not hasattr(self, 'port'):
            raise Exception('port is required but not defined.')
        else:
            self.port = int(self.port)
        if not hasattr(self, 'is_secure'):
            raise Exception('is_secure is required but not defined.')
        if not hasattr(self, 'bucket_name'):
            raise Exception('bucket_name is required but not defined.')

        if not hasattr(self, 'signed_url'):
            self.signed_url = True
        if not hasattr(self, 'lifetime'):
            self.lifetime = 3600 * 24 * 7
        else:
            self.lifetime = int(self.lifetime)

        self.request_queue = Queue.Queue()
        self.output_queue = None
        self.threads = []

    def set_output_queue(self, output_queue):
        self.output_queue = output_queue

    def start(self):
        for i in range(self.num_threads):
            stager = Stager(self.request_queue,
                            self.output_queue,
                            logger=self.logger,
                            bucket_name=self.bucket_name,
                            access_key=self.access_key,
                            secret_key=self.secret_key,
                            hostname=self.hostname,
                            port=self.port,
                            is_secure=self.is_secure,
                            signed_url=self.signed_url,
                            lifetime=self.lifetime)
            stager.start()
            self.threads.append(stager)

    def stop(self):
        for thread in self.threads:
            thread.stop()

    def is_alive(self):
        for thread in self.threads:
            if thread.is_alive():
                return True
        return False

    def stage_out_outputs(self, outputs):
        for output in outputs:
            self.logger.debug("Adding to stager queue: %s" % output)
            self.request_queue.put(output)

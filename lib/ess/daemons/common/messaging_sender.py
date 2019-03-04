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
Messaging sender plugin
"""

import json
import random
import socket
import threading
import time
import traceback

import stomp

from ess.daemons.common.plugin_base import PluginBase


class MessagingSender(PluginBase, threading.Thread):
    def __init__(self, **kwargs):
        super(MessagingSender, self).__init__(**kwargs)

        self.setup_logger()
        self.graceful_stop = threading.Event()
        self.request_queue = None

        if not hasattr(self, 'brokers'):
            raise Exception('brokers is required but not defined.')
        else:
            self.brokers = [b.strip() for b in self.brokers.split(',')]
        if not hasattr(self, 'port'):
            raise Exception('port is required but not defined.')
        if not hasattr(self, 'vhost'):
            raise Exception('vhost is required but not defined.')
        if not hasattr(self, 'destination'):
            raise Exception('destination is required but not defined.')
        if not hasattr(self, 'broker_timeout'):
            self.broker_timeout = 10
        else:
            self.broker_timeout = int(self.broker_timeout)

        self.conns = []

    def stop(self):
        self.graceful_stop.set()

    def set_request_queue(self, request_queue):
        self.request_queue = request_queue

    def connect_to_messaging_brokers(self):
        broker_addresses = []
        for b in self.brokers:
            try:
                addrinfos = socket.getaddrinfo(b, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
                for addrinfo in addrinfos:
                    b_addr = addrinfo[4][0]
                    broker_addresses.append(b_addr)
            except socket.gaierror as error:
                self.logger.error('Cannot resolve hostname %s: %s' % (b, str(error)))

        self.logger.info("Resolved broker addresses: %s" % broker_addresses)

        for broker in broker_addresses:
            conn = stomp.Connection12(host_and_ports=[(broker, self.port)],
                                      vhost=self.vhost,
                                      keepalive=True,
                                      timeout=self.broker_timeout)
            self.conns.append(conn)

    def send_message(self, msg):
        conn = random.sample(self.conns, 1)[0]
        if not conn.is_connected():
            conn.connect(self.username, self.password, wait=True)

        conn.send(body=json.dumps({'event_type': str(msg['event_type']).lower(),
                                   'payload': msg['payload'],
                                   'created_at': str(msg['created_at'])}),
                  destination=self.destination,
                  headers={'persistent': 'true',
                           'event_type': str(msg['event_type']).lower()})

    def run(self):
        self.connect_to_messaging_brokers()

        while not self.graceful_stop.is_set():
            try:
                if not self.request_queue.empty():
                    msg = self.request_queue.get(False)
                    if msg:
                        self.logger.debug("Got a message: %s" % msg)
                        self.send_message(msg)
                else:
                    time.sleep(1)
            except Exception as error:
                self.logger.error("Messaging sender throws an exception: %s, %s" % (error, traceback.format_exc()))

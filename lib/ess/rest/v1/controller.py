#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


import json
import web

from ess.common.constants import HTTP_STATUS_CODE


class ESSController:
    """ Default ESS Controller class. """

    def POST(self):
        """ Not supported. """
        raise web.BadRequest()

    def GET(self):
        """ Not supported. """
        raise web.BadRequest()

    def PUT(self):
        """ Not supported. """
        raise web.BadRequest()

    def DELETE(self):
        """ Not supported. """
        raise web.BadRequest()

    def generate_message(self, exc_cls=None, exc_msg=None):
        if exc_cls is None and exc_msg is None:
            return None
        else:
            message = {}
            if exc_cls is not None:
                message['ExceptionClass'] = exc_cls
            if exc_msg is not None:
                message['ExceptionMessage'] = str(exc_msg)
            return json.dumps(message)

    def generate_http_response(self, status_code, data=None, exc_cls=None, exc_msg=None):

        if status_code == HTTP_STATUS_CODE.OK:
            if data:
                raise web.OK(data=json.dumps(data), headers={})
            else:
                raise web.OK()
        elif status_code == HTTP_STATUS_CODE.Created:
            raise web.Created()
        elif status_code == HTTP_STATUS_CODE.Accepted:
            raise web.Accepted()
        elif status_code == HTTP_STATUS_CODE.BadRequest:
            raise web.BadRequest(message=self.generate_message(exc_cls, exc_msg))
        elif status_code == HTTP_STATUS_CODE.Unauthorized:
            raise web.Unauthorized(message=self.generate_message(exc_cls, exc_msg))
        elif status_code == HTTP_STATUS_CODE.Forbidden:
            raise web.Forbidden(message=self.generate_message(exc_cls, exc_msg))
        elif status_code == HTTP_STATUS_CODE.NotFound:
            raise web.NotFound(message=self.generate_message(exc_cls, exc_msg))
        elif status_code == HTTP_STATUS_CODE.Conflict:
            raise web.Conflict(message=self.generate_message(exc_cls, exc_msg))
        elif status_code == HTTP_STATUS_CODE.InternalError:
            raise web.InternalError(message=self.generate_message(exc_cls, exc_msg))
        else:
            if data:
                raise web.HTTPError(status_code, data=json.dumps(data))
            else:
                raise web.HTTPError(status_code)

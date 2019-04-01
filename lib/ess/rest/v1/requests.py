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
from traceback import format_exc

from web import application, data, header, input


from ess.common import exceptions
from ess.common.constants import HTTP_STATUS_CODE
from ess.rest.v1.controller import ESSController
from ess.core.requests import add_request, get_request, update_request, delete_request, get_requests


URLS = (
    '/(.+)', 'Request',
    '/', 'Requests',
)


class Requests(ESSController):
    """ Create request """

    def GET(self):
        """
        Get requests.

        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: A list containing requests.
        """
        header('Content-Type', 'application/x-json-stream')
        params = input()

        try:
            status = None
            edge_name = None
            edge_id = None
            if 'status' in params:
                status = params['status']
            if 'edge_name' in params:
                edge_name = params['edge_name']
            if 'edge_id' in params:
                edge_id = int(params['edge_id'])
            reqs = get_requests(status=status, edge_name=edge_name, edge_id=edge_id)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data=[req.to_dict() for req in reqs])

    def POST(self):
        """ Create Request.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        json_data = data()
        args = ['scope', 'name', 'data_type', 'status', 'granularity_type', 'granularity_level', 'priority', 'request_meta']

        try:
            parameters = {}
            json_data = json.loads(json_data)
            for key, value in json_data.items():
                if key in args:
                    parameters[key] = value
        except ValueError:
            raise self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            request_id = add_request(**parameters)
        except exceptions.DuplicatedObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data={'request_id': request_id})


class Request(ESSController):
    """ Update, get and delete Request. """

    def PUT(self, request_id):
        """ Update Request properties with a given id.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """
        json_data = data()
        args = ['scope', 'name', 'data_type', 'status', 'granularity_type', 'granularity_level', 'priority', 'request_meta']

        try:
            parameters = {}
            json_data = json.loads(json_data)
            for key, value in json_data.items():
                if key in args:
                    parameters[key] = value
        except ValueError:
            raise self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            update_request(request_id, parameters)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'update successfully'})

    def GET(self, request_id):
        """ Get details about a specific Request with given id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        header('Content-Type', 'application/json')

        try:
            request = get_request(request_id=request_id)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data=request.to_dict())

    def DELETE(self, request_id):
        """ Delete an Request with given id.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :param request_id: Request id.
        """
        try:
            delete_request(request_id=request_id)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'deleted successfully'})


"""----------------------
   Web service startup
----------------------"""


APP = application(URLS, globals())
application = APP.wsgifunc()

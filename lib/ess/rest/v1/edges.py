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
from ess.core.edges import register_edge, get_edge, update_edge, delete_edge, get_edges


URLS = (
    '/(.+)', 'Edge',
    '/', 'Edges',
)


class Edges(ESSController):
    """ List all Edges. """

    def GET(self):
        """ List all Edges.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: A list containing all Edges.
        """
        header('Content-Type', 'application/x-json-stream')
        params = input()

        status = None
        if 'status' in params:
            status = params['status']
        edges = get_edges(status=status)

        try:
            status = None
            if 'status' in params:
                status = params['status']
            edges = get_edges(status=status)
        except exceptions.NoObject as error:
            raise error
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data=[e.to_dict() for e in edges])


class Edge(ESSController):
    """ Create, update, get and disable Edge. """

    def POST(self, edge_name):
        """ Create Edge with given name.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            500 Internal Error
        """
        json_data = data()
        args = ['edge_type', 'status', 'is_independent', 'continent', 'country_name', 'region_code', 'city',
                'longitude', 'latitude', 'total_space', 'used_space', 'num_files']

        try:
            parameters = {}
            json_data = json.loads(json_data)
            for key, value in json_data.items():
                if key in args:
                    parameters[key] = value
        except ValueError:
            raise self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            edge_id = register_edge(edge_name, **parameters)
        except exceptions.DuplicatedObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.Conflict, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data={'edge_id': edge_id})

    def PUT(self, edge_name):
        """ Update Edge properties with a given name.
        HTTP Success:
            200 OK
        HTTP Error:
            400 Bad request
            404 Not Found
            500 Internal Error
        """
        json_data = data()
        args = ['edge_type', 'status', 'is_independent', 'continent', 'country_name', 'region_code', 'city',
                'longitude', 'latitude', 'total_space', 'used_space', 'num_files']

        try:
            parameters = {}
            json_data = json.loads(json_data)
            for key, value in json_data.items():
                if key in args:
                    parameters[key] = value
        except ValueError:
            raise self.generate_http_response(HTTP_STATUS_CODE.BadRequest, exc_cls=exceptions.BadRequest.__name__, exc_msg='Cannot decode json parameter dictionary')

        try:
            update_edge(edge_name, parameters)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data={'status': 0, 'message': 'update successfully'})

    def GET(self, edge_name):
        """ Get details about a specific Edge with given edge_name.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an edge.
        """

        header('Content-Type', 'application/json')

        try:
            edge = get_edge(edge_name=edge_name)
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data=edge.to_dict())

    def DELETE(self, edge_name):
        """ Delete an Edge with given name.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :param edge_name: Edge name.
        """
        try:
            delete_edge(edge_name=edge_name)
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

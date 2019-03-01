#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0OA
#
# Authors:
# - Wen Guan, <wen.guan@cern.ch>, 2019


from traceback import format_exc

from web import application, header, input


from ess.common import exceptions
from ess.common.constants import HTTP_STATUS_CODE
from ess.rest.v1.controller import ESSController
from ess.core.catalog import get_content_best_match
from ess.orm.constants import ContentStatus


URLS = (
    '/(.*)/(.*)', 'Catalog',
)


class Catalog(ESSController):
    """ Update, get and delete Catalog. """

    def GET(self, scope, name):
        """ Get content details about a scope and name.
        HTTP Success:
            200 OK
        HTTP Error:
            404 Not Found
            500 InternalError
        :returns: dictionary of an request.
        """

        header('Content-Type', 'application/json')
        params = input()

        min_id = None
        max_id = None
        status = None

        if 'min_id' in params:
            min_id = params['min_id']
            try:
                min_id = int(min_id)
            except:
                min_id = None

        if 'max_id' in params:
            max_id = params['max_id']
            try:
                max_id = int(max_id)
            except:
                max_id = None

        if 'status' in params:
            status = params['status']

        try:
            content = get_content_best_match(scope=scope, name=name, min_id=min_id, max_id=max_id, status=status)
            if content.status != ContentStatus.AVAILABLE:
                content.pfn_size = 0
                content.pfn = None
        except exceptions.NoObject as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.NotFound, exc_cls=error.__class__.__name__, exc_msg=error)
        except exceptions.ESSException as error:
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=error.__class__.__name__, exc_msg=error)
        except Exception as error:
            print(error)
            print(format_exc())
            raise self.generate_http_response(HTTP_STATUS_CODE.InternalError, exc_cls=exceptions.CoreException.__name__, exc_msg=error)

        raise self.generate_http_response(HTTP_STATUS_CODE.OK, data=content.to_dict())


"""----------------------
   Web service startup
----------------------"""


APP = application(URLS, globals())
application = APP.wsgifunc()

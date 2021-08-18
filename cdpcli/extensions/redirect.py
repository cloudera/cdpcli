# Copyright 2021 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.


import logging

from cdpcli.exceptions import RedirectExtensionError
from cdpcli.utils import is_absolute_url


LOG = logging.getLogger('cdpcli.extensions.redirect')


def register(operation_callers, operation_model):
    """
    Register an extension to run before or after the CLI command.
    To replace the original CLI caller:
    * operation_callers.insert(0, ReplacementCaller())
    * return False by the ReplacementCaller.invoke(...)
    """
    operation_callers.insert(0, Redirect())


class Redirect(object):
    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        service_name = operation_model.service_model.service_name
        operation_name = operation_model.name
        client = client_creator(service_name)
        redirect_url = self._find_redirect_location(client,
                                                    operation_name,
                                                    parameters)
        if not redirect_url or not is_absolute_url(redirect_url):
            raise RedirectExtensionError(
                err_msg='Missing or incorrect redirect URL.')
        operation_model.http['requestUri'] = redirect_url
        parsed_globals.endpoint_url = redirect_url
        # The command processing is not finished, continue to run other CLI callers.
        return True

    def _find_redirect_location(self, client, operation_name, parameters):
        # Sends an API request to find the redirect location.
        # We cannot use auto-redirect because we have to do the auth-sign
        # for the redirected location.
        http, resp = client.make_api_call(operation_name, parameters,
                                          allow_redirects=False)
        if http.is_redirect:
            redirect_url = http.headers.get('Location', None)
        else:
            redirect_url = None
        LOG.debug('Redirect request status: %d, redirect to: %s' %
                  (http.status_code, redirect_url))
        return redirect_url

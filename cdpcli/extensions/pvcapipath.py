# Copyright 2023 Cloudera, Inc. All rights reserved.
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


def register(operation_callers, operation_model, form_factor):
    """
    Register an extension to run before or after the CLI command.
    To replace the original CLI caller:
    * operation_callers.insert(0, ReplacementCaller())
    * return False by the ReplacementCaller.invoke(...)
    """
    operation_callers.insert(0, PvcApiPath(form_factor))


class PvcApiPath(object):
    def __init__(self, form_factor):
        self._form_factor = form_factor

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        """
        In case of PvC, we need to prepend '/api/v1' to the API path.
        No need to do anything for Public Cloud.
        """
        if self._form_factor == 'private':
            request_path = operation_model.http['requestUri']
            if not request_path.startswith('/api/v1'):
                operation_model.http['requestUri'] = '/api/v1' + request_path
        # The command processing is not finished, continue to run other CLI callers.
        return True

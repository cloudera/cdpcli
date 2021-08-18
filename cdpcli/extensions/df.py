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
import os

from cdpcli.clidriver import CLIOperationCaller
from cdpcli.exceptions import DfExtensionError

LOG = logging.getLogger('cdpcli.extensions.df')


def register(operation_callers, operation_model):
    """
    Register an extension to run before or after the CLI command.
    To replace the original CLI caller:
    * operation_callers.insert(0, ReplacementCaller())
    * return False by the ReplacementCaller.invoke(...)
    """
    operation_callers.insert(0, UploadFileToDf())


class UploadFileToDf(CLIOperationCaller):
    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        service_name = operation_model.service_model.service_name
        operation_name = operation_model.name
        if service_name == 'df' and operation_name == 'uploadFlow':
            self._df_upload_flow(client_creator,
                                 operation_model,
                                 parameters,
                                 parsed_globals)
        elif service_name == 'df-workload':
            self._df_workload_operation()
        else:
            raise DfExtensionError(
                err_msg='The operation is not supported.',
                service_name=service_name,
                operation_name=operation_name)
        # The command processing is finished, do not run the original CLI caller.
        return False

    def _df_upload_flow(self, client_creator, operation_model,
                        parameters, parsed_globals):
        client = client_creator('df')
        operation_name = operation_model.name
        url = operation_model.http['requestUri']
        method = "post"
        headers = {
            'Content-Type': 'application/json',
            'Flow-Definition-Name': parameters.get('name', None),
            'Flow-Definition-Description': parameters.get('description', None),
            'Flow-Definition-Comments': parameters.get('comments', None)
        }
        file_path = parameters.get('file', None)
        response = self._upload_file(client, operation_name,
                                     method, url, headers, file_path)
        self._display_response(operation_name, response, parsed_globals)

    def _df_workload_operation(self):
        pass

    def _upload_file(self, client, operation_name,
                     method, url, headers, file_path):
        headers = {k: v for k, v in headers.items() if v is not None}
        file_path = os.path.expandvars(os.path.expanduser(file_path))
        with open(file_path, 'rb') as f:
            http, parsed_response = client.make_request(
                operation_name, method, url, headers, f)
        return parsed_response

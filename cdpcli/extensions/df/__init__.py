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


def get_expanded_file_path(file_path):
    return os.path.expandvars(os.path.expanduser(file_path))


def upload_workload_asset(client, parameters):
    method = 'post'
    url = '/dfx/api/rpc-v1/deployments/upload-asset-content'
    file_path = parameters.get('filePath', None)
    headers = {
        'Content-Type': 'application/octet-stream',
        'Deployment-Request-Crn': parameters.get('deploymentRequestCrn', None),
        'Deployment-Name': parameters.get('deploymentName', None),
        'Asset-Update-Request-Crn': parameters.get('assetUpdateRequestCrn', None),
        'Parameter-Group': parameters.get('parameterGroup', None),
        'Parameter-Name': parameters.get('parameterName', None),
        'File-Path': file_path,
    }
    return upload_file(client, 'uploadAsset', method, url, headers, file_path)


def upload_file(client, operation_name, method, url, headers, file_path):
    headers = {k: v for k, v in headers.items() if v is not None}
    expanded_file_path = get_expanded_file_path(file_path)
    if os.path.exists(expanded_file_path):
        with open(expanded_file_path, 'rb') as f:
            http, parsed_response = client.make_request(
                operation_name, method, url, headers, f)
    else:
        raise DfExtensionError(
            err_msg='Path [{}] not found'.format(file_path),
            service_name=client.service_model.service_name,
            operation_name=operation_name)
    return parsed_response


class DfExtension(CLIOperationCaller):
    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        service_name = operation_model.service_model.service_name
        operation_name = operation_model.name
        if service_name == 'df' and operation_name == 'importFlowDefinition':
            self._df_upload_flow(client_creator,
                                 operation_model,
                                 parameters,
                                 parsed_globals)
        elif service_name == 'df' and operation_name == 'importFlowDefinitionVersion':
            self._df_upload_flow_version(client_creator,
                                         operation_model,
                                         parameters,
                                         parsed_globals)
        elif service_name == 'dfworkload' and operation_name == 'uploadAsset':
            self._df_workload_upload_asset(client_creator,
                                           operation_model,
                                           parameters,
                                           parsed_globals)
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
        method = 'post'
        headers = {
            'Content-Type': 'application/json',
            'Flow-Definition-Name': parameters.get('name', None),
            'Flow-Definition-Description': parameters.get('description', None),
            'Flow-Definition-Comments': parameters.get('comments', None)
        }
        file_path = parameters.get('file', None)
        response = upload_file(client, operation_name,
                               method, url, headers, file_path)
        self._display_response(operation_name, response, parsed_globals)

    def _df_upload_flow_version(self, client_creator, operation_model,
                                parameters, parsed_globals):
        client = client_creator('df')
        operation_name = operation_model.name
        url = operation_model.http['requestUri']
        method = 'post'
        headers = {
            'Content-Type': 'application/json',
            'Flow-Definition-Comments': parameters.get('comments', None)
        }
        file_path = parameters.get('file', None)
        response = upload_file(client, operation_name,
                               method, url, headers, file_path)
        self._display_response(operation_name, response, parsed_globals)

    def _df_workload_upload_asset(self, client_creator, operation_model,
                                  parameters, parsed_globals):
        client = client_creator('dfworkload')
        operation_name = operation_model.name
        response = upload_workload_asset(client, parameters)
        self._display_response(operation_name, response, parsed_globals)

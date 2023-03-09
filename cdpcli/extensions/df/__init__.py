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


import base64
import json
import logging
import os
import urllib.parse as urlparse

from cdpcli.clidriver import CLIOperationCaller
from cdpcli.exceptions import CdpCLIError, DfExtensionError

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
            service_name=client.meta.service_model.service_name,
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
        elif service_name == 'df' and operation_name == 'getFlowVersion':
            self._df_download_flow_version(client_creator,
                                           operation_model,
                                           parameters,
                                           parsed_globals)
        elif service_name == 'dfworkload' and operation_name == 'uploadAsset':
            self._df_workload_upload_asset(client_creator,
                                           operation_model,
                                           parameters,
                                           parsed_globals)
        elif service_name == 'dfworkload' and operation_name == 'updateDeployment':
            self._df_workload_update_deployment(client_creator,
                                                operation_model,
                                                parameters,
                                                parsed_globals)
            return True
        else:
            raise DfExtensionError(
                err_msg='The operation is not supported.',
                service_name=service_name,
                operation_name=operation_name)
        # The command processing is finished, do not run the original CLI caller.
        return False

    def _encode_value(self, value):
        if value:
            return urlparse.quote(value)
        return None

    def _df_upload_flow(self, client_creator, operation_model,
                        parameters, parsed_globals):
        client = client_creator('df')
        operation_name = operation_model.name
        url = operation_model.http['requestUri']
        method = 'post'

        # Encode the name, description, and comments fields.
        # The df api expects them to be URI encoded.
        flowName = self._encode_value(parameters.get('name', None))
        flowDescription = self._encode_value(parameters.get('description', None))
        flowComment = self._encode_value(parameters.get('comments', None))

        headers = {
            'Content-Type': 'application/json',
            'Flow-Definition-Name': flowName,
            'Flow-Definition-Description': flowDescription,
            'Flow-Definition-Comments': flowComment
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

        # Encode the comments fields, the df api expects them to be URI encoded.
        flowComment = self._encode_value(parameters.get('comments', None))
        headers = {
            'Content-Type': 'application/json',
            'Flow-Definition-Comments': flowComment
        }
        file_path = parameters.get('file', None)
        response = upload_file(client, operation_name,
                               method, url, headers, file_path)
        self._display_response(operation_name, response, parsed_globals)

    def _df_download_flow_version(self, client_creator, operation_model,
                                  parameters, parsed_globals):
        client = client_creator('df')
        operation_name = operation_model.name

        http, response = client.make_api_call(operation_name, parameters)

        decoded_flow_definition = base64.b64decode(response.get('flowDefinition'))
        flow_definition_json = json.loads(decoded_flow_definition)

        self._display_response(operation_name, flow_definition_json, parsed_globals)

    def _df_workload_upload_asset(self, client_creator, operation_model,
                                  parameters, parsed_globals):
        client = client_creator('dfworkload')
        operation_name = operation_model.name
        response = upload_workload_asset(client, parameters)
        self._display_response(operation_name, response, parsed_globals)

    def _df_workload_update_deployment(self, client_creator, operation_model,
                                       parameters, parsed_globals):
        kpis = parameters.get('kpis', None)
        if kpis:
            self._process_kpis(kpis)

        if self._has_asset_references(parameters):
            df_workload_client = client_creator('dfworkload')

            deployment_crn = parameters.get('deploymentCrn', None)
            environment_crn = parameters.get('environmentCrn', None)
            asset_update_request_crn = self._get_asset_update_request_crn(
                    df_workload_client, environment_crn, deployment_crn)

            LOG.debug('Asset Update Request CRN [%s]' % asset_update_request_crn)
            parameters['assetUpdateRequestCrn'] = asset_update_request_crn

            try:
                self._upload_asset_references(
                        df_workload_client, asset_update_request_crn, parameters)
            except CdpCLIError:
                df_workload_client.abort_asset_update_request(
                    deploymentCrn=deployment_crn,
                    environmentCrn=environment_crn,
                    assetUpdateRequestCrn=asset_update_request_crn
                )
                raise

    def _process_kpis(self, kpis):
        """
        Process Key Performance Indicators and set required unit properties
        """
        for kpi in kpis:
            alert = kpi.get('alert', None)
            if alert:
                frequency_tolerance = alert.get('frequencyTolerance', None)
                if frequency_tolerance:
                    unit = frequency_tolerance['unit']
                    id = unit['id']
                    unit['label'] = id.capitalize()
                    unit['abbreviation'] = id[:1].lower()

    def _has_asset_references(self,
                              parameters):
        """
        Evaluate Parameter Groups and Parameters for Asset References
        """
        parameter_groups = parameters.get('parameterGroups', None)
        if parameter_groups:
            for parameter_group in parameter_groups:
                parameters = parameter_group['parameters']
                for parameter in parameters:
                    asset_references = parameter.get('assetReferences', None)
                    if asset_references:
                        for asset_ref in asset_references:
                            asset_path = asset_ref.get('path', None)
                            if asset_path:
                                return True

        return False

    def _get_asset_update_request_crn(self,
                                      df_workload_client,
                                      environment_crn,
                                      deployment_crn):
        response = df_workload_client.create_asset_update_request(
                deploymentCrn=deployment_crn,
                environmentCrn=environment_crn
        )
        return response.get('assetUpdateRequestCrn', None)

    def _upload_asset_references(self,
                                 df_workload_client,
                                 asset_update_request_crn,
                                 parameters):
        """
        Process Parameter Groups and upload Asset References
        """
        parameter_groups = parameters.get('parameterGroups', None)
        if parameter_groups:
            for parameter_group in parameter_groups:
                parameter_group_name = parameter_group['name']
                parameters = parameter_group['parameters']
                for parameter in parameters:
                    asset_references = parameter.get('assetReferences', None)
                    if asset_references:
                        for asset_reference in asset_references:
                            asset_path = asset_reference.get('path', None)
                            if asset_path:
                                asset_params = {
                                    'assetUpdateRequestCrn': asset_update_request_crn,
                                    'parameterGroup': parameter_group_name,
                                    'parameterName': parameter.get('name', None),
                                    'filePath': asset_path
                                }
                                upload_workload_asset(df_workload_client, asset_params)

                                file_path = get_expanded_file_path(asset_path)
                                path, name = os.path.split(file_path)
                                asset_reference['name'] = name
                                asset_reference['path'] = path

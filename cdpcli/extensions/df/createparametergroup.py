# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from cdpcli.clidriver import CLIOperationCaller, ServiceOperation
from cdpcli.exceptions import ClientError, DfExtensionError
from cdpcli.extensions.df import (get_expanded_file_path,
                                  upload_parameter_asset)
from cdpcli.extensions.df.model import PARAMETER_GROUP_PARAMETER
from cdpcli.extensions.workload import set_workload_access_token
from cdpcli.model import ObjectShape, OperationModel, ShapeResolver
from cdpcli.utils import CachedProperty

LOG = logging.getLogger('cdpcli.extensions.df.createparametergroup')
MAX_ASSET_SIZE = 150 * 1024 * 1024
INITIAL_ASSET_VERSION = '0'

SERVICE_NAME = 'df'
OPERATION_NAME = 'createParameterGroup'
OPERATION_CLI_NAME = 'create-parameter-group'
OPERATION_SUMMARY = 'Create a parameter group with optional asset upload'
OPERATION_DESCRIPTION = """
    Create a new parameter group on workload with the specified configuration.
    Supports uploading assets for parameters of type FILE or FILES. When
    assetReferences contain local file paths, the assets will be uploaded
    automatically. This operation is supported for the CLI only.
    """
OPERATION_DATA = {
    'summary': OPERATION_SUMMARY,
    'description': OPERATION_DESCRIPTION,
    'operationId': OPERATION_NAME,
}
OPERATION_SHAPES = {
    'CreateParameterGroupRequest': {
        'type': 'object',
        'description': 'Request object for creating a parameter group.',
        'required': ['environmentCrn', 'name'],
        'properties': {
            'environmentCrn': {
                'type': 'string',
                'description': 'The CRN of an environment to execute the command.'
            },
            'name': {
                'type': 'string',
                'description': 'The name of the parameter group.'
            },
            'projectCrn': {
                'type': 'string',
                'description': 'Optional project CRN that parameter group is assigned to.'
            },
            'description': {
                'type': 'string',
                'description': 'The description of the parameter group.'
            },
            'parameters': {
                'type': 'array',
                'description': 'Parameters for the parameter group.',
                'items': {
                    '$ref': '#/definitions/ParameterGroupParameter'
                }
            },
        }
    },
    'CreateParameterGroupResponse': {
        'type': 'object',
        'description': 'Response for Create Parameter Group command.',
        'properties': {
            'parameterGroup': {
                'type': 'object',
                'description': 'The created parameter group.',
                'properties': {
                    'crn': {'type': 'string'},
                    'name': {'type': 'string'},
                    'id': {'type': 'string'},
                }
            }
        }
    },
    'ParameterGroupParameter': PARAMETER_GROUP_PARAMETER,
}


class CreateParameterGroup(ServiceOperation):

    def __init__(self, clidriver, service_model):
        super(CreateParameterGroup, self).__init__(
            clidriver=clidriver,
            name=OPERATION_CLI_NAME,
            parent_name=SERVICE_NAME,
            service_model=service_model,
            operation_model=CreateParameterGroupOperationModel(service_model),
            operation_caller=CreateParameterGroupOperationCaller())


class CreateParameterGroupOperationModel(OperationModel):

    def __init__(self, service_model):
        super(CreateParameterGroupOperationModel, self).__init__(
            operation_data=OPERATION_DATA,
            service_model=service_model,
            name=OPERATION_NAME,
            http_method=None,
            request_uri=None)

    @CachedProperty
    def input_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='input',
                           shape_data=OPERATION_SHAPES['CreateParameterGroupRequest'],
                           shape_resolver=resolver)

    @CachedProperty
    def output_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='output',
                           shape_data=OPERATION_SHAPES['CreateParameterGroupResponse'],
                           shape_resolver=resolver)


class CreateParameterGroupOperationCaller(CLIOperationCaller):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        # Set up workload access for dfworkload client
        environment_crn = parameters.get('environmentCrn', None)
        if not environment_crn:
            raise DfExtensionError(
                err_msg='environmentCrn is required',
                service_name=SERVICE_NAME,
                operation_name=OPERATION_NAME)

        iam_client = client_creator('iam')
        set_workload_access_token(
            iam_client, parsed_globals, SERVICE_NAME.upper(), environment_crn)

        df_workload_client = client_creator('dfworkload')

        try:
            response = self._create_parameter_group(df_workload_client, parameters)
            self._display_response(operation_model.name, response, parsed_globals)
        except ClientError as e:
            LOG.error('Failed to create parameter group: %s', str(e))
            raise

    def _validate_and_upload_assets(
            self, df_workload_client, parameter_group_crn, parameters):
        """
        Validate asset file sizes and upload assets for parameters
        that have assetReferences.
        """
        parameters_list = parameters.get('parameters') or []
        for parameter in parameters_list:
            asset_references = parameter.get('assetReferences', None)
            if asset_references:
                for asset_path in asset_references:
                    try:
                        file_stats = os.stat(get_expanded_file_path(asset_path))
                    except OSError as e:
                        raise DfExtensionError(
                            err_msg='Could not access file [{}]: {}'
                                    .format(asset_path, str(e)),
                            service_name=df_workload_client.meta.service_model
                                                           .service_name,
                            operation_name='uploadParameterAsset')
                    if file_stats.st_size > MAX_ASSET_SIZE:
                        raise DfExtensionError(
                            err_msg='The file size exceeds the 150 MB limit, file: [{}]'
                                    .format(asset_path),
                            service_name=df_workload_client.meta
                                                           .service_model.service_name,
                            operation_name='uploadParameterAsset')

        for parameter in parameters_list:
            asset_references = parameter.get('assetReferences', None)
            if asset_references:
                updated_asset_references = []
                for asset_path in asset_references:
                    asset_params = {
                        'parameterGroupCrn': parameter_group_crn,
                        'parameterName': parameter.get('name'),
                        'filePath': asset_path,
                    }
                    upload_parameter_asset(df_workload_client, asset_params)

                    file_path = get_expanded_file_path(asset_path)
                    path, name = os.path.split(file_path)
                    asset_reference = {
                        'name': name,
                        'path': path,
                        'version': INITIAL_ASSET_VERSION
                    }
                    updated_asset_references.append(asset_reference)
                parameter['assetReferences'] = updated_asset_references
                parameter['sensitive'] = False

    def _create_parameter_group(self, df_workload_client, parameters):
        """
        Create parameter group: first create with parameters (empty assetRefs),
        then upload assets, then update with asset references.
        """
        # Build create request - parameters with empty assetReferences for API
        create_params = self._build_create_parameters(parameters)
        create_request = {
            'environmentCrn': parameters.get('environmentCrn'),
            'name': parameters.get('name'),
        }
        if parameters.get('projectCrn') is not None:
            create_request['projectCrn'] = parameters.get('projectCrn')
        if parameters.get('description') is not None:
            create_request['description'] = parameters.get('description')
        if create_params:
            create_request['parameters'] = create_params

        LOG.debug('Create Parameter Group request: %s', create_request)
        http, create_response = df_workload_client.make_api_call(
            'createParameterGroup', create_request)

        parameter_group = create_response.get('parameterGroup', {})
        parameter_group_crn = parameter_group.get('crn', None)
        if not parameter_group_crn:
            raise DfExtensionError(
                err_msg='Create parameter group did not return a CRN',
                service_name=SERVICE_NAME,
                operation_name=OPERATION_NAME)

        self._validate_and_upload_assets(
            df_workload_client, parameter_group_crn, parameters)

        # If we uploaded any assets, update the parameter group with asset refs
        parameters_list = parameters.get('parameters') or []
        if any(p.get('assetReferences') for p in parameters_list):
            update_request = {
                'environmentCrn': parameters.get('environmentCrn'),
                'parameterGroupCrn': parameter_group_crn,
                'parameters': parameters_list,
            }
            if parameters.get('description') is not None:
                update_request['description'] = parameters.get('description')

            LOG.debug('Update Parameter Group with asset refs: %s', update_request)
            http, update_response = df_workload_client.make_api_call(
                'updateParameterGroup', update_request)
            return update_response

        return create_response

    def _build_create_parameters(self, parameters):
        """Build parameters for create - with empty assetReferences for file params."""
        result = []
        for param in parameters.get('parameters') or []:
            create_param = {
                'name': param.get('name'),
            }
            if param.get('sensitive') is not None:
                create_param['sensitive'] = param.get('sensitive')
            if param.get('description') is not None:
                create_param['description'] = param.get('description')
            if param.get('value') is not None:
                create_param['value'] = param.get('value')
            # Include empty assetReferences for params that will have assets
            if param.get('assetReferences'):
                create_param['assetReferences'] = []
            result.append(create_param)
        return result

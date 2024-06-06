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
from cdpcli.exceptions import CdpCLIError, ClientError, DfExtensionError
from cdpcli.extensions.df import (get_asset_update_request_crn,
                                  get_deployment_request_details,
                                  get_environment_crn,
                                  get_expanded_file_path,
                                  initiate_deployment,
                                  process_kpis,
                                  upload_workload_asset)
from cdpcli.extensions.df.model import (DEPLOYMENT_ALERT,
                                        DEPLOYMENT_ALERT_THRESHOLD,
                                        DEPLOYMENT_FLOW_PARAMETER_ASSET_REFERENCE,
                                        DEPLOYMENT_FLOW_PARAMETER_FOR_UPDATE,
                                        DEPLOYMENT_FLOW_PARAMETER_GROUP,
                                        DEPLOYMENT_FREQUENCY_TOLERANCE,
                                        DEPLOYMENT_KEY_PERFORMANCE_INDICATOR)
from cdpcli.extensions.workload import set_workload_access_token
from cdpcli.model import ObjectShape, OperationModel, ShapeResolver
from cdpcli.utils import CachedProperty

LOG = logging.getLogger('cdpcli.extensions.df.changeflowversion')

SERVICE_NAME = 'df'
OPERATION_NAME = 'changeFlowVersion'
OPERATION_CLI_NAME = 'change-flow-version'
OPERATION_SUMMARY = 'Initiate and change the flow version of a running deployment'
OPERATION_DESCRIPTION = """
    Initiates a deployment change flow version on the control plane,
    and change the flow version of a running deployment on the workload.
    This operation is supported for the CLI only.
    """
OPERATION_DATA = {
    'summary': OPERATION_SUMMARY,
    'description': OPERATION_DESCRIPTION,
    'operationId': OPERATION_NAME,
}
OPERATION_SHAPES = {
    'ChangeFlowVersionRequest': {
        'type': 'object',
        'description': 'Request object for Change '
                       'Flow Version of a running deployment.',
        'required': ['serviceCrn', 'flowVersionCrn', 'deploymentCrn'],
        'properties': {
            'serviceCrn': {
                'type': 'string',
                'description': 'CRN for the service.'
            },
            'flowVersionCrn': {
                'type': 'string',
                'description': 'The intended target flow definition version '
                               'CRN to change to for the deployment.'
            },
            'deploymentCrn': {
                'type': 'string',
                'description': 'CRN for the deployment.'
            },
            'strategy': {
                'type': 'string',
                'description': 'The strategy to use during change flow version. '
                               'The default is STOP_AND_PROCESS_DATA.',
                'enum': [
                    'STOP_AND_PROCESS_DATA',
                    'STOP_AND_EMPTY_QUEUES',
                    'ONLY_RESTART_AFFECTED_COMPONENTS'
                ],
            },
            'waitForFlowToStopInMinutes': {
                'type': 'integer',
                'description': 'The max time in minutes to wait for flow to bleed out. '
                               'This is only relevant when using the default change '
                               'flow version strategy (STOP_AND_PROCESS_DATA). '
                               'The default wait time is 15 minutes.'
            },
            'parameterGroups': {
                'type': 'array',
                'description': 'Parameter groups with each requiring a value or assets.',
                'items': {
                    '$ref': '#/definitions/DeploymentFlowParameterGroup'
                }
            },
            'kpis': {
                'type': 'array',
                'description': 'Key Performance Indicators with associated alerts. '
                               'If these are specified, the KPIs here will replace '
                               'all the existing KPIs in the deployment.',
                'items': {
                    '$ref': '#/definitions/DeploymentKeyPerformanceIndicator'
                }
            }
        }
    },
    'ChangeFlowVersionResponse': {
        'type': 'object',
        'description': 'Response for Change Flow Version command.',
        'properties': {
            'crn': {
                'type': 'string',
                'description': 'CRN for the deployment where '
                               'change flow version was performed.'
            }
        }
    },
    'DeploymentFlowParameterGroup': DEPLOYMENT_FLOW_PARAMETER_GROUP,
    'DeploymentFlowParameter': DEPLOYMENT_FLOW_PARAMETER_FOR_UPDATE,
    'AssetReference': DEPLOYMENT_FLOW_PARAMETER_ASSET_REFERENCE,
    'DeploymentKeyPerformanceIndicator': DEPLOYMENT_KEY_PERFORMANCE_INDICATOR,
    'DeploymentAlert': DEPLOYMENT_ALERT,
    'DeploymentAlertThreshold': DEPLOYMENT_ALERT_THRESHOLD,
    'DeploymentFrequencyTolerance': DEPLOYMENT_FREQUENCY_TOLERANCE
}


def has_asset_references(parameters):
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
                    for asset_path in asset_references:
                        if asset_path:
                            return True
    return False


def upload_asset_references(df_workload_client,
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


def abort_asset_update_request(df_workload_client,
                               deployment_crn,
                               environment_crn,
                               asset_update_request_crn):
    try:
        request = {
            'deploymentCrn': deployment_crn,
            'environmentCrn': environment_crn,
            'assetUpdateRequestCrn': asset_update_request_crn
        }
        df_workload_client.make_api_call(
            'abortAssetUpdateRequest',
            request
        )
    except ClientError as e:
        if e.http_status_code >= 400:
            LOG.error(
                'Failed to abort asset update request with CRN: [%s]',
                asset_update_request_crn
            )
        else:
            LOG.error(
                'Encountered an error while attempting to '
                'abort asset update request with CRN: [%s]',
                asset_update_request_crn
            )


class ChangeFlowVersion(ServiceOperation):

    def __init__(self, clidriver, service_model):
        super(ChangeFlowVersion, self).__init__(
            clidriver=clidriver,
            name=OPERATION_CLI_NAME,
            parent_name=SERVICE_NAME,
            service_model=service_model,
            operation_model=ChangeFlowVersionOperationModel(service_model),
            operation_caller=ChangeFlowVersionOperationCaller())


class ChangeFlowVersionOperationModel(OperationModel):

    def __init__(self, service_model):
        super(ChangeFlowVersionOperationModel, self).__init__(
            operation_data=OPERATION_DATA,
            service_model=service_model,
            name=OPERATION_NAME,
            http_method=None,
            request_uri=None)

    @CachedProperty
    def input_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='input',
                           shape_data=OPERATION_SHAPES['ChangeFlowVersionRequest'],
                           shape_resolver=resolver)

    @CachedProperty
    def output_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='output',
                           shape_data=OPERATION_SHAPES['ChangeFlowVersionResponse'],
                           shape_resolver=resolver)


class ChangeFlowVersionOperationCaller(CLIOperationCaller):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        self._validate_cfv_parameters(parameters)

        df_client = client_creator('df')

        service_crn = parameters.get('serviceCrn')
        flow_version_crn = parameters.get('flowVersionCrn')
        deployment_crn = parameters.get('deploymentCrn')
        deployment_request_crn = initiate_deployment(
                df_client, service_crn, flow_version_crn, deployment_crn)

        environment_crn = get_environment_crn(df_client, service_crn)

        iam_client = client_creator('iam')
        set_workload_access_token(iam_client, parsed_globals, SERVICE_NAME.upper(),
                                  environment_crn)

        df_workload_client = client_creator('dfworkload')
        get_deployment_request_details(
            df_workload_client,
            deployment_request_crn,
            environment_crn
        )

        response = self._change_flow_version(
            df_workload_client,
            deployment_request_crn,
            environment_crn,
            parameters
        )
        self._display_response(operation_model.name, response, parsed_globals)

    def _change_flow_version(self,
                             df_workload_client,
                             deployment_request_crn,
                             environment_crn,
                             parameters):
        """
        Changes the flow version of a running deployment using the
        initiated Deployment Request CRN.
        """
        deployment_crn = parameters.get('deploymentCrn')
        request = {
            'environmentCrn': environment_crn,
            'deploymentRequestCrn': deployment_request_crn,
            'deploymentCrn': deployment_crn,
        }

        strategy = parameters.get('strategy', None)
        if strategy:
            request['strategy'] = strategy

        flow_bleed_out_time = parameters.get('waitForFlowToStopInMinutes', None)
        if flow_bleed_out_time:
            request['waitForFlowToStopInMinutes'] = flow_bleed_out_time

        kpis = parameters.get('kpis', None)
        if kpis is not None:
            request['kpis'] = process_kpis(kpis)

        # perform upload of asset references then set parameterGroups in request
        parameterGroups = parameters.get('parameterGroups', None)
        if parameterGroups:
            if has_asset_references(parameters):
                asset_update_request_crn = get_asset_update_request_crn(
                    df_workload_client, environment_crn, deployment_crn)
                try:
                    upload_asset_references(df_workload_client,
                                            asset_update_request_crn,
                                            parameters)
                except CdpCLIError:
                    abort_asset_update_request(
                        df_workload_client,
                        deployment_crn,
                        environment_crn,
                        asset_update_request_crn
                    )
                    raise
                except Exception:
                    abort_asset_update_request(
                        df_workload_client,
                        deployment_crn,
                        environment_crn,
                        asset_update_request_crn
                    )
                    raise
                request['assetUpdateRequestCrn'] = asset_update_request_crn
            request['parameterGroups'] = parameterGroups

        LOG.debug('Change Flow Version Parameters %s', request)
        try:
            http, response = df_workload_client.make_api_call(
                'changeFlowVersion',
                request
            )
            # this should match the format of ChangeFlowVersionResponse
            # defined in OPERATION_SHAPES above
            deployment_configuration = response.get('deploymentConfiguration', {})
            change_flow_version_response = {
                'crn': deployment_configuration.get('deploymentCrn', None)
            }
            return change_flow_version_response
        except (ClientError, DfExtensionError):
            if 'assetUpdateRequestCrn' in request:
                abort_asset_update_request(
                    df_workload_client,
                    deployment_crn,
                    environment_crn,
                    asset_update_request_crn
                )
            raise

    def _validate_cfv_parameters(self,
                                 parameters):
        """
        Validates the parameters that were passed in to the
        change flow version command.
        """
        strategy = parameters.get('strategy', None)
        flow_bleed_out_time = parameters.get('waitForFlowToStopInMinutes', None)
        if (strategy == 'STOP_AND_EMPTY_QUEUES' or
                strategy == 'ONLY_RESTART_AFFECTED_COMPONENTS'):
            if flow_bleed_out_time is not None:
                err_msg = (('--waitForFlowToStopInMinutes is not required when '
                           'using the {strategy} change flow version strategy.')
                           .format(strategy=strategy))
                raise DfExtensionError(err_msg=err_msg,
                                       service_name='df',
                                       operation_name='changeFlowVersion')
        pass

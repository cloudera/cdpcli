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

from cdpcli.clidriver import CLIOperationCaller, ServiceOperation
from cdpcli.exceptions import CdpCLIError, ClientError, DfExtensionError
from cdpcli.extensions.df import (get_asset_update_request_crn,
                                  get_environment_crn,
                                  process_kpis)
from cdpcli.extensions.df.changeflowversion import (abort_asset_update_request,
                                                    has_asset_references,
                                                    upload_asset_references)
from cdpcli.extensions.df.model import (DEPLOYMENT_ALERT,
                                        DEPLOYMENT_ALERT_THRESHOLD,
                                        DEPLOYMENT_FLOW_PARAMETER_ASSET_REFERENCE,
                                        DEPLOYMENT_FLOW_PARAMETER_FOR_UPDATE,
                                        DEPLOYMENT_FLOW_PARAMETER_GROUP,
                                        DEPLOYMENT_FREQUENCY_TOLERANCE,
                                        DEPLOYMENT_KEY_PERFORMANCE_INDICATOR,
                                        PARAMETER_GROUP_REFERENCES)
from cdpcli.extensions.workload import set_workload_access_token
from cdpcli.model import ObjectShape, OperationModel, ShapeResolver
from cdpcli.utils import CachedProperty

LOG = logging.getLogger('cdpcli.extensions.df.retrychangeflowversion')

SERVICE_NAME = 'df'
OPERATION_NAME = 'retryChangeFlowVersion'
OPERATION_CLI_NAME = 'retry-change-flow-version'
OPERATION_SUMMARY = 'Retries a change flow version attempt of a deployment'
OPERATION_DESCRIPTION = """
    Retries a failed change flow version attempt of a deployment.
    This operation is supported for the CLI only.
    """
OPERATION_DATA = {
    'summary': OPERATION_SUMMARY,
    'description': OPERATION_DESCRIPTION,
    'operationId': OPERATION_NAME
}
OPERATION_SHAPES = {
    'RetryChangeFlowVersionRequest': {
        'type': 'object',
        'description': 'Request object for Retry Change '
                       'Flow version of a running deployment.',
        'required': ['serviceCrn', 'deploymentCrn'],
        'properties': {
            'serviceCrn': {
                'type': 'string',
                'description': 'CRN for the service.'
            },
            'deploymentCrn': {
                'type': 'string',
                'description': 'CRN for the deployment.'
            },
            'strategy': {
                'type': 'string',
                'description': 'The strategy to use for retry change flow version. '
                               'If nothing is specified, the strategy used in the '
                               'previous change flow version attempt will be used.',
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
                               'If nothing is specified and using the '
                               'STOP_AND_PROCESS_DATA strategy, then the'
                               'wait time used in the previous change flow version '
                               'attempt will be used.'
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
    'RetryChangeFlowVersionResponse': {
        'type': 'object',
        'description': 'Response for Retry Change Flow Version command.',
        'properties': {
            'crn': {
                'type': 'string',
                'description': 'CRN for the deployment where '
                               'retry change flow version was performed.'
            }
        }
    },
    'DeploymentFlowParameterGroup': DEPLOYMENT_FLOW_PARAMETER_GROUP,
    'DeploymentFlowParameter': DEPLOYMENT_FLOW_PARAMETER_FOR_UPDATE,
    'AssetReference': DEPLOYMENT_FLOW_PARAMETER_ASSET_REFERENCE,
    'DeploymentKeyPerformanceIndicator': DEPLOYMENT_KEY_PERFORMANCE_INDICATOR,
    'DeploymentAlert': DEPLOYMENT_ALERT,
    'DeploymentAlertThreshold': DEPLOYMENT_ALERT_THRESHOLD,
    'DeploymentFrequencyTolerance': DEPLOYMENT_FREQUENCY_TOLERANCE,
    'VersionedParameterGroupReference': PARAMETER_GROUP_REFERENCES
}


class RetryChangeFlowVersion(ServiceOperation):

    def __init__(self, clidriver, service_model):
        super(RetryChangeFlowVersion, self).__init__(
            clidriver=clidriver,
            name=OPERATION_CLI_NAME,
            parent_name=SERVICE_NAME,
            service_model=service_model,
            operation_model=RetryChangeFlowVersionOperationModel(service_model),
            operation_caller=RetryChangeFlowVersionOperationCaller())


class RetryChangeFlowVersionOperationModel(OperationModel):

    def __init__(self, service_model):
        super(RetryChangeFlowVersionOperationModel, self).__init__(
            operation_data=OPERATION_DATA,
            service_model=service_model,
            name=OPERATION_NAME,
            http_method=None,
            request_uri=None)

    @CachedProperty
    def input_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='input',
                           shape_data=OPERATION_SHAPES['RetryChangeFlowVersionRequest'],
                           shape_resolver=resolver)

    @CachedProperty
    def output_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='output',
                           shape_data=OPERATION_SHAPES['RetryChangeFlowVersionResponse'],
                           shape_resolver=resolver)


class RetryChangeFlowVersionOperationCaller(CLIOperationCaller):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        self._validate_retry_cfv_parameters(parameters)

        df_client = client_creator('df')

        service_crn = parameters.get('serviceCrn')

        environment_crn = get_environment_crn(df_client, service_crn)

        iam_client = client_creator('iam')
        set_workload_access_token(iam_client, parsed_globals, SERVICE_NAME.upper(),
                                  environment_crn)

        df_workload_client = client_creator('dfworkload')

        response = self._retry_change_flow_version(
            df_workload_client,
            environment_crn,
            parameters
        )
        self._display_response(operation_model.name, response, parsed_globals)

    def _retry_change_flow_version(self,
                                   df_workload_client,
                                   environment_crn,
                                   parameters):
        """
        Retries the change flow version of a running deployment. A key difference
        between a retry and the original change flow version is that a retry has no
        deployment request CRN, and will instead use the stored change
        flow version request details unless its overridden by parameters.
        """
        deployment_crn = parameters.get('deploymentCrn')
        request = {
            'environmentCrn': environment_crn,
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

        LOG.debug('Change Flow Version Parameters for retry %s', request)
        try:
            http, response = df_workload_client.make_api_call(
                'changeFlowVersion',
                request
            )

            # this should match the format of RetryChangeFlowVersionResponse
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

    def _validate_retry_cfv_parameters(self,
                                       parameters):
        """
        Validates the parameters that were passed in to the
        retry change flow version command.
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

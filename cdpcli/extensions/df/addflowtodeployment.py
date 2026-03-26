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
                                  get_flow_request_details_in_deployment,
                                  initiate_deployed_flow,
                                  process_kpis,
                                  upload_workload_asset)
from cdpcli.extensions.df.model import (DEPLOYMENT_ALERT,
                                        DEPLOYMENT_ALERT_THRESHOLD,
                                        DEPLOYMENT_FLOW_PARAMETER,
                                        DEPLOYMENT_FLOW_PARAMETER_GROUP,
                                        DEPLOYMENT_FREQUENCY_TOLERANCE,
                                        DEPLOYMENT_KEY_PERFORMANCE_INDICATOR,
                                        PARAMETER_GROUP_REFERENCES)
from cdpcli.extensions.workload import set_workload_access_token
from cdpcli.model import ObjectShape, OperationModel, ShapeResolver
from cdpcli.utils import CachedProperty

LOG = logging.getLogger('cdpcli.extensions.df.addflowtodeployment')
MAX_ASSET_SIZE = 150 * 1024 * 1024
INITIAL_CONFIGURATION_VERSION = 0
INITIAL_ASSET_VERSION = '0'

SERVICE_NAME = 'df'
OPERATION_NAME = 'addFlowToDeployment'
OPERATION_CLI_NAME = 'add-flow-to-deployment'
OPERATION_SUMMARY = 'Add a flow to an existing deployment on workload'
OPERATION_DESCRIPTION = """
    Add a flow to an existing deployment on workload. This operation is \
supported for the CLI only.
    """
OPERATION_DATA = {
    'summary': OPERATION_SUMMARY,
    'description': OPERATION_DESCRIPTION,
    'operationId': OPERATION_NAME,
}
OPERATION_SHAPES = {
    'AddFlowToDeploymentRequest': {
        'type': 'object',
        'description': 'Request object for adding a flow to an existing deployment.',
        'required': ['deploymentCrn', 'flowVersionCrn', 'name'],
        'properties': {
            'deploymentCrn': {
                'type': 'string',
                'description': 'CRN for the deployment.'
            },
            'flowVersionCrn': {
                'type': 'string',
                'description': 'CRN for the flow definition version.'
            },
            'name': {
                'type': 'string',
                'description': 'Unique name for the deployed flow within the deployment.'
            },
            'fromArchive': {
                'type': 'string',
                'description': 'The name of the deployment configuration archive to '
                               'import deployment configurations from.'
            },
            'importArchiveFlowName': {
                'type': 'string',
                'description': 'The name of the flow within the configuration archive to '
                               'import. This parameter is required when using '
                               '--from-archive.'
            },
            'autoStartFlow': {
                'type': 'boolean',
                'description': 'Automatically start the flow.'
            },
            'parameterGroups': {
                'type': 'array',
                'description': 'Parameter groups with each requiring a value or assets. '
                               'If --from-archive is used, then parameters defined here '
                               'will override what is defined in the archive. Sensitive '
                               'parameters must always be specified here.',
                'items': {
                    '$ref': '#/definitions/DeploymentFlowParameterGroup'
                }
            },
            'kpis': {
                'type': 'array',
                'description': 'Key Performance Indicators with associated alerts. '
                               'This argument will be ignored if --from-archive is used.',
                'items': {
                    '$ref': '#/definitions/DeploymentKeyPerformanceIndicator'
                }
            },
            'ignoreDeploymentInboundConfigurationChecks': {
                'type': 'boolean',
                'description': 'When specified, ignore the check to validate if '
                               'deployment has inbound connection configured for '
                               'all listen components.'
            },
        }
    },
    'AddFlowToDeploymentResponse': {
        'type': 'object',
        'description': 'Response for Add Flow To Deployment command.',
        'properties': {
            'crn': {
                'type': 'string',
                'description': 'CRN for the created deployed flow.'
            }
        }
    },
    'DeploymentFlowParameterGroup': DEPLOYMENT_FLOW_PARAMETER_GROUP,
    'DeploymentFlowParameter': DEPLOYMENT_FLOW_PARAMETER,
    'DeploymentKeyPerformanceIndicator': DEPLOYMENT_KEY_PERFORMANCE_INDICATOR,
    'DeploymentAlert': DEPLOYMENT_ALERT,
    'DeploymentAlertThreshold': DEPLOYMENT_ALERT_THRESHOLD,
    'DeploymentFrequencyTolerance': DEPLOYMENT_FREQUENCY_TOLERANCE,
    'VersionedParameterGroupReference': PARAMETER_GROUP_REFERENCES,
}


class AddFlowToDeployment(ServiceOperation):

    def __init__(self, clidriver, service_model):
        super(AddFlowToDeployment, self).__init__(
            clidriver=clidriver,
            name=OPERATION_CLI_NAME,
            parent_name=SERVICE_NAME,
            service_model=service_model,
            operation_model=AddFlowToDeploymentOperationModel(service_model),
            operation_caller=AddFlowToDeploymentOperationCaller())


class AddFlowToDeploymentOperationModel(OperationModel):

    def __init__(self, service_model):
        super(AddFlowToDeploymentOperationModel, self).__init__(
            operation_data=OPERATION_DATA,
            service_model=service_model,
            name=OPERATION_NAME,
            http_method=None,
            request_uri=None)

    @CachedProperty
    def input_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='input',
                           shape_data=OPERATION_SHAPES['AddFlowToDeploymentRequest'],
                           shape_resolver=resolver)

    @CachedProperty
    def output_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='output',
                           shape_data=OPERATION_SHAPES['AddFlowToDeploymentResponse'],
                           shape_resolver=resolver)


class AddFlowToDeploymentOperationCaller(CLIOperationCaller):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        df_client = client_creator('df')

        self._validate_operation_parameters(parameters)

        deployment_crn = parameters.get('deploymentCrn', None)
        flow_version_crn = parameters.get('flowVersionCrn', None)

        # Get service CRN, environment CRN, and deployment name from deployment CRN
        service_crn, environment_crn, deployment_name = (
            self._get_crns_and_deployment_name_from_deployment(
                df_client, deployment_crn))

        # Initiate the deployed flow request
        deployment_request_crn = initiate_deployed_flow(
                df_client, service_crn, deployment_crn, flow_version_crn)

        iam_client = client_creator('iam')
        set_workload_access_token(iam_client, parsed_globals, SERVICE_NAME.upper(),
                                  environment_crn)

        df_workload_client = client_creator('dfworkload')
        get_flow_request_details_in_deployment(
            df_workload_client, deployment_request_crn, environment_crn)
        try:
            self._upload_assets(df_workload_client, deployment_request_crn, parameters,
                                deployment_name)
        except (ClientError, DfExtensionError):
            self._abort_flow_request_in_deployment(
                df_workload_client, deployment_request_crn, environment_crn)
            raise
        response = self._add_flow_to_deployment(
                df_workload_client, deployment_request_crn, environment_crn, parameters)
        self._display_response(operation_model.name, response, parsed_globals)

    def _validate_operation_parameters(self,
                                       parameters):
        from_archive = parameters.get('fromArchive', None)
        flow_name = parameters.get('importArchiveFlowName', None)

        if from_archive is not None and flow_name is None:
            raise DfExtensionError(err_msg='--import-archive-flow-name is required when '
                                   '--from-archive is specified.',
                                   service_name='df',
                                   operation_name='addFlowToDeployment')

    def _get_crns_and_deployment_name_from_deployment(self, df_client, deployment_crn):
        """
        Get Service CRN, Environment CRN, and Deployment Name from Deployment CRN
        """
        http, response = df_client.make_api_call(
            'describeDeployment', {'deploymentCrn': deployment_crn})
        deployment = response.get('deployment', {})
        service = deployment.get('service', {})
        service_crn = service.get('crn', None)
        environment_crn = service.get('environmentCrn', None)
        deployment_name = deployment.get('name', None)
        LOG.debug('Found Service CRN [%s], Environment CRN [%s], and Deployment Name '
                  '[%s] for Deployment CRN [%s]', service_crn, environment_crn,
                  deployment_name, deployment_crn)
        return service_crn, environment_crn, deployment_name

    def _add_flow_to_deployment(self,
                                df_workload_client,
                                deployment_request_crn,
                                environment_crn,
                                parameters):
        """
        Add Flow to Deployment on Workload using Deployed Flow Request CRN
        """
        try:
            from_archive = parameters.get('fromArchive', None)
            flow_name = parameters.get('importArchiveFlowName', None)
            if from_archive is not None:
                import_deployment_configuration = self._import_flow_into_deployment(
                        df_workload_client,
                        deployment_request_crn,
                        from_archive,
                        flow_name,
                        environment_crn
                )
                LOG.debug('Imported Deployment Configuration %s',
                          import_deployment_configuration)
                flow_configuration = self._process_import_flow_configuration(
                        deployment_request_crn,
                        parameters,
                        import_deployment_configuration
                )
            else:
                flow_configuration = self._get_flow_configuration(
                    deployment_request_crn, parameters)

            flow_configuration['environmentCrn'] = environment_crn
            LOG.debug('Add Flow to Deployment Parameters %s', flow_configuration)
            http, response = df_workload_client.make_api_call(
                'addFlowToDeployment', flow_configuration)
            deployed_flow = response.get('deployedFlow', {})
            deployed_flow_crn = deployed_flow.get('crn', None)
            add_response = {
                'crn': deployed_flow_crn
            }
            return add_response
        except (ClientError, DfExtensionError):
            # attempts to clean up resources then
            # raise the error to pass on exception handling
            self._abort_flow_request_in_deployment(
                df_workload_client,
                deployment_request_crn,
                environment_crn
            )
            raise

    def _process_import_flow_configuration(self,
                                           deployment_request_crn,
                                           parameters,
                                           imported_deployment_configuration):
        flow_configuration = {
            'name': parameters.get('name', None),
            'deployedFlowRequestCrn': deployment_request_crn,
            'configurationVersion': INITIAL_CONFIGURATION_VERSION,
            'deploymentCrn': parameters.get('deploymentCrn', None)
        }

        # autoStartFlow should always be passed in
        autoStartFlow = parameters.get('autoStartFlow', None)
        if autoStartFlow is not None:
            flow_configuration['autoStartFlow'] = autoStartFlow

        # ignoreDeploymentInboundConfigurationChecks should always be passed in
        # if provided
        ignoreDeploymentInboundConfigurationChecks = parameters.get(
            'ignoreDeploymentInboundConfigurationChecks', None)
        if ignoreDeploymentInboundConfigurationChecks is not None:
            flow_configuration['ignoreDeploymentInboundConfigurationChecks'] = \
                ignoreDeploymentInboundConfigurationChecks

        # special handling for KPIs in order to unset KPI IDs
        kpis = imported_deployment_configuration.get('kpis', None)
        if kpis is not None:
            if type(kpis) is not list:
                raise DfExtensionError(err_msg='KPIs from archive not a list',
                                       service_name='df',
                                       operation_name='addFlowToDeployment')
            for kpi in kpis:
                kpi.pop('id', None)

            flow_configuration['kpis'] = kpis

        # parameter groups from the CLI arguments should always be passed in
        parameterGroups = parameters.get('parameterGroups', None)
        if parameterGroups:
            flow_configuration['parameterGroups'] = parameterGroups

        return flow_configuration

    def _get_flow_configuration(self,
                                deployment_request_crn,
                                parameters):
        """
        Get Flow Configuration request based on command parameters
        """
        flow_configuration = {
            'name': parameters.get('name', None),
            'deployedFlowRequestCrn': deployment_request_crn,
            'configurationVersion': INITIAL_CONFIGURATION_VERSION,
            'deploymentCrn': parameters.get('deploymentCrn', None)
        }

        autoStartFlow = parameters.get('autoStartFlow', None)
        if autoStartFlow is not None:
            flow_configuration['autoStartFlow'] = autoStartFlow

        ignoreDeploymentInboundConfigurationChecks = parameters.get(
            'ignoreDeploymentInboundConfigurationChecks', None)
        if ignoreDeploymentInboundConfigurationChecks is not None:
            flow_configuration['ignoreDeploymentInboundConfigurationChecks'] = \
                ignoreDeploymentInboundConfigurationChecks

        parameterGroups = parameters.get('parameterGroups', None)
        if parameterGroups:
            flow_configuration['parameterGroups'] = parameterGroups
        kpis = parameters.get('kpis', None)
        if kpis:
            # Handle the case where user passes '[]' as a string for empty KPIs
            if len(kpis) == 1 and kpis[0] == '[]':
                flow_configuration['kpis'] = []
            else:
                flow_configuration['kpis'] = process_kpis(kpis)
        else:
            # Always include kpis field, even if empty, since backend expects it
            flow_configuration['kpis'] = []

        projectCrn = parameters.get('projectCrn', None)
        if projectCrn is not None:
            flow_configuration['projectCrn'] = projectCrn

        return flow_configuration

    def _upload_assets(self,
                       df_workload_client,
                       deployment_request_crn,
                       parameters,
                       deployment_name):
        """
        Upload Assets associated with Flow when Asset References found
        """
        parameter_groups = parameters.get('parameterGroups', None)
        if parameter_groups:
            deployed_flow_name = parameters.get('name', None)

            for parameter_group in parameter_groups:
                parameters = parameter_group['parameters']
                for parameter in parameters:
                    asset_references = parameter.get('assetReferences', None)
                    if asset_references:
                        for asset_path in asset_references:
                            file_stats = os.stat(asset_path)
                            if file_stats.st_size > MAX_ASSET_SIZE:
                                raise DfExtensionError(
                                    err_msg='The file size exceeds '
                                            'the 150 MB limit, file: [{}]'
                                    .format(asset_path),
                                    service_name=df_workload_client.meta.service_model
                                    .service_name,
                                    operation_name='uploadAsset')

            for parameter_group in parameter_groups:
                parameter_group_name = parameter_group['name']
                parameters = parameter_group['parameters']
                for parameter in parameters:
                    asset_references = parameter.get('assetReferences', None)
                    if asset_references:
                        updated_asset_references = []
                        for asset_path in asset_references:
                            asset_params = {
                                'deploymentName': deployment_name,
                                'deploymentRequestCrn': deployment_request_crn,
                                'parameterGroup': parameter_group_name,
                                'parameterName': parameter.get('name', None),
                                'filePath': asset_path
                            }

                            # Add deployed flow name if available
                            if deployed_flow_name:
                                asset_params['deployedFlowName'] = deployed_flow_name

                            upload_workload_asset(df_workload_client, asset_params)

                            file_path = get_expanded_file_path(asset_path)
                            path, name = os.path.split(file_path)
                            asset_reference = {
                                'name': name,
                                'path': path,
                                'version': INITIAL_ASSET_VERSION
                            }
                            updated_asset_references.append(asset_reference)

                        parameter['assetReferences'] = updated_asset_references

    def _abort_flow_request_in_deployment(self,
                                          df_workload_client,
                                          deployment_request_crn,
                                          environment_crn):
        """
        Make a best effort attempt to clear up
        resources that need to be cleaned up upon
        deployed flow creation failure
        """
        try:
            parameters = {
                'deployedFlowRequestCrn': deployment_request_crn,
                'environmentCrn': environment_crn
            }
            http, response = df_workload_client.make_api_call(
                'abortFlowRequestInDeployment',
                parameters
            )
            LOG.debug('Successfully aborted flow deployment request with CRN: [%s]',
                      deployment_request_crn)
        except ClientError as e:
            if e.http_status_code >= 400:
                LOG.error(
                    'Failed to clean up flow deployment request with CRN: [%s]',
                    deployment_request_crn
                )
            else:
                LOG.error(
                    'Encountered an error while attempting to '
                    'abort flow deployment request with CRN: [%s]',
                    deployment_request_crn
                )

    def _import_flow_into_deployment(self,
                                     df_workload_client,
                                     deployed_flow_request_crn,
                                     archive_name,
                                     flow_name,
                                     environment_crn):
        """
        This function imports flow deployment configuration using the new
        import-flow-into-deployment endpoint.
        """
        parameters = {
            'deployedFlowRequestCrn': deployed_flow_request_crn,
            'archiveName': archive_name,
            'environmentCrn': environment_crn
        }
        if flow_name:
            parameters['flowName'] = flow_name

        http, response = df_workload_client.make_api_call(
            'importFlowIntoDeployment', parameters)
        LOG.debug('Imported flow deployment configuration from archive with name: [%s]'
                  'and for deployed flow request with CRN: [%s]',
                  archive_name, deployed_flow_request_crn)
        return response['rpcImportedDeploymentConfiguration']

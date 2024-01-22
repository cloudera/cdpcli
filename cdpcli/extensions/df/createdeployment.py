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
from cdpcli.extensions.df import get_expanded_file_path, upload_workload_asset
from cdpcli.extensions.workload import set_workload_access_token
from cdpcli.model import ObjectShape, OperationModel, ShapeResolver
from cdpcli.utils import CachedProperty

LOG = logging.getLogger('cdpcli.extensions.df.createdeployment')
MAX_ASSET_SIZE = 150 * 1024 * 1024
INITIAL_CONFIGURATION_VERSION = 0
INITIAL_ASSET_VERSION = '0'

SERVICE_NAME = 'df'
OPERATION_NAME = 'createDeployment'
OPERATION_CLI_NAME = 'create-deployment'
OPERATION_SUMMARY = 'Initiate and create deployment on workload'
OPERATION_DESCRIPTION = """
    Initiate deployment on the control plane, upload referenced when specified,
    and create the deployment on workload. This operation is supported for the CLI only.'
    """
OPERATION_DATA = {
    'summary': OPERATION_SUMMARY,
    'description': OPERATION_DESCRIPTION,
    'operationId': OPERATION_NAME,
}
OPERATION_SHAPES = {
    'CreateDeploymentRequest': {
        'type': 'object',
        'description': 'Request object for creating a deployment.',
        'required': ['serviceCrn', 'flowVersionCrn', 'deploymentName'],
        'properties': {
            'serviceCrn': {
                'type': 'string',
                'description': 'CRN for the service.'
            },
            'flowVersionCrn': {
                'type': 'string',
                'description': 'CRN for the flow definition version.'
            },
            'deploymentName': {
                'type': 'string',
                'description': 'Unique name for the deployment.'
            },
            'fromArchive': {
                'type': 'string',
                'description': 'The name of the deployment configuration archive to '
                               'import deployment configurations from. This argument '
                               'cannot be used with --import-parameters-from.'
            },
            'importParametersFrom': {
                'type': 'string',
                'description': 'The name of deployment configuration archive to '
                               'import parameter groups values from. This argument '
                               'cannot be used when using the --from-archive argument.'
            },
            'clusterSizeName': {
                'type': 'string',
                'description': 'Size for the cluster. The default is EXTRA_SMALL. This '
                               'argument will be ignored if --from-archive is used.',
                'enum': [
                    'EXTRA_SMALL',
                    'SMALL',
                    'MEDIUM',
                    'LARGE'
                ],
            },
            'staticNodeCount': {
                'type': 'integer',
                'description': 'The static number of nodes provisioned. '
                               'The default is 1. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'autoScalingEnabled': {
                'type': 'boolean',
                'description': 'Automatic node scaling. The default is disabled. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'flowMetricsScalingEnabled': {
                'type': 'boolean',
                'description': 'Flow metrics enabled for scaling. '
                               'The default is disabled. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'autoScaleMinNodes': {
                'type': 'integer',
                'description': 'The minimum number of nodes for automatic scaling. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'autoScaleMaxNodes': {
                'type': 'integer',
                'description': 'The maximum number of nodes for automatic scaling. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'cfmNifiVersion': {
                'type': 'string',
                'description': 'The CFM NiFi version. Defaults to the latest version. '
                               'This argument will be ignored if --from-archive is used.'
            },
            'autoStartFlow': {
                'type': 'boolean',
                'description': 'Automatically start the flow.'
            },
            'parameterGroups': {
                'type': 'array',
                'description': 'Parameter groups with each requiring a value or assets. '
                               'If --from-archive or --import-parameters-from is used, '
                               'then parameters defined here will override what is '
                               'defined in the archive. Sensitive parameters must '
                               'always be specified here.',
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
            'customNarConfiguration': {
                'type': 'object',
                'description': 'Custom NAR configuration properties. '
                               'This argument will be ignored if --from-archive is used.',
                'required': [
                    'username',
                    'password',
                    'storageLocation',
                    'configurationVersion'
                ],
                'properties': {
                    'username': {
                        'type': 'string',
                        'description': 'Username for access to NAR storage location'
                    },
                    'password': {
                        'type': 'string',
                        'description': 'Password for access to NAR storage location',
                        'x-sensitive': 'true'
                    },
                    'storageLocation': {
                        'type': 'string',
                        'description': 'Storage location containing custom NAR files',
                        'x-no-paramfile': 'true'
                    },
                    'configurationVersion': {
                        'type': 'integer',
                        'description': 'Custom configuration version number'
                    }
                }
            },
            'inboundHostname': {
                'type': 'string',
                'description':
                    'The FQDN of inbound host or just the prefix part of the hostname. '
                    'This argument will be ignored if --from-archive is used.'
            },
            'listenComponents': {
                'type': 'array',
                'description': 'Listen components port and protocol data. '
                               'This argument will be ignored if --from-archive is used.',
                'items': {
                    '$ref': '#/definitions/ListenComponent'
                }
            },
            'nodeStorageProfileName': {
                'type': 'string',
                'description': 'Node storage profile name. '
                               'This argument will be ignored if --from-archive is used.',
                'enum': [
                    'STANDARD_AWS',
                    'STANDARD_AZURE',
                    'PERFORMANCE_AWS',
                    'PERFORMANCE_AZURE'
                ]
            },
            'projectCrn': {
                'type': 'string',
                'description': 'CRN for the project to assign this deployment to. '
                               'Not specifying this will result in the '
                               'deployment to be unassigned to any project. '
                               'This argument will be ignored if --from-archive is used.'
            },
        }
    },
    'CreateDeploymentResponse': {
        'type': 'object',
        'description': 'Response for Create Deployment command.',
        'properties': {
            'crn': {
                'type': 'string',
                'description': 'CRN for the created deployment.'
            }
        }
    },
    'DeploymentFlowParameterGroup': {
        'type': 'object',
        'description': 'Parameter groups for NiFi flow deployment.',
        'required': ['name', 'parameters'],
        'properties': {
            'name': {
                'type': 'string',
                'description': 'Name for parameter group.'
            },
            'parameters': {
                'type': 'array',
                'description': 'Parameters for the parameter group.',
                'items': {
                    '$ref': '#/definitions/DeploymentFlowParameter'
                }
            },
        }
    },
    'DeploymentFlowParameter': {
        'type': 'object',
        'description': 'Parameter object for the NiFi flow deployment.',
        'required': ['name'],
        'properties': {
            'name': {
                'type': 'string',
                'description': 'Name for the parameter.'
            },
            'value': {
                'type': 'string',
                'description': 'Value for the named parameter.',
                'x-no-paramfile': 'true'
            },
            'assetReferences': {
                'type': 'array',
                'description': 'Asset references for the named parameter.',
                'items': {
                    'type': 'string'
                }
            }
        }
    },
    'DeploymentKeyPerformanceIndicator': {
        'type': 'object',
        'description': 'Key Performance Indicators for the deployment.',
        'required': ['metricId'],
        'properties': {
            'metricId': {
                'type': 'string',
                'description': 'Unique identifier for the metric object.'
            },
            'componentId': {
                'type': 'string',
                'description': 'Identifier for the NiFi component.'
            },
            'alert': {
                '$ref': '#/definitions/DeploymentAlert'
            }
        }
    },
    'DeploymentAlert': {
        'type': 'object',
        'properties': {
            'thresholdMoreThan': {
                '$ref': '#/definitions/DeploymentAlertThreshold',
                'description': 'The threshold above which alerts should be triggered.'
            },
            'thresholdLessThan': {
                '$ref': '#/definitions/DeploymentAlertThreshold',
                'description': 'The threshold below which alerts should be triggered.'
            },
            'frequencyTolerance': {
                '$ref': '#/definitions/DeploymentFrequencyTolerance'
            }
        }
    },
    'DeploymentAlertThreshold': {
        'type': 'object',
        'properties': {
            'unitId': {
                'type': 'string',
                'description': 'The unit identifier for the alert threshold.'
            },
            'value': {
                'type': 'double',
                'description': 'The numeric value for the alert threshold.'
            }
        }
    },
    'DeploymentFrequencyTolerance': {
        'type': 'object',
        'description': 'The frequency tolerance for the Key Performance Indicator.',
        'properties': {
            'value': {
                'type': 'double',
                'description': 'The amount of time before generating an alert.'
            },
            'unit': {
                'type': 'object',
                'description': 'The time unit for associated value number.',
                'properties': {
                    'id': {
                        'type': 'string',
                        'enum': [
                            'SECONDS',
                            'MINUTES',
                            'HOURS',
                            'DAYS'
                        ]
                    }
                }
            }
        }
    },
    'ListenComponent': {
        'type': 'object',
        'required': ['protocol', 'port'],
        'properties': {
            'protocol': {
                'type': 'string',
                'description': 'Inbound protocol',
                'enum': ['TCP', 'UDP']
            },
            'port': {
                'type': 'string',
                'description': 'Inbound port'
            }
        },
        'description': 'Provides subset of metadata of a Listen* component'
    }
}


class CreateDeployment(ServiceOperation):

    def __init__(self, clidriver, service_model):
        super(CreateDeployment, self).__init__(
            clidriver=clidriver,
            name=OPERATION_CLI_NAME,
            parent_name=SERVICE_NAME,
            service_model=service_model,
            operation_model=CreateDeploymentOperationModel(service_model),
            operation_caller=CreateDeploymentOperationCaller())


class CreateDeploymentOperationModel(OperationModel):

    def __init__(self, service_model):
        super(CreateDeploymentOperationModel, self).__init__(
            operation_data=OPERATION_DATA,
            service_model=service_model,
            name=OPERATION_NAME,
            http_method=None,
            request_uri=None)

    @CachedProperty
    def input_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='input',
                           shape_data=OPERATION_SHAPES['CreateDeploymentRequest'],
                           shape_resolver=resolver)

    @CachedProperty
    def output_shape(self):
        resolver = ShapeResolver(OPERATION_SHAPES)
        return ObjectShape(name='output',
                           shape_data=OPERATION_SHAPES['CreateDeploymentResponse'],
                           shape_resolver=resolver)


class CreateDeploymentOperationCaller(CLIOperationCaller):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        df_client = client_creator('df')

        self._validate_operation_parameters(parameters)

        service_crn = parameters.get('serviceCrn', None)
        flow_version_crn = parameters.get('flowVersionCrn', None)
        deployment_request_crn = self._initiate_deployment(
                df_client, service_crn, flow_version_crn)

        environment_crn = self._get_environment_crn(df_client, service_crn)

        iam_client = client_creator('iam')
        set_workload_access_token(iam_client, parsed_globals, SERVICE_NAME.upper(),
                                  environment_crn)

        df_workload_client = client_creator('dfworkload')
        self._get_deployment_request_details(
            df_workload_client, deployment_request_crn, environment_crn)
        try:
            self._upload_assets(df_workload_client, deployment_request_crn, parameters)
        except (ClientError, DfExtensionError):
            self._abort_deployment_request(
                df_workload_client, deployment_request_crn, environment_crn)
            raise
        response = self._create_deployment(
                df_workload_client, deployment_request_crn, environment_crn, parameters)
        self._display_response(operation_model.name, response, parsed_globals)

    def _validate_operation_parameters(self,
                                       parameters):
        from_archive = parameters.get('fromArchive', None)

        if from_archive is not None:
            if parameters.get('importParametersFrom', None) is not None:
                raise DfExtensionError(err_msg='Cannot use both --from-archive and '
                                               '--import-parameters-from arguments.',
                                       service_name='df',
                                       operation_name='createDeployment')

    def _create_deployment(self,
                           df_workload_client,
                           deployment_request_crn,
                           environment_crn,
                           parameters):
        """
        Create Deployment on Workload using initiated Deployment Request CRN
        """
        # initialize custom_nar_configuration because it's not always used
        # but necessary for clean-up in event of error
        custom_nar_configuration = None

        from_archive = parameters.get('fromArchive', None)
        if from_archive is not None:
            import_deployment_configuration = self._import_deployment(
                df_workload_client,
                deployment_request_crn,
                from_archive,
                environment_crn
            )
            LOG.debug('Imported Deployment Configuration %s',
                      import_deployment_configuration)
            deployment_configuration = self._process_import_deployment_configuration(
                deployment_request_crn,
                parameters,
                import_deployment_configuration
            )
        else:
            import_parameters_from = parameters.get('importParametersFrom', None)

            if import_parameters_from is not None:
                self._import_deployment(df_workload_client,
                                        deployment_request_crn,
                                        import_parameters_from,
                                        environment_crn)
            deployment_configuration = self._get_deployment_configuration(
                    deployment_request_crn, parameters)

            custom_nar_configuration = self._process_custom_nar_configuration(
                    df_workload_client, environment_crn, parameters)
            if custom_nar_configuration is not None:
                nar_configuration_crn = custom_nar_configuration['crn']
                deployment_configuration['customNarConfigurationCrn'] \
                    = nar_configuration_crn

        try:
            deployment_configuration['environmentCrn'] = environment_crn
            LOG.debug('Create Deployment Parameters %s', deployment_configuration)
            http, response = df_workload_client.make_api_call(
                'createDeployment', deployment_configuration)
            deployment = response.get('deployment', {})
            deployment_crn = deployment.get('crn', None)
            create_response = {
                'deploymentCrn': deployment_crn
            }
            return create_response
        except (ClientError, DfExtensionError):
            # attempts to clean up resources then
            # raise the error to pass on exception handling
            self._tear_down(
                df_workload_client,
                environment_crn,
                custom_nar_configuration,
                deployment_request_crn
            )
            raise

    def _initiate_deployment(self,
                             df_client,
                             service_crn,
                             flow_version_crn):
        """
        Initiate Deployment on Control Plane using Service CRN and Flow Version CRN
        """
        deployment_parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn
        }
        http, response = df_client.make_api_call(
                'initiateDeployment', deployment_parameters)
        request_crn = response.get('deploymentRequestCrn', None)
        url = response.get('dfxLocalUrl', None)
        LOG.debug('Initiated Deployment Request CRN [%s] URL [%s]', request_crn, url)
        return request_crn

    def _get_deployment_request_details(self,
                                        df_workload_client,
                                        deployment_request_crn,
                                        environment_crn):
        """
        This function fetches the deployment request details
        from the workload, and triggers the download of the flow.
        This also triggers the registration of the deployment
        details in the workload.
        This should be done right after the deployment is
        initiated due to the limited TTL of the resources
        generated during the deployment initiation.
        """
        parameters = {
            'deploymentRequestCrn': deployment_request_crn,
            'environmentCrn': environment_crn
        }
        http, response = df_workload_client.make_api_call(
            'getDeploymentRequestDetails', parameters)
        LOG.debug('Obtained Deployment Request Details for CRN [%s]',
                  deployment_request_crn)

    def _get_environment_crn(self,
                             df_client,
                             service_crn):
        """
        Get Environment CRN using Service CRN
        """
        http, response = df_client.make_api_call(
                'listDeployableServicesForNewDeployments', {})
        services = response.get('services', [])

        for service in services:
            crn = service.get('crn', None)
            if service_crn == crn:
                environment_crn = service.get('environmentCrn', None)
                LOG.debug('Found Environment CRN [%s]', environment_crn)
                return environment_crn
        raise DfExtensionError(err_msg='Environment CRN not found for Service CRN',
                               service_name='df',
                               operation_name='listDeployableServicesForNewDeployments')

    def _process_custom_nar_configuration(self,
                                          df_workload_client,
                                          environment_crn,
                                          parameters):
        """
        Process Custom NAR Configuration and return the response
        """
        custom_nar_configuration = parameters.get('customNarConfiguration', None)
        if custom_nar_configuration:
            custom_nar_configuration['environmentCrn'] = environment_crn

            try:
                http, response = df_workload_client.make_api_call(
                    'createCustomNarConfiguration',
                    custom_nar_configuration
                )
                crn = response.get('crn', None)
                LOG.debug('Created Custom NAR Configuration CRN [%s]', crn)
                return response
            except ClientError as e:
                if e.http_status_code == 409:
                    return self._get_default_nar_configuration(
                        df_workload_client,
                        environment_crn,
                        custom_nar_configuration
                    )
                else:
                    raise
        else:
            return None

    def _get_default_nar_configuration(self,
                                       df_workload_client,
                                       environment_crn,
                                       custom_nar_configuration):
        default_parameters = {
            'environmentCrn': environment_crn
        }
        default_http, default_configuration = df_workload_client.make_api_call(
            'getDefaultCustomNarConfiguration',
            default_parameters
        )
        custom_nar_configuration['crn'] = default_configuration.get('crn', None)
        default_version = default_configuration.get('configurationVersion', None)
        custom_nar_configuration['configurationVersion'] = default_version

        http, response = df_workload_client.make_api_call(
            'updateCustomNarConfiguration',
            custom_nar_configuration
        )
        crn = response.get('crn', None)
        LOG.debug('Updated Custom NAR Configuration CRN [%s]', crn)
        return response

    def _process_import_deployment_configuration(self,
                                                 deployment_request_crn,
                                                 parameters,
                                                 imported_deployment_configuration):
        deployment_configuration = {
            'name': parameters.get('deploymentName', None),
            'deploymentRequestCrn': deployment_request_crn,
            'configurationVersion': INITIAL_CONFIGURATION_VERSION
        }

        # This maps the field name in the imported_deployment_configuration
        # to the one expected in the create_deployment_configuration.
        # These items also do not require special handling.
        standard_config_items = {
            'clusterSizeName': 'clusterSizeName',
            'staticNodeCount': 'staticNodeCount',
            'cfmNifiVersion': 'cfmNifiVersion',
            'customNarConfigurationCrn': 'customNarConfigurationCrn',
            'nodeStorageProfile': 'nodeStorageProfileName',
            'projectCrn': 'projectCrn'
        }

        items = standard_config_items.items()
        for (imported_deployment_config_item, deployment_config_item) in items:
            value = imported_deployment_configuration.get(
                imported_deployment_config_item,
                None
            )
            if value is not None:
                deployment_configuration[deployment_config_item] = value

        # autoStartFlow should always be passed in
        autoStartFlow = parameters.get('autoStartFlow', None)
        if autoStartFlow is not None:
            deployment_configuration['autoStartFlow'] = autoStartFlow

        # special handling for autoscaling configuration items
        self._process_scaling_parameters(deployment_configuration,
                                         imported_deployment_configuration)

        # special handling for inbound connection parameters
        inboundHostname = imported_deployment_configuration.get('inboundHostName', None)
        if inboundHostname is not None:
            deployment_configuration['inboundHostname'] = inboundHostname
            deployment_configuration['listenComponents'] = \
                imported_deployment_configuration.get('listenComponents', None)

        # special handling for KPIs in order to unset KPI IDs
        kpis = imported_deployment_configuration.get('kpis', None)
        if kpis is not None:
            if type(kpis) is not list:
                raise DfExtensionError(err_msg='KPIs from archive not a list',
                                       service_name='df',
                                       operation_name='createDeployment')
            for kpi in kpis:
                kpi.pop('id', None)

            deployment_configuration['kpis'] = kpis

        # parameter groups from the CLI arguments should always be passed in
        parameterGroups = parameters.get('parameterGroups', None)
        if parameterGroups:
            deployment_configuration['parameterGroups'] = parameterGroups

        return deployment_configuration

    def _get_deployment_configuration(self,
                                      deployment_request_crn,
                                      parameters):
        """
        Get Deployment Configuration request based on command parameters
        """
        deployment_configuration = {
            'name': parameters.get('deploymentName', None),
            'deploymentRequestCrn': deployment_request_crn,
            'configurationVersion': INITIAL_CONFIGURATION_VERSION,
            'clusterSizeName': parameters.get('clusterSizeName', 'EXTRA_SMALL'),
            'staticNodeCount': parameters.get('staticNodeCount', 1)
        }

        # If nodeStorageProfileName is not set, then
        # sending it as empty in the configuration will trigger
        # dfx-local to choose the default nodeStorageProfileName for
        # the given cloud platform
        nodeStorageProfileName = parameters.get('nodeStorageProfileName', None)
        if nodeStorageProfileName is not None:
            deployment_configuration['nodeStorageProfileName'] = nodeStorageProfileName

        # If projectCrn is not set, then
        # sending it as empty in the configuration will trigger
        # dfx-local to not assign the created deployment to any project
        projectCrn = parameters.get('projectCrn', None)
        if projectCrn is not None:
            deployment_configuration['projectCrn'] = projectCrn

        inboundHostname = parameters.get('inboundHostname', None)
        if inboundHostname is not None:
            deployment_configuration['inboundHostname'] = inboundHostname
            deployment_configuration['listenComponents'] = \
                parameters.get('listenComponents', None)

        self._process_scaling_parameters(deployment_configuration, parameters)

        autoStartFlow = parameters.get('autoStartFlow', None)
        if autoStartFlow is not None:
            deployment_configuration['autoStartFlow'] = autoStartFlow

        cfmNifiVersion = parameters.get('cfmNifiVersion', None)
        if cfmNifiVersion:
            deployment_configuration['cfmNifiVersion'] = cfmNifiVersion

        parameterGroups = parameters.get('parameterGroups', None)
        if parameterGroups:
            deployment_configuration['parameterGroups'] = parameterGroups
        kpis = parameters.get('kpis', None)
        if kpis:
            deployment_configuration['kpis'] = self._process_kpis(kpis)

        return deployment_configuration

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
        return kpis

    def _process_scaling_parameters(self, deployment_configuration, parameters):
        autoScalingEnabled = parameters.get('autoScalingEnabled', None)
        if autoScalingEnabled is not None:
            deployment_configuration['autoScalingEnabled'] = autoScalingEnabled
        if autoScalingEnabled:
            deployment_configuration.pop('staticNodeCount', None)

            flowMetricsScalingEnabled = parameters.get('flowMetricsScalingEnabled', None)
            if flowMetricsScalingEnabled is not None:
                deployment_configuration['flowMetricsScalingEnabled'] \
                    = flowMetricsScalingEnabled

            autoScaleMinNodes = parameters.get('autoScaleMinNodes', None)
            if autoScaleMinNodes:
                deployment_configuration['autoScaleMinNodes'] = autoScaleMinNodes
            autoScaleMaxNodes = parameters.get('autoScaleMaxNodes', None)
            if autoScaleMaxNodes:
                deployment_configuration['autoScaleMaxNodes'] = autoScaleMaxNodes

    def _upload_assets(self,
                       df_workload_client,
                       deployment_request_crn,
                       parameters):
        """
        Upload Assets associated with Deployment when Asset References found
        """
        parameter_groups = parameters.get('parameterGroups', None)
        if parameter_groups:
            deployment_name = parameters.get('deploymentName', None)

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

    def _tear_down(self,
                   df_workload_client,
                   environment_crn,
                   custom_nar_configuration,
                   deployment_request_crn):
        """
        This function makes a best-effort attempt to clean-up
        resources that would otherwise be orphaned
        due to a deployment creation failure.
        """
        if (deployment_request_crn is not None):
            self._abort_deployment_request(
                df_workload_client,
                deployment_request_crn,
                environment_crn
            )
        if (custom_nar_configuration is not None):
            self._delete_custom_nar_configuration(
                df_workload_client,
                environment_crn,
                custom_nar_configuration
            )

    def _delete_custom_nar_configuration(self,
                                         df_workload_client,
                                         environment_crn,
                                         custom_nar_configuration):
        """
        Deletes Custom NAR Configuration
        """
        try:
            parameters = {}
            parameters['customNarConfigurationCrn'] = custom_nar_configuration['crn']
            parameters['configurationVersion'] = \
                custom_nar_configuration['configurationVersion']
            parameters['environmentCrn'] = environment_crn
            http, response = df_workload_client.make_api_call(
                'deleteCustomNarConfiguration',
                parameters
            )
            LOG.debug('Successfully deleted Custom NAR Configuration: [%s]', parameters)
        except ClientError as e:
            if e.http_status_code >= 400:
                LOG.error(
                    'Failed to clean up Custom NAR Configuration: [%s]',
                    parameters
                )
            else:
                LOG.error('Encountered an error while attempting to '
                          'cleanup Custom NAR configuration: [%s]', parameters)

    def _abort_deployment_request(self,
                                  df_workload_client,
                                  deployment_request_crn,
                                  environment_crn):
        """
        Make a best effort attempt to clear up
        resources that need to be cleaned up upon
        deployment creation failure
        """
        try:
            parameters = {
                'deploymentRequestCrn': deployment_request_crn,
                'environmentCrn': environment_crn
            }
            http, response = df_workload_client.make_api_call(
                'abortDeploymentRequest',
                parameters
            )
            LOG.debug('Successfully aborted deployment request with CRN: [%s]',
                      deployment_request_crn)
        except ClientError as e:
            if e.http_status_code >= 400:
                LOG.error(
                    'Failed to clean up deployment request with CRN: [%s]',
                    deployment_request_crn
                )
            else:
                LOG.error(
                    'Encountered an error while attempting to '
                    'abort deployment request with CRN: [%s]',
                    deployment_request_crn
                )

    def _import_deployment(self,
                           df_workload_client,
                           deployment_request_crn,
                           archive_name,
                           environment_crn):
        """
        This function imports deployment configuration.
        """
        parameters = {
            'deploymentRequestCrn': deployment_request_crn,
            'archiveName': archive_name,
            'environmentCrn': environment_crn
        }
        http, response = df_workload_client.make_api_call(
            'importDeployment', parameters)
        LOG.debug('Imported deployment configuration from archive with name: [%s]'
                  'and for deployment request with CRN: [%s]',
                  archive_name, deployment_request_crn)
        return response['rpcImportedDeploymentConfiguration']

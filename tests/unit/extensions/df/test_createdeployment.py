# Copyright (c) 2021 Cloudera, Inc. All rights reserved.
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

import os

from cdpcli.exceptions import ClientError, DfExtensionError
from cdpcli.extensions.df.createdeployment import CreateDeploymentOperationCaller
from cdpcli.extensions.df.createdeployment import CreateDeploymentOperationModel
from cdpcli.model import ServiceModel
from mock import Mock
from tests import unittest


BASE_DIR = os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + '/..')


class TestCreateDeployment(unittest.TestCase):

    def setUp(self):
        self.df_client = Mock()
        self.df_client.raise_error.side_effect = Exception('ClientError')

        self.df_workload_client = Mock()
        self.df_workload_client.raise_error.side_effect = Exception('ClientError')

        self.iam_client = Mock()
        self.iam_client.raise_error.side_effect = Exception('ClientError')

        self.client_creator = Mock()

        def _create_client(*args, **kwargs):
            service_name = args[0]
            if service_name == 'df':
                return self.df_client
            elif service_name == 'dfworkload':
                return self.df_workload_client
            elif service_name == 'iam':
                return self.iam_client
            else:
                raise Exception('Unexpected service_name [' + service_name + ']')
        self.client_creator.side_effect = _create_client

        service_model = ServiceModel({}, 'df')
        self.deployment_model = CreateDeploymentOperationModel(service_model)
        self.deployment_caller = CreateDeploymentOperationCaller()

    def test_invoke_create_deployment_environment_crn_not_found(self):
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name
        }
        parsed_args = {}
        parsed_globals = {}

        response = {
            'services': []
        }
        self.df_client.make_api_call.return_value = (Mock(status_code=200), response)

        with self.assertRaises(DfExtensionError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)

    def test_invoke_create_deployment_required_parameters(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        workload_url = 'https://localhost.localdomain/'

        initiate_request_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                initiate_deployment_response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': workload_url
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), initiate_deployment_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        token = 'WORKLOAD_TOKEN'
        auth_token_response = {
            'token': token,
            'endpointUrl': workload_url
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response

        create_deployment_parameters = []
        deployment_crn = 'DEPLOYMENT_CRN'

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'createDeployment':
                create_response = {
                    'deployment': {
                        'crn': deployment_crn
                    }
                }
                create_deployment_parameters.append(args[1])
                return (Mock(status_code=200), create_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                      parameters, parsed_args, parsed_globals)

        self.assertEquals(
            {
                'serviceCrn': service_crn,
                'flowVersionCrn': flow_version_crn
            },
            initiate_request_parameters[0])

        self.assertEquals('Bearer ' + token, parsed_globals.access_token)
        self.assertEquals(workload_url, parsed_globals.endpoint_url)

        self.assertEquals(
            {
                'environmentCrn': environment_crn,
                'deploymentRequestCrn': deployment_request_crn,
                'name': deployment_name,
                'configurationVersion': 0,
                'clusterSizeName': 'EXTRA_SMALL',
                'staticNodeCount': 1
            },
            create_deployment_parameters[0])

    def test_invoke_create_deployment_auto_scaling_auto_start(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        auto_scale_min_nodes = 1
        auto_scale_max_nodes = 3
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name,
            'autoScalingEnabled': True,
            'autoScaleMinNodes': auto_scale_min_nodes,
            'autoScaleMaxNodes': auto_scale_max_nodes,
            'autoStartFlow': True,
            'parameterGroups': [],
            'kpis': []
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        workload_url = 'https://localhost.localdomain/'

        initiate_request_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                initiate_deployment_response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': workload_url
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), initiate_deployment_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        token = 'WORKLOAD_TOKEN'
        auth_token_response = {
            'token': token,
            'endpointUrl': workload_url
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response

        create_deployment_parameters = []
        deployment_crn = 'DEPLOYMENT_CRN'

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'createDeployment':
                create_response = {
                    'deployment': {
                        'crn': deployment_crn
                    }
                }
                create_deployment_parameters.append(args[1])
                return (Mock(status_code=200), create_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                      parameters, parsed_args, parsed_globals)

        self.assertEquals(
            {
                'serviceCrn': service_crn,
                'flowVersionCrn': flow_version_crn
            },
            initiate_request_parameters[0])

        self.assertEquals('Bearer ' + token, parsed_globals.access_token)
        self.assertEquals(workload_url, parsed_globals.endpoint_url)

        self.assertEquals(
            {
                'environmentCrn': environment_crn,
                'deploymentRequestCrn': deployment_request_crn,
                'name': deployment_name,
                'configurationVersion': 0,
                'clusterSizeName': 'EXTRA_SMALL',
                'autoScalingEnabled': True,
                'autoScaleMinNodes': auto_scale_min_nodes,
                'autoScaleMaxNodes': auto_scale_max_nodes,
                'autoStartFlow': True
            },
            create_deployment_parameters[0])

    def test_invoke_create_deployment(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        parameter_group_name = 'ParameterGroup'
        parameter_name = 'Files'
        asset_reference_name = 'df-workload.asset.bin'
        asset_reference_file_path = os.path.join(BASE_DIR, asset_reference_name)

        frequencyUnit = {
            'id': 'MINUTES'
        }
        frequencyUnitUpdated = {
            'id': 'MINUTES',
            'label': 'Minutes',
            'abbreviation': 'm'
        }
        kpi = {
            'metricId': 'cpuUtilization',
            'alert': {
                'thresholdMoreThan': {
                    'unitId': 'percentage',
                    'value': 75.5
                },
                'frequencyTolerance': {
                    'unit': frequencyUnit,
                    'value': 2.5
                }
            }
        }

        custom_nar_configuration = {
            'username': 'USER',
            'password': 'PROTECTED',
            'storageLocation': 's3a://bucket',
            'configurationVersion': 0
        }

        cluster_size_name = 'MEDIUM'
        auto_scaling_enabled = False
        static_node_count = 2
        cfm_nifi_version = '1.14.0'
        auto_start_flow = False
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name,
            'clusterSizeName': cluster_size_name,
            'autoScalingEnabled': auto_scaling_enabled,
            'staticNodeCount': static_node_count,
            'cfmNifiVersion': cfm_nifi_version,
            'autoStartFlow': auto_start_flow,
            'parameterGroups': [
                {
                    'name': parameter_group_name,
                    'parameters': [
                        {
                            'name': parameter_name,
                            'assetReferences': [
                                asset_reference_file_path
                            ]
                        }
                    ]
                }
            ],
            'kpis': [
                kpi
            ],
            'customNarConfiguration': custom_nar_configuration
        }
        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        custom_nar_configuration_crn = 'NAR_CONFIGURATION_CRN'
        workload_url = 'https://localhost.localdomain/'

        initiate_request_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                initiate_deployment_response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': workload_url
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), initiate_deployment_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        token = 'WORKLOAD_TOKEN'
        auth_token_response = {
            'token': token,
            'endpointUrl': workload_url
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response

        create_deployment_parameters = []
        deployment_crn = 'DEPLOYMENT_CRN'

        create_custom_nar_configuration_params = []

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'createDeployment':
                create_response = {
                    'deployment': {
                        'crn': deployment_crn
                    }
                }
                create_deployment_parameters.append(args[1])
                return (Mock(status_code=200), create_response)
            elif operation_name == 'getDefaultCustomNarConfiguration':
                error = {
                    'error': {
                        'code': '404',
                        'message': 'Not Found'
                    }
                }
                raise ClientError(error, operation_name, 'dfworkload', 404, 'requestId')
            elif operation_name == 'createCustomNarConfiguration':
                create_custom_nar_configuration_params.append(args[1])
                configuration_response = {
                    'crn': custom_nar_configuration_crn
                }
                return (Mock(status_code=200), configuration_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        upload_parameters = []

        def _df_workload_make_request(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'uploadAsset':
                upload_parameters.append(args[3])
                return (Mock(status_code=200), {})
            else:
                raise Exception('Unexpected make_request [' + operation_name + ']')
        self.df_workload_client.make_request.side_effect = _df_workload_make_request

        self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                      parameters, parsed_args, parsed_globals)

        self.assertEquals(
            {
                'serviceCrn': service_crn,
                'flowVersionCrn': flow_version_crn
            },
            initiate_request_parameters[0])

        self.assertEquals('Bearer ' + token, parsed_globals.access_token)
        self.assertEquals(workload_url, parsed_globals.endpoint_url)

        self.assertEquals(
            {
                'Content-Type': 'application/octet-stream',
                'Deployment-Name': deployment_name,
                'Deployment-Request-Crn': deployment_request_crn,
                'File-Path': asset_reference_file_path,
                'Parameter-Group': parameter_group_name,
                'Parameter-Name': parameter_name
            },
            upload_parameters[0]
        )

        kpi['alert']['frequencyTolerance']['unit'] = frequencyUnitUpdated
        self.assertEquals(
            {
                'environmentCrn': environment_crn,
                'deploymentRequestCrn': deployment_request_crn,
                'name': deployment_name,
                'configurationVersion': 0,
                'clusterSizeName': cluster_size_name,
                'staticNodeCount': static_node_count,
                'cfmNifiVersion': cfm_nifi_version,
                'autoStartFlow': auto_start_flow,
                'autoScalingEnabled': auto_scaling_enabled,
                'parameterGroups': [
                    {
                        'name': parameter_group_name,
                        'parameters': [
                            {
                                'name': parameter_name,
                                'assetReferences': [
                                    {
                                        'name': asset_reference_name,
                                        'path': BASE_DIR,
                                        'version': '0'
                                    }
                                ]
                            }
                        ]
                    }
                ],
                'kpis': [
                    kpi
                ],
                'customNarConfigurationCrn': custom_nar_configuration_crn
            },
            create_deployment_parameters[0])

        custom_nar_configuration['environmentCrn'] = environment_crn
        self.assertEquals(
            custom_nar_configuration,
            create_custom_nar_configuration_params[0]
        )

    def test_invoke_create_deployment_default_custom_nar_configuration_found(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'
        custom_nar_configuration_crn = 'NAR_CONFIGURATION_CRN'

        custom_nar_configuration = {
            'username': 'UPDATED',
            'password': 'PROTECTED',
            'storageLocation': 's3a://bucket',
            'configurationVersion': 1
        }

        default_custom_nar_configuration = {
            'crn': custom_nar_configuration_crn,
            'username': 'USER',
            'password': 'PROTECTED',
            'storageLocation': 's3a://bucket',
            'configurationVersion': 1
        }

        cluster_size_name = 'MEDIUM'
        auto_scaling_enabled = False
        static_node_count = 2
        cfm_nifi_version = '1.14.0'
        auto_start_flow = False
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name,
            'clusterSizeName': cluster_size_name,
            'autoScalingEnabled': auto_scaling_enabled,
            'staticNodeCount': static_node_count,
            'cfmNifiVersion': cfm_nifi_version,
            'autoStartFlow': auto_start_flow,
            'customNarConfiguration': custom_nar_configuration
        }
        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        workload_url = 'https://localhost.localdomain/'

        initiate_request_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                initiate_deployment_response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': workload_url
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), initiate_deployment_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        token = 'WORKLOAD_TOKEN'
        auth_token_response = {
            'token': token,
            'endpointUrl': workload_url
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response

        create_deployment_parameters = []
        deployment_crn = 'DEPLOYMENT_CRN'

        create_custom_nar_configuration_params = []

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'createDeployment':
                create_response = {
                    'deployment': {
                        'crn': deployment_crn
                    }
                }
                create_deployment_parameters.append(args[1])
                return (Mock(status_code=200), create_response)
            elif operation_name == 'getDefaultCustomNarConfiguration':
                return (Mock(status_code=200), default_custom_nar_configuration)
            elif operation_name == 'updateCustomNarConfiguration':
                create_custom_nar_configuration_params.append(args[1])
                configuration_response = {
                    'crn': custom_nar_configuration_crn
                }
                return (Mock(status_code=200), configuration_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        upload_parameters = []

        def _df_workload_make_request(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'uploadAsset':
                upload_parameters.append(args[3])
                return (Mock(status_code=200), {})
            else:
                raise Exception('Unexpected make_request [' + operation_name + ']')
        self.df_workload_client.make_request.side_effect = _df_workload_make_request

        self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                      parameters, parsed_args, parsed_globals)

        self.assertEquals(
            {
                'serviceCrn': service_crn,
                'flowVersionCrn': flow_version_crn
            },
            initiate_request_parameters[0])

        self.assertEquals('Bearer ' + token, parsed_globals.access_token)
        self.assertEquals(workload_url, parsed_globals.endpoint_url)

        self.assertEquals(
            {
                'environmentCrn': environment_crn,
                'deploymentRequestCrn': deployment_request_crn,
                'name': deployment_name,
                'configurationVersion': 0,
                'clusterSizeName': cluster_size_name,
                'staticNodeCount': static_node_count,
                'cfmNifiVersion': cfm_nifi_version,
                'autoStartFlow': auto_start_flow,
                'autoScalingEnabled': auto_scaling_enabled,
                'customNarConfigurationCrn': custom_nar_configuration_crn
            },
            create_deployment_parameters[0])

        custom_nar_configuration['environmentCrn'] = environment_crn
        self.assertEquals(
            custom_nar_configuration,
            create_custom_nar_configuration_params[0]
        )

    def test_invoke_create_deployment_initiate_error(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                error_response = {
                    'error': {
                        'code': '500',
                        'message': 'Server Error'
                    }
                }
                raise ClientError(error_response, operation_name, 'df', 500, 'requestId')
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        with self.assertRaises(ClientError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)

        self.assertEqual(2, self.df_client.make_api_call.call_count)
        self.assertEqual(0, self.iam_client.make_api_call.call_count)
        self.assertEqual(0, self.df_workload_client.make_api_call.call_count)

    def test_invoke_create_deployment_workload_error(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_name = 'DEPLOYMENT'

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentName': deployment_name
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        workload_url = 'https://localhost.localdomain/'

        initiate_request_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'listDeployableServicesForNewDeployments':
                return self._get_deployable_services(service_crn, environment_crn)
            elif operation_name == 'initiateDeployment':
                initiate_deployment_response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': workload_url
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), initiate_deployment_response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_client.make_api_call.side_effect = _df_make_api_call

        token = 'WORKLOAD_TOKEN'
        auth_token_response = {
            'token': token,
            'endpointUrl': workload_url
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'createDeployment':
                error_response = {
                    'error': {
                        'code': '403',
                        'message': 'Access Denied'
                    }
                }
                raise ClientError(error_response, operation_name, 'df', 403, 'requestId')
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        with self.assertRaises(ClientError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)

        self.assertEqual(2, self.df_client.make_api_call.call_count)
        self.assertEqual(1, self.iam_client.generate_workload_auth_token.call_count)
        self.assertEqual(1, self.df_workload_client.make_api_call.call_count)

    def _get_deployable_services(self, service_crn, environment_crn):
        response = {
            'services': [
                {
                    'crn': service_crn,
                    'environmentCrn': environment_crn
                }
            ]
        }
        return (Mock(status_code=200), response)

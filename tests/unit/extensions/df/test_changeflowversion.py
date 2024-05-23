# Copyright (c) 2024 Cloudera, Inc. All rights reserved.
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
from cdpcli.extensions.df.changeflowversion import ChangeFlowVersionOperationCaller
from cdpcli.extensions.df.changeflowversion import ChangeFlowVersionOperationModel
from cdpcli.model import ServiceModel
from mock import Mock
from tests import unittest

BASE_DIR = os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + '/..')
WORKLOAD_URL = 'https://localhost.localdomain/'
TOKEN = 'WORKLOAD_TOKEN'


class TestChangeFlowVersion(unittest.TestCase):

    def setUp(self):
        self.df_client = Mock()
        self.df_client.raise_error.side_effect = Exception('ClientError')

        self.df_workload_client = Mock()
        self.df_workload_client.raise_error.side_effect = Exception('ClientError')

        self.iam_client = Mock()
        auth_token_response = {
            'token': TOKEN,
            'endpointUrl': WORKLOAD_URL
        }
        self.iam_client.generate_workload_auth_token.return_value = auth_token_response
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
        self.deployment_model = ChangeFlowVersionOperationModel(service_model)
        self.deployment_caller = ChangeFlowVersionOperationCaller()

    def test_invoke_change_flow_version_success(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_crn = 'DEPLOYMENT_CRN'
        strategy = 'STOP_AND_PROCESS_DATA'
        wait_for_flow_to_stop_in_minutes = 20
        parameter_group_name = 'ParameterGroup'
        parameter_name = 'Files'
        asset_reference_name = 'df-workload.asset.bin'
        asset_reference_file_path = os.path.join(BASE_DIR, asset_reference_name)
        existing_asset_name = 'existing-asset'

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
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentCrn': deployment_crn,
            'strategy': strategy,
            'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes,
            'parameterGroups': [
                {
                    'name': parameter_group_name,
                    'parameters': [
                        {
                            'name': parameter_name,
                            'assetReferences': [
                                {
                                    'name': existing_asset_name
                                },
                                {
                                    'path': asset_reference_file_path
                                }
                            ]
                        }
                    ]
                }
            ],
            'kpis': [
                kpi
            ]
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        asset_update_request_crn = 'ASSET_UPDATE_REQUEST_CRN'

        initiate_request_parameters = []
        get_deployment_request_details_parameters = []
        create_asset_update_request_parameters = []
        upload_parameters = []
        change_flow_version_parameters = []

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'initiateDeployment':
                response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': WORKLOAD_URL
                }
                initiate_request_parameters.append(args[1])
                return (Mock(status_code=200), response)
            elif operation_name == 'listDeployableServicesForNewDeployments':
                response = {
                    'services': [
                        {
                            'crn': service_crn,
                            'environmentCrn': environment_crn
                        }
                    ]
                }
                return (Mock(status_code=200), response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        def _df_workload_create_asset_update_request_crn(*args, **kwargs):
            create_asset_update_request_parameters.append(kwargs)
            response = {
                'assetUpdateRequestCrn': asset_update_request_crn
            }
            return response

        def _df_workload_make_request(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'uploadAsset':
                upload_parameters.append(args[3])
                return (Mock(status_code=200), {})
            else:
                raise Exception('Unexpected make_request [' + operation_name + ']')

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'getDeploymentRequestDetails':
                get_deployment_request_details_parameters.append(args[1])
                return (Mock(status_code=200), {})
            elif operation_name == 'changeFlowVersion':
                # the fields here correspond to the
                # ChangeFlowVersionResponse object in DFX
                response = {
                    'deploymentConfiguration': {
                        'deploymentCrn': deployment_crn
                    }
                }
                change_flow_version_parameters.append(args[1])
                return (Mock(status_code=200), response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        self.df_client.make_api_call.side_effect = _df_make_api_call
        self.df_workload_client.create_asset_update_request.side_effect \
            = _df_workload_create_asset_update_request_crn
        self.df_workload_client.make_request.side_effect = _df_workload_make_request
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                      parameters, parsed_args, parsed_globals)

        self.assertEquals(
            {
                'serviceCrn': service_crn,
                'flowVersionCrn': flow_version_crn,
                'deploymentCrn': deployment_crn
            },
            initiate_request_parameters[0]
        )

        self.assertEquals('Bearer ' + TOKEN, parsed_globals.access_token)
        self.assertEquals(WORKLOAD_URL, parsed_globals.endpoint_url)

        self.assertEquals(
            {
                'deploymentRequestCrn': deployment_request_crn,
                'environmentCrn': environment_crn
            },
            get_deployment_request_details_parameters[0]
        )

        self.assertEquals(
            {
                'deploymentCrn': deployment_crn,
                'environmentCrn': environment_crn
            },
            create_asset_update_request_parameters[0]
        )

        # this ensures that only the edited asset was uploaded
        # even though there are two asset references
        self.assertEquals(1, len(upload_parameters))

        self.assertEquals(
            {
                'Content-Type': 'application/octet-stream',
                'Asset-Update-Request-Crn': asset_update_request_crn,
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
                'deploymentCrn': deployment_crn,
                'strategy': strategy,
                'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes,
                'assetUpdateRequestCrn': asset_update_request_crn,
                'parameterGroups': [
                    {
                        'name': parameter_group_name,
                        'parameters': [
                            {
                                'name': parameter_name,
                                'assetReferences': [
                                    {
                                        'name': existing_asset_name
                                    },
                                    {
                                        'name': asset_reference_name,
                                        'path': BASE_DIR,
                                    }
                                ]
                            }
                        ]
                    }
                ],
                'kpis': [
                    kpi
                ]
            },
            change_flow_version_parameters[0]
        )

    def test_invoke_change_flow_version_environment_crn_not_found(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_crn = 'DEPLOYMENT_CRN'
        wait_for_flow_to_stop_in_minutes = 20
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentCrn': deployment_crn,
            'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'initiateDeployment':
                response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': WORKLOAD_URL
                }
                return (Mock(status_code=200), response)
            elif operation_name == 'listDeployableServicesForNewDeployments':
                response = {
                    'services': []
                }
                return (Mock(status_code=200), response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        self.df_client.make_api_call.side_effect = _df_make_api_call

        with self.assertRaises(DfExtensionError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)
        self.assertEqual(2, self.df_client.make_api_call.call_count)
        self.assertEqual(0, self.iam_client.generate_workload_auth_token.call_count)
        self.assertEqual(
            0,
            self.df_workload_client.create_asset_update_request.call_count
        )
        self.assertEqual(0, self.df_workload_client.make_api_call.call_count)
        self.assertEqual(0, self.df_workload_client.make_request.call_count)

    def test_invoke_change_flow_version_deployment_crn_not_found(self):
        # initiate deployment will fail if deployment_crn is not found in the DFX
        # backend
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_crn = 'DEPLOYMENT_CRN'
        wait_for_flow_to_stop_in_minutes = 20
        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentCrn': deployment_crn,
            'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'initiateDeployment':
                error = {
                    'error': {
                        'code': '404',
                        'message': 'Deployment Not Found'
                    }
                }
                raise ClientError(error, operation_name, 'df', 404, 'requestId')
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        self.df_client.make_api_call.side_effect = _df_make_api_call

        with self.assertRaises(ClientError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)
        self.assertEqual(1, self.df_client.make_api_call.call_count)
        self.assertEqual(0, self.iam_client.generate_workload_auth_token.call_count)
        self.assertEqual(
            0,
            self.df_workload_client.create_asset_update_request.call_count
        )
        self.assertEqual(0, self.df_workload_client.make_api_call.call_count)
        self.assertEqual(0, self.df_workload_client.make_request.call_count)

    def test_failed_change_flow_version_should_abort_asset_update_request(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_crn = 'DEPLOYMENT_CRN'
        wait_for_flow_to_stop_in_minutes = 20
        parameter_group_name = 'ParameterGroup'
        parameter_name = 'Files'
        asset_reference_name = 'df-workload.asset.bin'
        asset_reference_file_path = os.path.join(BASE_DIR, asset_reference_name)

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentCrn': deployment_crn,
            'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes,
            'parameterGroups': [
                {
                    'name': parameter_group_name,
                    'parameters': [
                        {
                            'name': parameter_name,
                            'assetReferences': [
                                {
                                    'path': asset_reference_file_path
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        environment_crn = 'ENVIRONMENT_CRN'
        deployment_request_crn = 'DEPLOYMENT_REQUEST_CRN'
        asset_update_request_crn = 'ASSET_UPDATE_REQUEST_CRN'

        def _df_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'initiateDeployment':
                response = {
                    'deploymentRequestCrn': deployment_request_crn,
                    'dfxLocalUrl': WORKLOAD_URL
                }
                return (Mock(status_code=200), response)
            elif operation_name == 'listDeployableServicesForNewDeployments':
                response = {
                    'services': [
                        {
                            'crn': service_crn,
                            'environmentCrn': environment_crn
                        }
                    ]
                }
                return (Mock(status_code=200), response)
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        def _df_workload_create_asset_update_request_crn(*args, **kwargs):
            response = {
                'assetUpdateRequestCrn': asset_update_request_crn
            }
            return response

        def _df_workload_make_request(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'uploadAsset':
                return (Mock(status_code=200), {})
            else:
                raise Exception('Unexpected make_request [' + operation_name + ']')

        def _df_workload_make_api_call(*args, **kwargs):
            operation_name = args[0]
            if operation_name == 'getDeploymentRequestDetails':
                return (Mock(status_code=200), {})
            elif operation_name == 'abortAssetUpdateRequest':
                return (Mock(status_code=200), {})
            elif operation_name == 'changeFlowVersion':
                error_response = {
                    'error': {
                        'code': '403',
                        'message': 'Access Denied'
                    }
                }
                print('raising')
                raise ClientError(error_response, operation_name, 'df', 403, 'requestId')
            else:
                raise Exception('Unexpected make_api_call [' + operation_name + ']')

        self.df_client.make_api_call.side_effect = _df_make_api_call
        self.df_workload_client.create_asset_update_request.side_effect \
            = _df_workload_create_asset_update_request_crn
        self.df_workload_client.make_request.side_effect = _df_workload_make_request
        self.df_workload_client.make_api_call.side_effect = _df_workload_make_api_call

        with self.assertRaises(ClientError):
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)
        self.assertEqual(2, self.df_client.make_api_call.call_count)
        self.assertEqual(1, self.iam_client.generate_workload_auth_token.call_count)
        self.assertEqual(
            1,
            self.df_workload_client.create_asset_update_request.call_count
        )
        self.assertEqual(3, self.df_workload_client.make_api_call.call_count)
        self.assertEqual(1, self.df_workload_client.make_request.call_count)

    def test_change_flow_version_strategy_validation_failure(self):
        self.maxDiff = 2000
        service_crn = 'SERVICE_CRN'
        flow_version_crn = 'FLOW_VERSION_CRN'
        deployment_crn = 'DEPLOYMENT_CRN'
        strategy = 'STOP_AND_EMPTY_QUEUES'
        wait_for_flow_to_stop_in_minutes = 20

        parameters = {
            'serviceCrn': service_crn,
            'flowVersionCrn': flow_version_crn,
            'deploymentCrn': deployment_crn,
            'strategy': strategy,
            'waitForFlowToStopInMinutes': wait_for_flow_to_stop_in_minutes,
        }

        parsed_args = {}
        parsed_globals = Mock()
        parsed_globals.output = 'json'

        with self.assertRaises(DfExtensionError) as err:
            self.deployment_caller.invoke(self.client_creator, self.deployment_model,
                                          parameters, parsed_args, parsed_globals)
            self.assertTrue('change flow version strategy' in err.msg)
        self.assertEqual(0, self.df_client.make_api_call.call_count)
        self.assertEqual(0, self.iam_client.generate_workload_auth_token.call_count)
        self.assertEqual(
            0,
            self.df_workload_client.create_asset_update_request.call_count
        )
        self.assertEqual(0, self.df_workload_client.make_api_call.call_count)
        self.assertEqual(0, self.df_workload_client.make_request.call_count)

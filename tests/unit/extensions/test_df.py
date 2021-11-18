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

from cdpcli.exceptions import DfExtensionError
from cdpcli.extensions.df import DfExtension, upload_workload_asset
from mock import Mock
from tests import unittest


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDfExtension(unittest.TestCase):

    def setUp(self):
        self.client = Mock()
        self.client.raise_error.side_effect = Exception('ClientError')
        self.client_creator = Mock(return_value=self.client)
        self.parsed_globals = Mock(output=None)

    def test_df_upload_flow(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='df')
        operation_model.name = 'importFlowDefinition'
        operation_model.http = {
            'method': 'post',
            'requestUri': 'https://test.com/path/importFlowDefinition'}
        parameters = {
            'name': 'flow_name',
            'description': 'flow_description\nline 2',
            'comments': 'flow_comments',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('importFlowDefinition', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/importFlowDefinition', args[2])
        self.assertEqual(
            {'Content-Type': 'application/json',
             'Flow-Definition-Name': 'flow_name',
             'Flow-Definition-Description': 'flow_description%0Aline%202',
             'Flow-Definition-Comments': 'flow_comments'},
            args[3])
        body = body_list[0]
        self.assertEqual(
            (b'{"flowContents":{"identifier":"97047f2a-9d0d-3669-977e-9d022308feb9",'
             b'"name":"Simple","comments":""},"parameterContexts":{},"flowEncoding'
             b'Version":"1.0"}'),
            body)

    def test_df_upload_flow_no_optional_parameters(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='df')
        operation_model.name = 'importFlowDefinition'
        operation_model.http = {
            'method': 'post',
            'requestUri': 'https://test.com/path/importFlowDefinition'}
        parameters = {
            'name': 'flow_name',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('importFlowDefinition', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/importFlowDefinition', args[2])
        self.assertEqual(
            {'Content-Type': 'application/json',
             'Flow-Definition-Name': 'flow_name'},
            args[3])
        body = body_list[0]
        self.assertEqual(
            (b'{"flowContents":{"identifier":"97047f2a-9d0d-3669-977e-9d022308feb9",'
             b'"name":"Simple","comments":""},"parameterContexts":{},"flowEncoding'
             b'Version":"1.0"}'),
            body)

    def test_df_upload_flow_version(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='df')
        operation_model.name = 'importFlowDefinitionVersion'
        operation_model.http = {
            'method': 'post',
            'requestUri': 'https://test.com/path/importFlowDefinitionVersion'}
        parameters = {
            'flowCrn': 'flow_crn',
            'comments': 'flow_comments\non 2 lines',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('importFlowDefinitionVersion', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/importFlowDefinitionVersion', args[2])
        self.assertEqual(
            {'Content-Type': 'application/json',
             'Flow-Definition-Comments': 'flow_comments%0Aon%202%20lines'},
            args[3])
        body = body_list[0]
        self.assertEqual(
            (b'{"flowContents":{"identifier":"97047f2a-9d0d-3669-977e-9d022308feb9",'
             b'"name":"Simple","comments":""},"parameterContexts":{},"flowEncoding'
             b'Version":"1.0"}'),
            body)

    def test_df_upload_flow_version_no_optional_parameters(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='df')
        operation_model.name = 'importFlowDefinitionVersion'
        operation_model.http = {
            'method': 'post',
            'requestUri': 'https://test.com/path/importFlowDefinitionVersion'}
        parameters = {
            'flowCrn': 'flow_crn',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('importFlowDefinitionVersion', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/importFlowDefinitionVersion', args[2])
        self.assertEqual(
            {'Content-Type': 'application/json'},
            args[3])
        body = body_list[0]
        self.assertEqual(
            (b'{"flowContents":{"identifier":"97047f2a-9d0d-3669-977e-9d022308feb9",'
             b'"name":"Simple","comments":""},"parameterContexts":{},"flowEncoding'
             b'Version":"1.0"}'),
            body)

    def test_df_workload_upload_asset(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='dfworkload')
        operation_model.name = 'uploadAsset'
        operation_model.http = {
            'method': 'post',
            'requestUri': '/dfx/api/rpc-v1/deployments/uploadAsset'}
        parameters = {
            'environmentCrn': 'env_crn',
            'parameterGroup': 'param_group',
            'parameterName': 'param_name',
            'deploymentRequestCrn': 'deployment_request_crn',
            'deploymentName': 'deployment_name',
            'assetUpdateRequestCrn': 'asset_update_request_crn',
            'filePath': os.path.join(BASE_DIR, 'df-workload.asset.bin')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('uploadAsset', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('/dfx/api/rpc-v1/deployments/upload-asset-content', args[2])
        self.assertEqual(
            {'Content-Type': 'application/octet-stream',
             'Deployment-Request-Crn': 'deployment_request_crn',
             'Deployment-Name': 'deployment_name',
             'Asset-Update-Request-Crn': 'asset_update_request_crn',
             'Parameter-Group': 'param_group',
             'Parameter-Name': 'param_name',
             'File-Path': os.path.join(BASE_DIR, 'df-workload.asset.bin')},
            args[3])
        body = body_list[0]
        self.assertIsNotNone(body)
        self.assertEqual(42, len(body))

    def test_df_workload_upload_asset_no_optional_parameters(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        operation_model = Mock()
        operation_model.service_model = Mock(service_name='dfworkload')
        operation_model.name = 'uploadAsset'
        operation_model.http = {
            'method': 'post',
            'requestUri': '/dfx/api/rpc-v1/deployments/uploadAsset'}
        parameters = {
            'environmentCrn': 'env_crn',
            'parameterGroup': 'param_group',
            'parameterName': 'param_name',
            'filePath': os.path.join(BASE_DIR, 'df-workload.asset.bin')}
        df_extension = DfExtension()
        df_extension.invoke(self.client_creator, operation_model,
                            parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('uploadAsset', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('/dfx/api/rpc-v1/deployments/upload-asset-content', args[2])
        self.assertEqual(
            {'Content-Type': 'application/octet-stream',
             'Parameter-Group': 'param_group',
             'Parameter-Name': 'param_name',
             'File-Path': os.path.join(BASE_DIR, 'df-workload.asset.bin')},
            args[3])
        body = body_list[0]
        self.assertIsNotNone(body)
        self.assertEqual(42, len(body))

    def test_operation_not_supported(self):
        operation_model = Mock()
        operation_model.service_model = Mock(service_name='foo')
        operation_model.name = 'importFlowDefinition'
        df_extension = DfExtension()
        with self.assertRaisesRegex(DfExtensionError,
                                    'The DF extension failed: '
                                    'The operation is not supported. '
                                    'Service name: foo, '
                                    'operation name: importFlowDefinition'):
            df_extension.invoke(self.client_creator, operation_model,
                                None, None, None)
        self.assertEqual(0, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)

    def test_upload_file_not_found(self):
        parameters = {
            'environmentCrn': 'env_crn',
            'parameterGroup': 'param_group',
            'parameterName': 'param_name',
            'filePath': os.path.join(BASE_DIR, 'file-not-found')
        }

        with self.assertRaises(DfExtensionError):
            upload_workload_asset(self.client, parameters)
        self.assertEqual(0, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)

    def test_kpis(self):
        operation_model = Mock()
        operation_model.name = 'updateDeployment'
        operation_model.service_model = Mock(service_name='dfworkload')

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
            'kpis': [
                kpi
            ]
        }

        parsed_args = Mock()
        parsed_globals = Mock()

        extension = DfExtension()

        result = extension.invoke(self.client_creator,
                                  operation_model,
                                  parameters,
                                  parsed_args,
                                  parsed_globals)
        self.assertTrue(result)

        self.assertEquals(
            kpi['alert']['frequencyTolerance']['unit'],
            frequencyUnitUpdated
        )

    def test_no_asset_references(self):
        operation_model = Mock()
        operation_model.name = 'updateDeployment'
        operation_model.service_model = Mock(service_name='dfworkload')
        parameters = {
            'parameterGroups': [
                {
                    'parameters': [
                        {
                            'name': 'parameterName',
                            'value': 'parameterValue'
                        }
                    ]
                }
            ]
        }
        parsed_args = Mock()
        parsed_globals = Mock()

        extension = DfExtension()

        result = extension.invoke(self.client_creator,
                                  operation_model,
                                  parameters,
                                  parsed_args,
                                  parsed_globals)
        self.assertTrue(result)

    def test_asset_references_uploaded(self):
        operation_model = Mock()
        operation_model.name = 'updateDeployment'
        operation_model.service_model = Mock(service_name='dfworkload')

        deployment_crn = 'DEPLOYMENT_CRN'
        asset_reference_name = 'df-workload.asset.bin'
        asset_reference_file_path = os.path.join(BASE_DIR, asset_reference_name)
        parameter_group_name = 'TestParameterGroup'
        parameter_name = 'TestParameter'

        parameters = {
            'deploymentCrn': deployment_crn,
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
        parsed_args = Mock()
        parsed_globals = Mock()

        extension = DfExtension()

        asset_update_request_crn = 'ASSET_UPDATE_REQUEST_CRN'
        update_response = {
            'assetUpdateRequestCrn': asset_update_request_crn
        }
        self.client.create_asset_update_request.return_value = update_response

        self.client.make_request.return_value = (Mock(status_code=200), {})

        result = extension.invoke(self.client_creator,
                                  operation_model,
                                  parameters,
                                  parsed_args,
                                  parsed_globals)
        self.assertTrue(result)

        found_request_crn = parameters.get('assetUpdateRequestCrn', None)
        self.assertEquals(asset_update_request_crn, found_request_crn)

        self.assertEquals(
            {
                'Content-Type': 'application/octet-stream',
                'Asset-Update-Request-Crn': asset_update_request_crn,
                'File-Path': asset_reference_file_path,
                'Parameter-Group': parameter_group_name,
                'Parameter-Name': parameter_name
            },
            self.client.make_request.call_args[0][3]
        )

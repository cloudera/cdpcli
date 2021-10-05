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
            'description': 'flow_description',
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
             'Flow-Definition-Description': 'flow_description',
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
            'comments': 'flow_comments',
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
             'Flow-Definition-Comments': 'flow_comments'},
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

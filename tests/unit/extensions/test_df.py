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
from cdpcli.extensions.df import UploadFileToDf
from mock import Mock
from tests import unittest


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class TestDfExtension(unittest.TestCase):

    def setUp(self):
        self.client = Mock()
        self.client.raise_error.side_effect = Exception('ClientError')
        self.client_creator = Mock(return_value=self.client)
        self.operation_model = Mock()
        self.operation_model.service_model = Mock(service_name='df')
        self.operation_model.name = 'uploadFlow'
        self.operation_model.http = {'method': 'post',
                                     'requestUri': 'https://test.com/path/uploadFlow'}
        self.parsed_globals = Mock(output=None)

    def test_df_upload_flow(self):
        body_list = []

        def _make_request(*args, **kwargs):
            body_list.append(args[4].read())
            return Mock(status_code=200), {}

        self.client.make_request.side_effect = _make_request

        parameters = {
            'name': 'flow_name',
            'description': 'flow_description',
            'comments': 'flow_comments',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        upload_file_to_df = UploadFileToDf()
        upload_file_to_df.invoke(self.client_creator, self.operation_model,
                                 parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('uploadFlow', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/uploadFlow', args[2])
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

        parameters = {
            'name': 'flow_name',
            'file': os.path.join(BASE_DIR, 'df.flow.json')}
        upload_file_to_df = UploadFileToDf()
        upload_file_to_df.invoke(self.client_creator, self.operation_model,
                                 parameters, None, self.parsed_globals)

        self.assertEqual(1, self.client.make_request.call_count)
        args, kwargs = self.client.make_request.call_args
        self.assertEqual('uploadFlow', args[0])
        self.assertEqual('post', args[1])
        self.assertEqual('https://test.com/path/uploadFlow', args[2])
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

    def test_operation_not_supported(self):
        operation_model = Mock()
        operation_model.service_model = Mock(service_name='foo')
        operation_model.name = 'uploadFlow'
        upload_file_to_df = UploadFileToDf()
        with self.assertRaisesRegex(DfExtensionError,
                                    'The DF extension failed: '
                                    'The operation is not supported. '
                                    'Service name: foo, operation name: uploadFlow'):
            upload_file_to_df.invoke(self.client_creator, operation_model,
                                     None, None, None)
        self.assertEqual(0, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)

# Copyright (c) 2023 Cloudera, Inc. All rights reserved.
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

from cdpcli.extensions.pvcapipath import PvcApiPath
from mock import Mock
from tests import unittest


class TestPvcApiPathExtension(unittest.TestCase):

    def setUp(self):
        self.client_creator = Mock()
        self.operation_model = Mock()
        self.parameters = Mock()
        self.parsed_args = Mock()
        self.parsed_globals = Mock()

    def test_pvc_api_path_updated_by_private_cloud(self):
        self.operation_model.http = {'method': 'post', 'requestUri': '/operation'}

        extension = PvcApiPath('private')
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/api/v1/operation',
                          self.operation_model.http['requestUri'])

    def test_pvc_api_path_updated_for_empty_path_by_private_cloud(self):
        self.operation_model.http = {'method': 'post', 'requestUri': ''}

        extension = PvcApiPath('private')
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/api/v1',
                          self.operation_model.http['requestUri'])

    def test_pvc_api_path_no_change_prefix_already_present_by_private_cloud(self):
        self.operation_model.http = {'method': 'post', 'requestUri': '/api/v1/operation'}

        extension = PvcApiPath('private')
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/api/v1/operation',
                          self.operation_model.http['requestUri'])

    def test_pvc_api_path_no_change_by_public_cloud(self):
        self.operation_model.http = {'method': 'post', 'requestUri': '/operation'}

        extension = PvcApiPath('public')
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/operation',
                          self.operation_model.http['requestUri'])

    def test_pvc_api_path_no_change_by_other_form_factor(self):
        self.operation_model.http = {'method': 'post', 'requestUri': '/operation'}

        extension = PvcApiPath('other')
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/operation',
                          self.operation_model.http['requestUri'])

    def test_pvc_api_path_no_change_by_missing_form_factor(self):
        self.operation_model.http = {'method': 'post', 'requestUri': '/operation'}

        extension = PvcApiPath(None)
        extension.invoke(self.client_creator, self.operation_model,
                         self.parameters, self.parsed_args, self.parsed_globals)

        self.assertEquals('/operation',
                          self.operation_model.http['requestUri'])

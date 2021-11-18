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
from cdpcli.extensions.df import upload_file
from cdpcli.extensions.df.createdeployment import CreateDeploymentOperationModel
from cdpcli.model import ServiceModel
from mock import Mock
from tests import unittest


BASE_DIR = os.path.normpath(os.path.dirname(os.path.abspath(__file__)) + '/..')


class TestUploadFile(unittest.TestCase):

    def setUp(self):
        self.df_client = Mock()
        self.df_client.raise_error.side_effect = Exception('ClientError')

        service_model = ServiceModel({}, 'df')
        self.deployment_model = CreateDeploymentOperationModel(service_model)

    def test_upload_file_path(self):
        file_name = 'df-workload.asset.bin'
        file_path = os.path.join(BASE_DIR, file_name)
        headers = {
            'Content-Type': 'application/octet-stream'
        }

        expected = {
            'crn': 'UPLOAD_CRN'
        }
        self.df_client.make_request.return_value = (Mock(status_code=200), expected)

        response = upload_file(self.df_client,
                               'importFlowDefinition',
                               'POST', 'http://localhost', headers, file_path)

        self.assertEquals(expected, response)

    def test_upload_file_path_not_found(self):
        file_name_not_found = 'file_not_found'
        file_path = os.path.join(BASE_DIR, file_name_not_found)
        headers = {
            'Content-Type': 'application/octet-stream'
        }

        with self.assertRaises(DfExtensionError):
            upload_file(self.df_client,
                        'importFlowDefinition',
                        'POST', 'http://localhost', headers, file_path)

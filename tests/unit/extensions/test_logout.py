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

from cdpcli import CDP_ACCESS_KEY_ID_KEY_NAME, CDP_PRIVATE_KEY_KEY_NAME
from cdpcli.extensions.configure import CREDENTIAL_FILE_COMMENT
from cdpcli.extensions.logout import LogoutCommand
import mock
from tests import unittest
from tests.unit import FakeContext


class TestLogoutCommand(unittest.TestCase):

    def setUp(self):
        self.writer = mock.Mock()
        self.context = FakeContext({'config_file': 'myconfigfile'})
        self.args = []
        self.logout = LogoutCommand(config_writer=self.writer)

    def assert_credentials_file_updated_with(self,
                                             new_values,
                                             config_file_comment=None):
        called_args = self.writer.update_config.call_args_list
        credentials_file_call = called_args[0]
        expected_creds_file = os.path.expanduser('~/fake_credentials_filename')
        self.assertEqual(credentials_file_call,
                         mock.call(new_values,
                                   expected_creds_file,
                                   config_file_comment=config_file_comment))

    def test_logout_command_succeeded_default_profile(self):
        parsed_globals = mock.Mock()
        self.logout(self.context, args=self.args, parsed_globals=parsed_globals)
        self.assert_credentials_file_updated_with(
            {CDP_ACCESS_KEY_ID_KEY_NAME: '',
             CDP_PRIVATE_KEY_KEY_NAME: ''},
            config_file_comment=CREDENTIAL_FILE_COMMENT)

    def test_logout_command_succeeded_for_profile(self):
        parsed_globals = mock.Mock()
        parsed_globals.profile = 'myname'
        self.context.effective_profile = 'myname'
        self.logout(self.context, args=self.args, parsed_globals=parsed_globals)
        self.assert_credentials_file_updated_with(
            {CDP_ACCESS_KEY_ID_KEY_NAME: '',
             CDP_PRIVATE_KEY_KEY_NAME: '',
             '__section__': 'myname'},
            config_file_comment=CREDENTIAL_FILE_COMMENT)

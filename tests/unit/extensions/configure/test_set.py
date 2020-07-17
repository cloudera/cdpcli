# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2016 Cloudera, Inc. All rights reserved.
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
from cdpcli.extensions.configure.set import ConfigureSetCommand
import mock
from tests import unittest
from tests.unit import FakeContext


class TestConfigureSetCommand(unittest.TestCase):

    def setUp(self):
        self.context = FakeContext({'config_file': 'myconfigfile'})
        self.fake_credentials_filename = os.path.expanduser(
            '~/fake_credentials_filename')
        self.context.profile = None
        self.config_writer = mock.Mock()

    def test_configure_set_command(self):
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['region', 'us-west-2'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'default', 'region': 'us-west-2'}, 'myconfigfile')

    def test_configure_set_command_dotted(self):
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['preview.foo', 'true'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'preview', 'foo': 'true'}, 'myconfigfile')

    def test_configure_set_command_dotted_with_default_profile(self):
        self.context.variables['profile'] = 'default'
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['foo.instance_profile', 'my_ip_foo'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'default',
             'foo': {'instance_profile': 'my_ip_foo'}}, 'myconfigfile')

    def test_configure_set_command_dotted_with_profile(self):
        self.context.profile = 'thu-dev'
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['thu.instance_profile', 'my_ip_thu'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'profile thu-dev', 'thu':
                {'instance_profile': 'my_ip_thu'}}, 'myconfigfile')

    def test_configure_set_with_profile(self):
        self.context.profile = 'testing'
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['region', 'us-west-2'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'profile testing', 'region': 'us-west-2'},
            'myconfigfile')

    def test_configure_set_triple_dotted(self):
        # cdp configure set default.s3.signature_version s3v4
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['default.s3.signature_version', 's3v4'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'default', 's3': {'signature_version': 's3v4'}},
            'myconfigfile')

    def test_configure_set_with_profile_nested(self):
        # cdp configure set default.s3.signature_version s3v4
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['profile.foo.s3.signature_version', 's3v4'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'profile foo',
             's3': {'signature_version': 's3v4'}}, 'myconfigfile')

    def test_access_key_written_to_shared_credentials_file(self):
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=[CDP_ACCESS_KEY_ID_KEY_NAME, 'foo'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'default',
             CDP_ACCESS_KEY_ID_KEY_NAME: 'foo'}, self.fake_credentials_filename)

    def test_private_key_written_to_shared_credentials_file(self):
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=[CDP_PRIVATE_KEY_KEY_NAME, 'foo'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'default',
             CDP_PRIVATE_KEY_KEY_NAME: 'foo'}, self.fake_credentials_filename)

    def test_access_key_written_to_shared_credentials_file_profile(self):
        set_command = ConfigureSetCommand(self.config_writer)
        set_command(self.context,
                    args=['profile.foo.cdp_access_key_id', 'bar'],
                    parsed_globals=None)
        self.config_writer.update_config.assert_called_with(
            {'__section__': 'foo',
             CDP_ACCESS_KEY_ID_KEY_NAME: 'bar'}, self.fake_credentials_filename)

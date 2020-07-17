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

from cdpcli.compat import six
from cdpcli.extensions.configure.get import ConfigureGetCommand
from tests import unittest
from tests.unit import FakeContext


class TestConfigureGetCommand(unittest.TestCase):

    def test_configure_get_command(self):
        context = FakeContext({})
        context.config['region'] = 'us-west-2'
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context, args=['region'], parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'us-west-2')

    def test_configure_get_command_no_exist(self):
        no_vars_defined = {}
        context = FakeContext(no_vars_defined)
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        rc = config_get(context, args=['region'], parsed_globals=None)
        rendered = stream.getvalue()
        # If a config value does not exist, we don't print any output.
        self.assertEqual(rendered, '')
        # And we exit with an rc of 1.
        self.assertEqual(rc, 1)

    def test_dotted_get(self):
        context = FakeContext({})
        context.full_config = {'preview': {'foo': 'true'}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context, args=['preview.foo'], parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'true')

    def test_predefined_section_with_profile(self):
        # Test that we retrieve the predefined section config var even if it's
        # under a profile.
        context = FakeContext({})
        context.full_config = {'profiles': {'thu-dev': {
            'thu': {'instance_profile': 'my_ip'}}, 'preview': {'foo': 'true'}}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context, args=['preview.foo'], parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'true')

    def test_dotted_get_with_profile(self):
        context = FakeContext({})
        context.full_config = {'profiles': {'thu-dev': {
            'thu': {'instance_profile': 'my_ip'}}}}
        context.config = {'thu': {'instance_profile': 'my_ip'}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['thu-dev.thu.instance_profile'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'my_ip')

    def test_get_from_profile(self):
        context = FakeContext({})
        context.full_config = \
            {'profiles': {'testing': {'cdp_access_key_id': 'access_key'}}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['profile.testing.cdp_access_key_id'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'access_key')

    def test_get_nested_attribute(self):
        context = FakeContext({})
        context.full_config = {
            'profiles': {'testing': {'s3': {'signature_version': 's3v4'}}}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['profile.testing.s3.signature_version'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 's3v4')

    def test_get_nested_attribute_from_default(self):
        context = FakeContext({})
        context.full_config = {
            'profiles': {'default': {'s3': {'signature_version': 's3v4'}}}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['default.s3.signature_version'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 's3v4')

    def test_get_nested_attribute_from_default_does_not_exist(self):
        context = FakeContext({})
        context.full_config = {'profiles': {}}
        stream = six.StringIO()
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['default.s3.signature_version'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), '')

    def test_dotted_not_in_full_config_get(self):
        context = FakeContext({})
        context.full_config = {
            'profiles': {'dev': {'someconf': {'foobar': 'true'}}}}
        stream = six.StringIO()
        context.variables['profile'] = 'dev'
        config_get = ConfigureGetCommand(stream)
        config_get(context,
                   args=['someconf.foobar'],
                   parsed_globals=None)
        rendered = stream.getvalue()
        self.assertEqual(rendered.strip(), 'true')

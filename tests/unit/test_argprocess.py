# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import json


from cdpcli import xform_name
from cdpcli.argprocess import uri_param
from cdpcli.arguments import BooleanArgument
from cdpcli.arguments import CLIArgument
from cdpcli.arguments import ListArgument
import mock
from tests import BaseCLIDriverTest
from tests import temporary_file


# These tests use real service types so that we can
# verify the real shapes of services.
class BaseArgProcessTest(BaseCLIDriverTest):

    def get_param_model(self, dotted_name):
        service_name, operation_name, param_name = dotted_name.split('.')
        service_model = self.driver.get_service_model(service_name)
        operation = service_model.operation_model(operation_name)
        input_shape = operation.input_shape
        required_arguments = input_shape.required_members
        is_required = param_name in required_arguments
        member_shape = input_shape.members[param_name]
        type_name = member_shape.type_name
        cli_arg_name = xform_name(param_name, '-')
        if type_name == 'boolean':
            cls = BooleanArgument
        elif type_name == 'list':
            cls = ListArgument
        else:
            cls = CLIArgument
        return cls(cli_arg_name, member_shape, mock.Mock(), is_required)

    def create_argument(self, argument_name=None):
        if argument_name is None:
            argument_name = 'foo'
        argument = mock.Mock()
        argument.name = argument_name
        argument.cli_name = "--" + argument_name
        return argument


class TestURIParams(BaseArgProcessTest):
    def test_uri_param(self):
        p = self.get_param_model('iam.getUser.userId')
        with temporary_file('r+') as f:
            json_argument = json.dumps([{"Name": "user-id", "Values": ["u-1234"]}])
            f.write(json_argument)
            f.flush()
            result = uri_param(p, 'file://%s' % f.name)
        self.assertEqual(result, json_argument)

    def test_uri_param_no_paramfile_false(self):
        p = self.get_param_model('iam.getUser.userId')
        p.no_paramfile = False
        with temporary_file('r+') as f:
            json_argument = json.dumps([{"Name": "user-id", "Values": ["u-1234"]}])
            f.write(json_argument)
            f.flush()
            result = uri_param(p, 'file://%s' % f.name)
        self.assertEqual(result, json_argument)

    def test_uri_param_no_paramfile_true(self):
        p = self.get_param_model('iam.getUser.userId')
        p.no_paramfile = True
        with temporary_file('r+') as f:
            json_argument = json.dumps([{"Name": "user-id", "Values": ["u-1234"]}])
            f.write(json_argument)
            f.flush()
            result = uri_param(p, 'file://%s' % f.name)
        self.assertEqual(result, None)

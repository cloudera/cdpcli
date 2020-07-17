# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import shutil
import tempfile

from cdpcli.argprocess import ParamError
from cdpcli.extensions.cliinputjson import add_cli_input_json
from cdpcli.extensions.cliinputjson import CliInputJSONArgument
from cdpcli.model import DenormalizedStructureBuilder
import mock
from tests import unittest


def _make_file_uri(file_name):
    # this is needed for windows but
    # just to be sure on mac/unix
    file_name = file_name.replace("\\", "/")
    return file_name


class TestCliInputJSONArgument(unittest.TestCase):
    def setUp(self):
        # Make an arbitrary input model shape.
        self.input_shape = {
            'A': {'type': 'string'},
            'B': {'type': 'string'}
        }
        shape = DenormalizedStructureBuilder().with_members(
            self.input_shape).build_model()
        self.operation_model = mock.Mock(input_shape=shape)
        self.argument = CliInputJSONArgument(self.operation_model)

        # Create the various forms the data could come in. The two main forms
        # are as a string and or as a path to a file.
        self.input_json = '{"A": "foo", "B": "bar"}'

        # Make a temporary file
        self.temp_dir = tempfile.mkdtemp()
        self.temp_file = os.path.join(self.temp_dir, 'foo.json')
        with open(self.temp_file, 'w') as f:
            f.write(self.input_json)

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_add_argument_action(self):
        argument_table = {}
        add_cli_input_json(self.operation_model, argument_table)
        arg_handler = argument_table['cli-input-json']
        self.assertIsNotNone(arg_handler)
        self.assertTrue(isinstance(arg_handler, CliInputJSONArgument))

    def test_add_to_call_parameters_no_file(self):
        parsed_args = mock.Mock()
        # Make the value a JSON string
        parsed_args.cli_input_json = self.input_json
        call_parameters = {}
        self.argument.add_to_call_parameters(
            call_parameters=call_parameters,
            parsed_args=parsed_args
        )
        self.assertEqual(call_parameters, {'A': 'foo', 'B': 'bar'})

    def test_add_to_call_parameters_with_file(self):
        parsed_args = mock.Mock()
        # Make the value a file with JSON located inside.
        parsed_args.cli_input_json = 'file://' + self.temp_file
        call_parameters = {}
        self.argument.add_to_call_parameters(
            call_parameters=call_parameters,
            parsed_args=parsed_args
        )
        self.assertEqual(call_parameters, {'A': 'foo', 'B': 'bar'})

    def test_add_to_call_parameters_with_file_from_visitor(self):
        temp_file1 = os.path.join(self.temp_dir, 'foo.json')
        with open(temp_file1, 'w') as f:
            f.write("baz")
        temp_file1 = _make_file_uri(temp_file1)
        temp_file2 = os.path.join(self.temp_dir, 'bar.json')
        with open(temp_file2, 'w') as f:
            f.write('{"A": "foo", "B": "file://%s"}' % temp_file1)
        temp_file2 = _make_file_uri(temp_file2)
        parsed_args = mock.Mock()
        parsed_args.cli_input_json = 'file://' + temp_file2
        call_parameters = {}
        self.argument.add_to_call_parameters(
            call_parameters=call_parameters,
            parsed_args=parsed_args
        )
        self.assertEqual(call_parameters, {'A': 'foo', 'B': 'baz'})

    def test_add_to_call_parameters_bad_json(self):
        parsed_args = mock.Mock()
        # Create a bad JSON input
        parsed_args.cli_input_json = self.input_json + ','
        call_parameters = {}
        with self.assertRaises(ParamError):
            self.argument.add_to_call_parameters(
                call_parameters=call_parameters,
                parsed_args=parsed_args
            )

    def test_add_to_call_parameters_no_clobber(self):
        parsed_args = mock.Mock()
        parsed_args.cli_input_json = self.input_json
        # The value for ``A`` should not be clobbered by the input JSON
        call_parameters = {'A': 'baz'}
        self.argument.add_to_call_parameters(
            call_parameters=call_parameters,
            parsed_args=parsed_args
        )
        self.assertEqual(call_parameters, {'A': 'baz', 'B': 'bar'})

    def test_no_add_to_call_parameters(self):
        parsed_args = mock.Mock()
        parsed_args.cli_input_json = None
        call_parameters = {'A': 'baz'}
        self.argument.add_to_call_parameters(
            call_parameters=call_parameters,
            parsed_args=parsed_args
        )
        # Nothing should have been added to the call parameters because
        # ``cli_input_json`` is not in the ``parsed_args``
        self.assertEqual(call_parameters, {'A': 'baz'})

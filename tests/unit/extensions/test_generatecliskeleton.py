# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from cdpcli.extensions.generatecliskeleton import add_generate_skeleton
from cdpcli.extensions.generatecliskeleton import GenerateCliSkeletonArgument
from cdpcli.model import DenormalizedStructureBuilder
import mock
from tests import unittest

from . import FakeArgs


class TestGenerateCliSkeleton(unittest.TestCase):
    def setUp(self):
        # Create a mock service operation object
        self.service_operation = mock.Mock()

        # Make an arbitrary input model shape.
        self.input_shape = {
            'A': {
                'type': 'object',
                'properties': {
                    'B': {'type': 'string'},
                }
            }
        }
        shape = DenormalizedStructureBuilder().with_members(
            self.input_shape).build_model()
        self.operation_model = mock.Mock(input_shape=shape)
        self.argument = GenerateCliSkeletonArgument(self.operation_model)

        # This is what the json should should look like after being
        # generated to standard output.
        self.ref_json_output = \
            '{\n    "A": {\n        "B": ""\n    }\n}\n'

    def test_add_argument_action(self):
        argument_table = {}
        add_generate_skeleton(self.operation_model, argument_table)
        arg_handler = argument_table['generate-cli-skeleton']
        self.assertIsNotNone(arg_handler)
        self.assertTrue(isinstance(arg_handler, GenerateCliSkeletonArgument))

    def test_generate_json_skeleton(self):
        parsed_args = mock.Mock()
        parsed_args.generate_cli_skeleton = True
        with mock.patch('sys.stdout', six.StringIO()) as mock_stdout:
            rc = self.argument._generate_json_skeleton(
                parsed_args=parsed_args
            )
            # Ensure the contents printed to standard output are correct.
            self.assertEqual(self.ref_json_output, mock_stdout.getvalue())
            # Ensure it is the correct return code of zero.
            self.assertEqual(rc, 0)

    def test_no_generate_json_skeleton(self):
        parsed_args = FakeArgs(generate_cli_skeleton=False)
        with mock.patch('sys.stdout', six.StringIO()) as mock_stdout:
            rc = self.argument._generate_json_skeleton(
                parsed_args=parsed_args
            )
            # Ensure nothing is printed to standard output
            self.assertEqual('', mock_stdout.getvalue())
            # Ensure True is returned because it was never called.
            self.assertEqual(rc, True)

    def test_generate_json_skeleton_no_input_shape(self):
        parsed_args = FakeArgs(generate_cli_skeleton=True)
        # Set the input shape to ``None``.
        self.argument = GenerateCliSkeletonArgument(mock.Mock(input_shape=None))
        with mock.patch('sys.stdout', six.StringIO()) as mock_stdout:
            rc = self.argument._generate_json_skeleton(
                parsed_args=parsed_args
            )
            # Ensure the contents printed to standard output are correct,
            # which should be an empty dictionary.
            self.assertEqual('{}\n', mock_stdout.getvalue())
            # Ensure False is returned because it was called.
            self.assertEqual(rc, False)

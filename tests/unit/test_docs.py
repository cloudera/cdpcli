# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2017 Cloudera, Inc. All rights reserved.
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

from cdpcli.doc.restdoc import ReSTDocument
from cdpcli.docs import OperationDocumentGenerator
from cdpcli.model import DenormalizedStructureBuilder
from cdpcli.model import ShapeResolver
from cdpcli.model import StringShape
import mock
from tests import unittest


class TestRecursiveShapes(unittest.TestCase):
    def setUp(self):
        self.arg_table = {}
        self.help_command = mock.Mock()
        self.help_command.arg_table = self.arg_table
        self.operation_model = mock.Mock()
        self.operation_model.service_model.operation_names = []
        self.help_command.obj = self.operation_model
        self.operation_generator = OperationDocumentGenerator(self.help_command)

    def assert_rendered_docs_contain(self, expected):
        writes = [args[0][0] for args in
                  self.help_command.doc.write.call_args_list]
        writes = '\n'.join(writes)
        self.assertIn(expected, writes)


class TestTranslationMap(unittest.TestCase):
    def setUp(self):
        self.arg_table = {}
        self.help_command = mock.Mock()
        self.help_command.arg_table = self.arg_table
        self.operation_model = mock.Mock()
        self.operation_model.service_model.operation_names = []
        self.help_command.obj = self.operation_model
        self.operation_generator = OperationDocumentGenerator(self.help_command)

    def assert_rendered_docs_contain(self, expected):
        writes = [args[0][0] for args in
                  self.help_command.doc.write.call_args_list]
        writes = '\n'.join(writes)
        self.assertIn(expected, writes)

    def test_boolean_arg_groups(self):
        builder = DenormalizedStructureBuilder()
        input_model = builder.with_members({
            'Flag': {'type': 'boolean'}
        }).build_model()
        argument_model = input_model.members['Flag']
        argument_model.name = 'Flag'
        self.arg_table['flag'] = mock.Mock(
            cli_type_name='boolean', argument_model=argument_model)
        self.arg_table['no-flag'] = mock.Mock(
            cli_type_name='boolean', argument_model=argument_model)
        # The --no-flag should not be used in the translation.
        self.assertEqual(
            self.operation_generator.build_translation_map(),
            {'Flag': 'flag'})


class TestCLIDocumentEventHandler(unittest.TestCase):
    def setUp(self):
        self.session = mock.Mock()
        self.obj = None
        self.command_table = {}
        self.arg_table = {}
        self.name = 'my-command'

    def create_help_command(self):
        help_command = mock.Mock()
        help_command.doc = ReSTDocument()
        help_command.arg_table = {}
        operation_model = mock.Mock()
        operation_model.documentation = 'description'
        operation_model.service_model.operation_names = []
        help_command.obj = operation_model
        return help_command

    def test_documents_enum_values(self):
        shape_map = {
            'EnumArg': {
                'type': 'string',
                'enum': ['FOO', 'BAZ']
            }
        }
        shape = StringShape('EnumArg',
                            shape_map['EnumArg'],
                            ShapeResolver(shape_map))
        arg_table = {'arg-name': mock.Mock(argument_model=shape,
                                           _UNDOCUMENTED=False)}
        help_command = mock.Mock()
        help_command.doc = ReSTDocument()
        help_command.arg_table = arg_table
        operation_model = mock.Mock()
        operation_model.service_model.operation_names = []
        help_command.obj = operation_model
        operation_generator = OperationDocumentGenerator(help_command)
        operation_generator.doc_option('arg-name', help_command)
        rendered = help_command.doc.getvalue().decode('utf-8')
        self.assertIn('Possible values', rendered)
        self.assertIn('FOO', rendered)
        self.assertIn('BAZ', rendered)

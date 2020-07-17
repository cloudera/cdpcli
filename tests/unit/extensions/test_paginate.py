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

from cdpcli.exceptions import PaginationError
from cdpcli.extensions import paginate
from cdpcli.help import OperationHelpCommand
from cdpcli.model import OperationModel
import mock
from mock import Mock
from tests import unittest


class TestPaginateBase(unittest.TestCase):

    def setUp(self):
        self.operation_model = mock.Mock()
        self.foo_param = mock.Mock()
        self.foo_param.name = 'Foo'
        self.foo_param.type_name = 'string'
        self.bar_param = mock.Mock()
        self.bar_param.type_name = 'string'
        self.bar_param.name = 'Bar'
        self.bartoo_param = mock.Mock()
        self.bartoo_param.type_name = 'string'
        self.bartoo_param.name = 'BarToo'
        self.params = [self.foo_param, self.bar_param, self.bartoo_param]
        self.operation_model.input_shape.members = {"Foo": self.foo_param,
                                                    "Bar": self.bar_param,
                                                    "BarToo": self.bartoo_param}
        self.operation_model.can_paginate = True
        self.operation_model.paging_input_token = "Foo"
        self.operation_model.paging_page_size = "Bar"
        self.operation_model.paging_default_max_items = 100

    def validate_page_param(self, param, is_shadowing=False):
        self.assertIsInstance(param, paginate.PageArgument)
        if is_shadowing:
            self.assertIsNotNone(param.shadowed_argument)
        else:
            self.assertIsNone(param.shadowed_argument)


class TestArgumentTableModifications(TestPaginateBase):

    def test_customize_arg_table(self):
        argument_table = {
            'foo': mock.Mock(),
            'bar': mock.Mock(),
            'bar-too': mock.Mock()
        }
        paginate.unify_paging_params(self.operation_model, argument_table)
        # We should mark the built in input_token as 'hidden'.
        self.assertTrue(argument_table['foo']._UNDOCUMENTED)
        # Also need to hide the page size.
        self.assertTrue(argument_table['bar']._UNDOCUMENTED)
        # Also need to hide the max size.
        self.assertTrue(argument_table['bar-too']._UNDOCUMENTED)
        # We also need to inject startin-token and max-items.
        self.assertIn('starting-token', argument_table)
        self.assertIn('max-items', argument_table)
        self.assertIn('page-size', argument_table)

        self.validate_page_param(argument_table['starting-token'])
        self.validate_page_param(argument_table['page-size'])
        self.validate_page_param(argument_table['max-items'])

    def test_operation_with_no_paginate(self):
        self.operation_model.can_paginate = False
        argument_table = {
            'foo': 'FakeArgObject',
            'bar': 'FakeArgObject',
        }
        starting_table = argument_table.copy()
        paginate.unify_paging_params(self.operation_model, argument_table)
        self.assertEqual(starting_table, argument_table)


class TestHelpDocumentationModifications(TestPaginateBase):
    def test_injects_pagination_help_text(self):
        help_command = OperationHelpCommand(
            Mock(), Mock(), 'foo', Mock())
        help_command.obj = Mock(OperationModel)
        help_command.obj.can_paginate = True
        help_command.obj.name = 'foo'
        paginate.add_paging_description(help_command)
        self.assertIn('``foo`` is a paginated operation. Multiple API',
                      help_command.doc.getvalue().decode())

    def test_does_not_inject_when_no_pagination(self):
        help_command = OperationHelpCommand(
            Mock(), Mock(), 'foo', Mock())
        help_command.obj = Mock(OperationModel)
        help_command.obj.can_paginate = False
        help_command.obj.name = 'foo'
        paginate.add_paging_description(help_command)
        self.assertNotIn('``foo`` is a paginated operation',
                         help_command.doc.getvalue().decode())


class TestShouldEnablePagination(TestPaginateBase):
    def setUp(self):
        super(TestShouldEnablePagination, self).setUp()
        self.parsed_globals = mock.Mock()
        self.parsed_args = mock.Mock()
        self.parsed_args.starting_token = None
        self.parsed_args.page_size = None
        self.parsed_args.max_items = None
        self.argument_table = {
            'foo': mock.Mock(),
            'bar': mock.Mock(),
            'bar-too': mock.Mock()
        }

    def test_should_not_enable_pagination(self):
        # Here the user has specified a manual pagination argument,
        # so we should turn pagination off.
        self.parsed_globals.paginate = True
        # Corresponds to --bar 10
        self.parsed_args.foo = None
        self.parsed_args.bar = 10
        self.parsed_args.bar_too = None
        paginate.check_should_enable_pagination(self.argument_table,
                                                self.operation_model,
                                                self.parsed_args,
                                                self.parsed_globals)
        # We should have turned paginate off because the
        # user specified --bar 10
        self.assertFalse(self.parsed_globals.paginate)

    def test_should_enable_pagination_with_no_args(self):
        self.parsed_globals.paginate = True
        # Corresponds to not specifying --foo nor --bar
        self.parsed_args.foo = None
        self.parsed_args.bar = None
        self.parsed_args.bar_too = None
        paginate.check_should_enable_pagination(self.argument_table,
                                                self.operation_model,
                                                self.parsed_args,
                                                self.parsed_globals)
        self.assertTrue(self.parsed_globals.paginate)

    def test_default_to_pagination_on_when_ambiguous(self):
        argument_table = {
            'foo': mock.Mock(),
            'max-times': mock.Mock()
        }
        self.parsed_globals.paginate = True
        # Here the user specifies --max-items 10 This is ambiguous because the
        # input_token also contains 'max-items'.  Should we assume they want
        # pagination turned off or should we assume that this is the normalized
        # --max-items?
        # Will we default to assuming they meant the normalized
        # --max-items.
        self.parsed_args.foo = None
        self.parsed_args.bar = None
        self.parsed_args.bar_too = None
        self.parsed_args.max_items = 10
        paginate.check_should_enable_pagination(argument_table,
                                                self.operation_model,
                                                self.parsed_args,
                                                self.parsed_globals)
        self.assertTrue(self.parsed_globals.paginate,
                        "Pagination was not enabled.")

    def test_shadowed_args_are_replaced_when_pagination_turned_off(self):
        operation_model = Mock(OperationModel)
        operation_model.can_paginate = True
        operation_model.paging_input_token = "StartingToken"
        operation_model.paging_page_size = "Bar"
        operation_model.paging_default_max_items = 100
        self.parsed_globals.paginate = True
        # Corresponds to --bar 10
        self.parsed_args.foo = None
        self.parsed_args.bar = 10
        self.parsed_args.bar_too = None

        argument_table = {
            'starting-token': mock.sentinel.FOO_ARG,
            'bar': mock.sentinel.BAR_ARG,
            'bar-too': mock.sentinel.BAR_TOO_ARG
        }
        paginate.unify_paging_params(operation_model, argument_table)
        self.validate_page_param(argument_table['starting-token'], True)
        paginate.check_should_enable_pagination(argument_table,
                                                operation_model,
                                                self.parsed_args,
                                                self.parsed_globals)

        # We should have turned paginate off because the
        # user specified --bar 10
        self.assertFalse(self.parsed_globals.paginate)
        self.assertEqual(argument_table['starting-token'], mock.sentinel.FOO_ARG)

    def test_shadowed_args_are_replaced_when_pagination_set_off(self):
        operation_model = Mock(OperationModel)
        operation_model.can_paginate = True
        operation_model.paging_input_token = "Foo"
        operation_model.paging_page_size = "page-size"
        operation_model.paging_default_max_items = 100
        self.parsed_globals.paginate = False
        # Corresponds to --foo 10
        self.parsed_args.foo = 10
        self.parsed_args.bar = None
        argument_table = {
            'foo': mock.sentinel.FOO_ARG,
            'page-size': mock.sentinel.BAR_ARG,
            'bar-too': mock.sentinel.BAR_TOO_ARG
        }
        paginate.unify_paging_params(operation_model, argument_table)
        self.validate_page_param(argument_table['page-size'], True)
        paginate.check_should_enable_pagination(argument_table,
                                                operation_model,
                                                self.parsed_args,
                                                self.parsed_globals)
        # We should have turned paginate off because the
        # user specified --bar 10
        self.assertFalse(self.parsed_globals.paginate)
        self.assertEqual(argument_table['page-size'], mock.sentinel.BAR_ARG)


class TestEnsurePagingParamsNotSet(TestPaginateBase):
    def setUp(self):
        super(TestEnsurePagingParamsNotSet, self).setUp()
        self.parsed_args = mock.Mock()

        self.parsed_args.starting_token = None
        self.parsed_args.page_size = None
        self.parsed_args.max_items = None

    def test_pagination_params_raise_error_with_no_paginate(self):
        self.parsed_args.max_items = 100

        with self.assertRaises(PaginationError):
            paginate.ensure_paging_params_not_set({}, self.parsed_args)

    def test_can_handle_missing_page_size(self):
        # Not all pagination operations have a page_size.
        del self.parsed_args.page_size
        self.assertIsNone(paginate.ensure_paging_params_not_set(
            {}, self.parsed_args))

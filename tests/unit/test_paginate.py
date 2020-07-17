# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

from types import MethodType

from cdpcli.exceptions import PaginationError
from cdpcli.model import ObjectShape
from cdpcli.model import OperationModel
from cdpcli.paginate import Paginator
from mock import call
from mock import Mock
from tests import unittest


class TestPagination(unittest.TestCase):

    def setUp(self):
        self.method = Mock(spec=MethodType)
        self.operation_model = Mock(spec=OperationModel)
        self.operation_model.paging_input_token = 'NextToken'
        self.operation_model.paging_output_token = 'NextToken'
        self.operation_model.paging_result = 'Foo'
        self.operation_model.paging_page_size = 'pageSize'
        self.operation_model.input_shape = Mock(spec=ObjectShape)
        self.operation_model.input_shape.members = {'pageSize': Mock()}
        self.operation_model.input_shape.members['pageSize'].maximum = 100
        self.operation_model.paging_default_max_items = 1000
        self.paginator = Paginator(self.method, self.operation_model)

    def test_no_next_token(self):
        response = {'not_the_next_token': 'foobar'}
        self.method.return_value = response
        actual = list(self.paginator.paginate())
        self.assertEqual(actual, [{'not_the_next_token': 'foobar'}])

    def test_next_token_in_response(self):
        responses = [{'NextToken': 'token1'},
                     {'NextToken': 'token2'},
                     {'not_next_token': 'foo'}]
        self.method.side_effect = responses
        actual = list(self.paginator.paginate())
        self.assertEqual(actual, responses)
        self.assertEqual(self.method.call_args_list, [call(pageSize=100),
                                                      call(NextToken='token1',
                                                           pageSize=100),
                                                      call(NextToken='token2',
                                                           pageSize=100)])

    def test_build_full_result(self):
        responses = [{'Foo': ['User1'], 'NextToken': 'm1'},
                     {'Foo': ['User2'], 'NextToken': 'm2'},
                     {'Foo': ['User3']}]
        self.method.side_effect = responses
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})

    def test_build_full_result_no_result(self):
        responses = [{'Foo': ['User1'], 'NextToken': 'm1'},
                     {'NextToken': 'm2'},
                     {'Foo': ['User2']}]
        self.method.side_effect = responses
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2']})

    def test_build_full_result_sum(self):
        responses = [{'Foo': 2.0, 'NextToken': 'm1'},
                     {'NextToken': 'm2'},
                     {'Foo': 4.0}]
        self.method.side_effect = responses
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': 6.0})

    def test_build_full_result_strings(self):
        responses = [{'Foo': 'foo', 'NextToken': 'm1'},
                     {'NextToken': 'm2'},
                     {'Foo': 'bar'}]
        self.method.side_effect = responses
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': 'foobar'})

    def test_exception_raised_if_same_next_token(self):
        responses = [{'NextToken': 'token1'},
                     {'NextToken': 'token2'},
                     {'NextToken': 'token2'}]
        self.method.side_effect = responses
        with self.assertRaises(PaginationError):
            list(self.paginator.paginate())

    def test_build_full_result_max_items(self):
        responses = [{'Foo': ['User1'], 'NextToken': 'm1'},
                     {'Foo': ['User2'], 'NextToken': 'm2'},
                     {'Foo': ['User3']}]
        self.operation_model.paging_default_max_items = 1
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1'], 'nextToken': 'm1'})
        self.method.assert_called_with(pageSize=1)

        self.operation_model.paging_default_max_items = 2
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2'],
                                  'nextToken': 'm2'})
        self.method.assert_any_call(pageSize=2)
        self.method.assert_called_with(pageSize=1, NextToken='m1')

        self.operation_model.paging_default_max_items = 3
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=3)
        self.method.assert_any_call(pageSize=2, NextToken='m1')
        self.method.assert_called_with(pageSize=1, NextToken='m2')

        self.operation_model.paging_default_max_items = 4
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=4)
        self.method.assert_any_call(pageSize=3, NextToken='m1')
        self.method.assert_called_with(pageSize=2, NextToken='m2')

        self.operation_model.paging_default_max_items = 101
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=100)
        self.method.assert_any_call(pageSize=100, NextToken='m1')
        self.method.assert_called_with(pageSize=99, NextToken='m2')

        self.operation_model.paging_default_max_items = 1000
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=100)
        self.method.assert_any_call(pageSize=100, NextToken='m1')
        self.method.assert_called_with(pageSize=100, NextToken='m2')

        # all items at once
        responses = [{'Foo': ['User1', 'User2', 'User3']}]
        self.operation_model.paging_default_max_items = 3
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate().build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_called_with(pageSize=3)

    def test_build_full_result_paging_params(self):
        responses = [{'Foo': ['User1'], 'NextToken': 'm1'},
                     {'Foo': ['User2'], 'NextToken': 'm2'},
                     {'Foo': ['User3']}]

        self.operation_model.paging_default_max_items = 500

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 1}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1'], 'nextToken': 'm1'})
        self.method.assert_called_with(pageSize=1)

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 2}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2'],
                                  'nextToken': 'm2'})
        self.method.assert_any_call(pageSize=2)
        self.method.assert_called_with(pageSize=1, NextToken='m1')

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 3}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=3)
        self.method.assert_any_call(pageSize=2, NextToken='m1')
        self.method.assert_called_with(pageSize=1, NextToken='m2')

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 4}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=4)
        self.method.assert_any_call(pageSize=3, NextToken='m1')
        self.method.assert_called_with(pageSize=2, NextToken='m2')

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 101,
                              'PageSize': 50}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=50)
        self.method.assert_any_call(pageSize=50, NextToken='m1')
        self.method.assert_called_with(pageSize=50, NextToken='m2')

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 1000,
                              'PageSize': 500}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=100)
        self.method.assert_any_call(pageSize=100, NextToken='m1')
        self.method.assert_called_with(pageSize=100, NextToken='m2')

        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        result = self.paginator.paginate(
            PaginationConfig={'MaxItems': 101,
                              'PageSize': 50,
                              'StartingToken': 'm0'}).build_full_result()
        self.assertEqual(result, {'Foo': ['User1', 'User2', 'User3']})
        self.method.assert_any_call(pageSize=50, NextToken='m0')
        self.method.assert_any_call(pageSize=50, NextToken='m1')
        self.method.assert_called_with(pageSize=50, NextToken='m2')

    # make sure we raise an error if service sends too many items
    def test_build_full_result_max_items_overshoot(self):
        # one page
        self.operation_model.paging_default_max_items = 2
        responses = [{'Foo': ['User1', 'User2', 'User3']}]
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        with self.assertRaises(PaginationError):
            self.paginator.paginate().build_full_result()
        self.method.assert_called_with(pageSize=2)
        # head
        self.operation_model.paging_default_max_items = 1
        responses = [{'Foo': ['User1', 'User2'], 'NextToken': 'm1'},
                     {'Foo': ['User3', 'User4'], 'NextToken': 'm2'},
                     {'Foo': ['User5', 'User6']}]
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        with self.assertRaises(PaginationError):
            self.paginator.paginate().build_full_result()
        self.method.assert_called_with(pageSize=1)
        # tail
        responses = [{'Foo': ['User1', 'User2'], 'NextToken': 'm1'},
                     {'Foo': ['User3', 'User4'], 'NextToken': 'm2'},
                     {'Foo': ['User5', 'User6']}]
        self.operation_model.paging_default_max_items = 5
        self.paginator = Paginator(self.method, self.operation_model)
        self.method.side_effect = responses
        self.method.reset_mock()
        with self.assertRaises(PaginationError):
            self.paginator.paginate().build_full_result()
        self.method.assert_called_with(pageSize=1, NextToken='m2')

    #  violate Paginator preconditions
    def test_paginator_preconditions(self):
        def feed_bad_values(values, fn):
            for value in values:
                with self.assertRaises(AssertionError):
                    fn(value)
        self.operation_model.paging_default_max_items = 1
        feed_bad_values(
            [None, 1, 'abc', {'abc': 1}, (1, 2), ['a', 'b']],
            lambda value: Paginator(value, self.operation_model))
        feed_bad_values(
            [None, 1, 'abc', {'abc': 1}, (1, 2), ['a', 'b'], feed_bad_values, eval],
            lambda value: Paginator(self.method, value))

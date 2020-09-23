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

from datetime import datetime
import decimal
import os
import sys

from cdpcli.model import ServiceModel
from cdpcli.model import ShapeResolver
from cdpcli.validate import ParamValidator
from tests import unittest
import yaml

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'validate')


class BaseTestValidate(unittest.TestCase):

    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')
        self.resolver = ShapeResolver(self.model['definitions'])

    def assert_has_validation_errors(self, shape, params, errors):
        errors_found = self.get_validation_error_message(shape, params)
        self.assertTrue(errors_found.has_errors())
        error_message = errors_found.generate_report()
        for error in errors:
            self.assertIn(error, error_message)

    def get_validation_error_message(self, shape, params):
        validator = ParamValidator()
        return validator.validate(params, shape)


class TestValidateRequiredParams(BaseTestValidate):

    def test_validate_required_params(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('director', 'Director'),
            {'name': 'foo'},
            ['Missing required parameter'])

    def test_validate_nested_required_param(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('director', 'Director'),
            {'name': 'foo',
             'crn': 'bar',
             'endpoint': {'address': 'baz'}},
            ['Missing required parameter'])

    def test_validate_unknown_param(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('director', 'Director'),
            {'name': 'foo',
             'crn': 'bar',
             'unknown': 'unknown'},
            ['Unknown parameter'])


class TestValidateTypes(BaseTestValidate):

    def test_validate_string(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'stringparam': 24,
             'intparam': 'notint',
             'boolparam': 'notbool',
             'numberparam': 'notnumber',
             'datetimeparam': 'notdatetime',
             'arrayparam': 'notarray',
             'objectparam': 'notobject',
             'mapparam': 'notmap',
             'blobparam': 123},
            ['Invalid type for parameter stringparam',
             'Invalid type for parameter intparam',
             'Invalid type for parameter boolparam',
             'Invalid type for parameter numberparam',
             'Invalid type for parameter datetimeparam',
             'Invalid type for parameter arrayparam',
             'Invalid type for parameter objectparam',
             'Invalid type for parameter mapparam',
             'Invalid type for parameter blobparam'])

    def test_datetime_type_accepts_datetime_object(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'datetimeparam': datetime.now()})
        self.assertEqual(errors.generate_report(), '')

    def test_datetime_accepts_string_timestamp(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'datetimeparam': '2014-01-01 12:00:00'})
        self.assertEqual(errors.generate_report(), '')

    def test_can_handle_none_datetimes(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'datetimeparam': None},
            ['Invalid type for parameter datetimeparam'])

    def test_valid_boolean(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'boolparam': True})
        self.assertEqual(errors.generate_report(), '')

    def test_valid_map(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'mapparam': {'key1': 'value1', 'key2': 'value2'}})
        self.assertEqual(errors.generate_report(), '')

    def test_invalid_key_map(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'mapparam': {'key1': 'value1', 20: 'value2'}},
            ["Invalid type for parameter mapparam (key: 20), value: 20"])

    def test_invalid_value_map(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'mapparam': {'key1': 'value1', 'key2': 2}},
            ["Invalid type for parameter mapparam.key2, value: 2"])

    def test_valid_blob_base64(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'blobparam': 'SGVsbG8gV29ybGQ='})
        self.assertEqual(errors.generate_report(), '')

    def test_valid_blob_empty_base64(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'blobparam': ''})
        self.assertEqual(errors.generate_report(), '')

    def test_invalid_value_blob_base64(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'blobparam': 'ABC'},
            ["Invalid base64 value for parameter blobparam, error: Incorrect padding"])

    if sys.version_info.major >= 3:  # bytes is introduced in python3
        def test_valid_blob_bytes(self):
            errors = self.get_validation_error_message(
                self.resolver.get_shape_by_name('typestest', 'TypesTest'),
                {'blobparam': b'BlobTest'})
            self.assertEqual(errors.generate_report(), '')

    if sys.version_info.major >= 3:  # bytes is introduced in python3
        def test_valid_blob_empty_bytes(self):
            errors = self.get_validation_error_message(
                self.resolver.get_shape_by_name('typestest', 'TypesTest'),
                {'blobparam': b''})
            self.assertEqual(errors.generate_report(), '')

    def test_valid_blob_bytearray(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'blobparam': bytearray(b'BlobTest')})
        self.assertEqual(errors.generate_report(), '')

    def test_valid_blob_empty_bytearray(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('typestest', 'TypesTest'),
            {'blobparam': bytearray(b'')})
        self.assertEqual(errors.generate_report(), '')


class TestValidateRanges(BaseTestValidate):

    def test_less_than_range(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'intparam': -10,
             'numberparam': -10.0},
            ['Invalid range for parameter intparam',
             'Invalid range for parameter numberparam'])

    def test_more_than_range(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'intparam': 100000,
             'numberparam': 100000.0},
            ['Invalid range for parameter intparam',
             'Invalid range for parameter numberparam'])

    def test_within_range(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'intparam': 50,
             'numberparam': 50.0})
        self.assertEqual(errors.generate_report(), '')

    def test_string_min_length_contraint(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'stringparam': ''},
            ['Invalid length for parameter stringparam'])

    def test_string_max_length_contraint(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'stringparam': 'thisistoolong'},
            ['Invalid length for parameter stringparam'])

    def test_list_min_length_constraint(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'arrayparam': []},
            ['Invalid length for parameter arrayparam'])

    def test_list_max_length_constraint(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'arrayparam': ['a', 'b', 'c', 'd', 'e', 'f']},
            ['Invalid length for parameter arrayparam'])

    def test_only_min_value_specified(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'minonlystringparam': ''},
            ['Invalid length for parameter minonlystringparam'])

    def test_only_max_value_specified(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'maxonlystringparam': 'thisistoolong'},
            ['Invalid length for parameter maxonlystringparam'])

    if sys.version_info.major >= 3:  # bytes is introduced in python3
        def test_blob_min_length_contraint_bytes(self):
            self.assert_has_validation_errors(
                self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
                {'blobparam': b''},
                ['Invalid length for parameter blobparam'])

    if sys.version_info.major >= 3:  # bytes is introduced in python3
        def test_blob_max_length_contraint_bytes(self):
            self.assert_has_validation_errors(
                self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
                {'blobparam': b'thisistoolong'},
                ['Invalid length for parameter blobparam'])

    def test_blob_min_length_contraint_bytearray(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'blobparam': bytearray(b'')},
            ['Invalid length for parameter blobparam'])

    def test_blob_max_length_contraint_bytearray(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'blobparam': bytearray(b'thisistoolong')},
            ['Invalid length for parameter blobparam'])

    def test_blob_min_length_contraint_base64(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'blobparam': ''},
            ['Invalid length for parameter blobparam'])

    def test_blob_max_length_contraint_base64(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('rangetest', 'RangeTest'),
            {'blobparam': 'dGhpc2lzdG9vbG9uZw=='},
            ['Invalid length for parameter blobparam'])


class TestValidationNumberType(BaseTestValidate):

    def test_range_number(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('numbertest', 'NumberTest'),
            {'numberparam': 1},
            ['Invalid range for parameter numberparam'])

    def test_decimal_allowed(self):
        errors = self.get_validation_error_message(
            self.resolver.get_shape_by_name('numbertest', 'NumberTest'),
            {'numberparam': decimal.Decimal('22.12345')})
        self.assertEqual(errors.generate_report(), '')

    def test_decimal_still_validates_range(self):
        self.assert_has_validation_errors(
            self.resolver.get_shape_by_name('numbertest', 'NumberTest'),
            {'numberparam': decimal.Decimal('1')},
            ['Invalid range for parameter numberparam'])

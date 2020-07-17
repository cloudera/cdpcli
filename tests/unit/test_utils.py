# Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2016 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import datetime

from cdpcli import OBJECT_TYPE
from cdpcli import xform_name
from cdpcli.model import DenormalizedStructureBuilder
from cdpcli.model import ServiceModel
from cdpcli.utils import ArgumentGenerator
from cdpcli.utils import CachedProperty
from cdpcli.utils import datetime2timestamp
from cdpcli.utils import get_service_module_name
from cdpcli.utils import instance_cache
from cdpcli.utils import parse_timestamp
from cdpcli.utils import parse_to_aware_datetime
from dateutil.tz import tzoffset, tzutc
from tests import unittest


class TestTransformName(unittest.TestCase):
    def test_upper_camel_case(self):
        self.assertEqual(xform_name('UpperCamelCase'), 'upper_camel_case')
        self.assertEqual(xform_name('UpperCamelCase', '-'), 'upper-camel-case')

    def test_lower_camel_case(self):
        self.assertEqual(xform_name('lowerCamelCase'), 'lower_camel_case')
        self.assertEqual(xform_name('lowerCamelCase', '-'), 'lower-camel-case')

    def test_consecutive_upper_case(self):
        self.assertEqual(xform_name('HTTPHeaders'), 'http_headers')
        self.assertEqual(xform_name('HTTPHeaders', '-'), 'http-headers')

    def test_consecutive_upper_case_middle_string(self):
        self.assertEqual(xform_name('MainHTTPHeaders'), 'main_http_headers')
        self.assertEqual(xform_name('MainHTTPHeaders', '-'), 'main-http-headers')

    def test_s3_prefix(self):
        self.assertEqual(xform_name('S3BucketName'), 's3_bucket_name')

    def test_already_snake_cased(self):
        self.assertEqual(xform_name('leave_alone'), 'leave_alone')
        self.assertEqual(xform_name('s3_bucket_name'), 's3_bucket_name')
        self.assertEqual(xform_name('bucket_s3_name'), 'bucket_s3_name')

    def test_special_case_ends_with_s(self):
        self.assertEqual(xform_name('GatewayCRNs', '-'), 'gateway-crns')


class TestParseTimestamps(unittest.TestCase):
    def test_parse_iso8601(self):
        self.assertEqual(
            parse_timestamp('1970-01-01T00:10:00.000Z'),
            datetime.datetime(1970, 1, 1, 0, 10, tzinfo=tzutc()))

    def test_parse_epoch(self):
        self.assertEqual(
            parse_timestamp(1222172800),
            datetime.datetime(2008, 9, 23, 12, 26, 40, tzinfo=tzutc()))

    def test_parse_epoch_zero_time(self):
        self.assertEqual(
            parse_timestamp(0),
            datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc()))

    def test_parse_epoch_as_string(self):
        self.assertEqual(
            parse_timestamp('1222172800'),
            datetime.datetime(2008, 9, 23, 12, 26, 40, tzinfo=tzutc()))

    def test_parse_rfc822(self):
        self.assertEqual(
            parse_timestamp('Wed, 02 Oct 2002 13:00:00 GMT'),
            datetime.datetime(2002, 10, 2, 13, 0, tzinfo=tzutc()))

    def test_parse_invalid_timestamp(self):
        with self.assertRaises(ValueError):
            parse_timestamp('invalid date')


class TestDatetime2Timestamp(unittest.TestCase):
    def test_datetime2timestamp_naive(self):
        self.assertEqual(
            datetime2timestamp(datetime.datetime(1970, 1, 2)), 86400)

    def test_datetime2timestamp_aware(self):
        tzinfo = tzoffset("BRST", -10800)
        self.assertEqual(
            datetime2timestamp(datetime.datetime(1970, 1, 2, tzinfo=tzinfo)),
            97200)


class TestParseToUTCDatetime(unittest.TestCase):
    def test_handles_utc_time(self):
        original = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(parse_to_aware_datetime(original), original)

    def test_handles_other_timezone(self):
        tzinfo = tzoffset("BRST", -10800)
        original = datetime.datetime(2014, 1, 1, 0, 0, 0, tzinfo=tzinfo)
        self.assertEqual(parse_to_aware_datetime(original), original)

    def test_handles_naive_datetime(self):
        original = datetime.datetime(1970, 1, 1, 0, 0, 0)
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(parse_to_aware_datetime(original), expected)

    def test_handles_string_epoch(self):
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(parse_to_aware_datetime('0'), expected)

    def test_handles_int_epoch(self):
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(parse_to_aware_datetime(0), expected)

    def test_handles_full_iso_8601(self):
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(
            parse_to_aware_datetime('1970-01-01T00:00:00Z'),
            expected)

    def test_year_only_iso_8601(self):
        expected = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=tzutc())
        self.assertEqual(parse_to_aware_datetime('1970-01-01'), expected)


class TestCachedProperty(unittest.TestCase):
    def test_cached_property_same_value(self):
        class CacheMe(object):
            @CachedProperty
            def foo(self):
                return 'foo'

        c = CacheMe()
        self.assertEqual(c.foo, 'foo')
        self.assertEqual(c.foo, 'foo')

    def test_cached_property_only_called_once(self):
        # Note: you would normally never want to cache
        # a property that returns a new value each time,
        # but this is done to demonstrate the caching behavior.

        class NoIncrement(object):
            def __init__(self):
                self.counter = 0

            @CachedProperty
            def current_value(self):
                self.counter += 1
                return self.counter

        c = NoIncrement()
        self.assertEqual(c.current_value, 1)
        # If the property wasn't cached, the next value should be
        # be 2, but because it's cached, we know the value will be 1.
        self.assertEqual(c.current_value, 1)


class TestArgumentGenerator(unittest.TestCase):
    def setUp(self):
        self.arg_generator = ArgumentGenerator()

    def assert_skeleton_from_model_is(self, model, generated_skeleton):
        shape = DenormalizedStructureBuilder().with_members(
            model).build_model()
        actual = self.arg_generator.generate_skeleton(shape)
        self.assertEqual(actual, generated_skeleton)

    def test_generate_string(self):
        self.assert_skeleton_from_model_is(
            model={
                'A': {'type': 'string'}
            },
            generated_skeleton={
                'A': ''
            }
        )

    def test_generate_scalars(self):
        self.assert_skeleton_from_model_is(
            model={
                'A': {'type': 'string'},
                'B': {'type': 'integer'},
                'C': {'type': 'float'},
                'D': {'type': 'double'},
                'E': {'type': 'boolean'},
                'F': {'type': 'datetime'}
            },
            generated_skeleton={
                'A': '',
                'B': 0,
                'C': 0.0,
                'D': 0.0,
                'E': True,
                'F': '2002-10-02T13:00:00+00:00'
            }
        )

    def test_generate_nested_objects(self):
        self.assert_skeleton_from_model_is(
            model={
                'A': {
                    'type': OBJECT_TYPE,
                    'properties': {
                        'B': {'type': 'string'},
                    }
                }
            },
            generated_skeleton={
                'A': {'B': ''}
            }
        )

    def test_generate_scalar_list(self):
        self.assert_skeleton_from_model_is(
            model={
                'A': {
                    'type': 'array',
                    'items': {
                        'type': 'string'
                    }
                },
            },
            generated_skeleton={
                'A': [''],
            }
        )

    def test_generate_scalar_map(self):
        self.assert_skeleton_from_model_is(
            model={
                'A': {
                    'type': 'object',
                    'additionalProperties': {
                        'type': 'string'
                    }
                }
            },
            generated_skeleton={
                'A': {
                    'KeyName': '',
                }
            }
        )


class TestInstanceCache(unittest.TestCase):
    class DummyClass(object):
        def __init__(self, cache):
            self._instance_cache = cache

        @instance_cache
        def add(self, x, y):
            return x + y

        @instance_cache
        def sub(self, x, y):
            return x - y

    def setUp(self):
        self.cache = {}

    def test_cache_single_method_call(self):
        adder = self.DummyClass(self.cache)
        self.assertEqual(adder.add(2, 1), 3)
        # This should result in one entry in the cache.
        self.assertEqual(len(self.cache), 1)
        # When we call the method with the same args,
        # we should reuse the same entry in the cache.
        self.assertEqual(adder.add(2, 1), 3)
        self.assertEqual(len(self.cache), 1)

    def test_can_cache_multiple_methods(self):
        adder = self.DummyClass(self.cache)
        adder.add(2, 1)

        # A different method results in a new cache entry,
        # so now there should be two elements in the cache.
        self.assertEqual(adder.sub(2, 1), 1)
        self.assertEqual(len(self.cache), 2)
        self.assertEqual(adder.sub(2, 1), 1)

    def test_can_cache_kwargs(self):
        adder = self.DummyClass(self.cache)
        adder.add(x=2, y=1)
        self.assertEqual(adder.add(x=2, y=1), 3)
        self.assertEqual(len(self.cache), 1)


class TestGetServiceModuleName(unittest.TestCase):
    def setUp(self):
        self.service_description = {
            'metadata': {
                'serviceFullName': 'CDP MyService',
                'apiVersion': '2014-01-01',
                'endpointPrefix': 'myservice',
                'signatureVersion': 'v4',
                'protocol': 'query'
            },
            'operations': {},
            'shapes': {},
        }
        self.service_model = ServiceModel(
            self.service_description, 'myservice')

    def test_default(self):
        self.assertEqual(
            get_service_module_name(self.service_model),
            'myservice'
        )

    def test_client_name_with_cloudera(self):
        self.service_description['metadata']['serviceFullName'] = (
            'Cloudera MyService')
        self.assertEqual(
            get_service_module_name(self.service_model),
            'myservice'
        )

    def test_client_name_with_non_alphabet_characters(self):
        self.service_description['metadata']['serviceFullName'] = (
            'Cloudera My-Service')
        self.assertEqual(
            get_service_module_name(self.service_model),
            'myservice'
        )

    def test_client_name_with_no_full_name_or_abbreviation(self):
        del self.service_description['metadata']['serviceFullName']
        self.assertEqual(
            get_service_module_name(self.service_model),
            'myservice'
        )


if __name__ == '__main__':
    unittest.main()

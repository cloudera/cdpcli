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

import datetime
import os
import sys

from cdpcli.compat import json
from cdpcli.compat import six
from cdpcli.exceptions import ParamValidationError
from cdpcli.model import ServiceModel
from cdpcli.parser import ResponseParserFactory
from cdpcli.serialize import create_serializer
from cdpcli.serialize import Serializer
import dateutil.parser
from tests import unittest
import yaml

UTF8 = 'utf-8'
PROTOCOL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'protocol')


class TestSerializer(unittest.TestCase):

    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(PROTOCOL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')
        self.serializer = create_serializer()
        self.non_validatring_serializer = Serializer()

    def _validate_request(self, request, url_path, params):
        self.assertEqual(len(request), 4)
        self.assertTrue('headers' in request)
        headers = request['headers']
        self.assertEqual(len(headers), 1)
        self.assertEqual(headers['Content-Type'], 'application/json')
        self.assertEqual(request['url_path'], url_path)
        self.assertEqual(request['body'], json.dumps(params).encode(UTF8))

    def test_no_params(self):
        params = {}
        operation_model = self.service_model.operation_model('createDirector')
        request = self.non_validatring_serializer.serialize_to_request(
            params, operation_model)
        self._validate_request(request, '/directors/createDirector', params)
        with self.assertRaises(ParamValidationError):
            self.serializer.serialize_to_request(params, operation_model)

    def test_param_with_none_value(self):
        params = {}
        params['name'] = None
        operation_model = self.service_model.operation_model('createDirector')
        request = self.non_validatring_serializer.serialize_to_request(
            params, operation_model)
        self._validate_request(request, '/directors/createDirector', {})
        with self.assertRaises(ParamValidationError):
            self.serializer.serialize_to_request(params, operation_model)

    def test_string_param(self):
        params = {}
        params['name'] = 'test-name'
        operation_model = self.service_model.operation_model('createDirector')
        request = self.serializer.serialize_to_request(params, operation_model)
        self._validate_request(request, '/directors/createDirector', params)

    def test_list_param(self):
        params = {}
        params['names'] = ['test-name-1', 'test-name-2']
        operation_model = self.service_model.operation_model('describeDirectors')
        request = self.serializer.serialize_to_request(params, operation_model)
        self._validate_request(request, '/directors/describeDirectors', params)

    def test_nested_objects(self):
        params = {}
        params['inner'] = {'name': 'test-name'}
        operation_model = self.service_model.operation_model('nestedObjects')
        request = self.serializer.serialize_to_request(params, operation_model)
        self._validate_request(request, '/directors/nestedObjects', params)

    def test_array_of_objects(self):
        params = {}
        params['directors'] = [{'name': 'test-name-1',
                                'crn': 'test-crn-1'},
                               {'name': 'test-name-2',
                                'crn': 'test-crn-1'}]
        operation_model = self.service_model.operation_model('arrayOfObjects')
        request = self.serializer.serialize_to_request(params, operation_model)
        self._validate_request(request, '/directors/arrayOfObjects', params)

    if sys.version_info.major >= 3:  # bytes is introduced in python3
        def test_blob_param_bytes(self):
            params = {}
            params['data'] = b'Hello World'
            operation_model = self.service_model.operation_model('blobObject')
            request = self.serializer.serialize_to_request(params, operation_model)
            params['data'] = 'SGVsbG8gV29ybGQ='
            self._validate_request(request, '/directors/blobObject', params)

    def test_blob_param_bytearray(self):
        params = {}
        params['data'] = bytearray(b'Hello World')
        operation_model = self.service_model.operation_model('blobObject')
        request = self.serializer.serialize_to_request(params, operation_model)
        params['data'] = 'SGVsbG8gV29ybGQ='
        self._validate_request(request, '/directors/blobObject', params)

    def test_blob_param_base64(self):
        params = {}
        params['data'] = 'SGVsbG8gV29ybGQ='
        operation_model = self.service_model.operation_model('blobObject')
        request = self.serializer.serialize_to_request(params, operation_model)
        self._validate_request(request, '/directors/blobObject', params)


class TestParser(unittest.TestCase):

    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(PROTOCOL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')
        self.parser = ResponseParserFactory().create_parser()

    def _create_response(self, body):
        response = {}
        response["status_code"] = 200
        response["headers"] = {}
        response["body"] = json.dumps(body).encode(UTF8)
        return response

    def _create_error_response(self, code, message):
        body = {}
        body['code'] = code
        body['message'] = message
        response = {}
        response["status_code"] = 301
        response["body"] = json.dumps(body).encode(UTF8)
        return response

    def _assert_same(self, v1, v2, path=""):
        if type(v1) is dict:
            for k in v1.keys():
                self.assertTrue(k in v2)
                self._assert_same(v1[k], v2[k])
            for k in v2.keys():
                self.assertTrue(k in v1)
                self._assert_same(v1[k], v2[k])
            return

        if type(v1) is datetime.datetime and isinstance(v2, six.text_type):
            v2 = dateutil.parser.parse(v2)
        if type(v2) is datetime.datetime and isinstance(v1, six.text_type):
            v1 = dateutil.parser.parse(v1)
        self.assertEquals(v1, v2)

    def _validate_parsed_response(self, response, parsed_response):
        self._assert_same(json.loads(response['body'].decode(UTF8)),
                          parsed_response)

    def _validate_error_response(self, response, parsed_response):
        self._assert_same(json.loads(response['body'].decode(UTF8)),
                          parsed_response['error'])

    def test_no_shape(self):
        director = {}
        director['name'] = 'test-name'
        director['crn'] = 'test-crn'
        director['endpoint'] = {'address': '1.2.3.4', 'port': 12345}
        response = self._create_response({'director': director})
        parsed_response = self.parser.parse(response, None)
        self.assertEqual({}, parsed_response)

    def test_no_response(self):
        response = {}
        response["status_code"] = 200
        response["headers"] = {}
        response["body"] = ""
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self.assertEqual({}, parsed_response)
        response["body"] = None
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self.assertEqual({}, parsed_response)
        response["body"] = {}
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self.assertEqual({}, parsed_response)

    def test_non_json_response(self):
        response = {}
        response["status_code"] = 404
        response["headers"] = {}
        response["body"] = "Page not found".encode(UTF8)
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self.assertEqual(parsed_response,
                         {'error': {'code': 'UNKNOWN_ERROR',
                                    'message': 'Page not found'}})

    def test_response(self):
        director = {}
        director['name'] = 'test-name'
        director['crn'] = 'test-crn'
        director['endpoint'] = {'address': '1.2.3.4', 'port': 12345}
        director['creationDate'] = "2014-07-31T12:26:00.000-07:00"
        director['booleanData'] = True
        director['numberData'] = "123.456"
        response = self._create_response({'director': director})
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self._validate_parsed_response(response, parsed_response)

    def test_invalid_datetime(self):
        director = {}
        director['creationDate'] = "BAD!-2014-07-31T12:26:00.000-07:00-BAD!"
        response = self._create_response({'director': director})
        operation_model = self.service_model.operation_model('createDirector')
        with self.assertRaises(ValueError):
            self.parser.parse(response, operation_model.output_shape)

    def test_response_with_array(self):
        director1 = {}
        director1['name'] = 'test-name-1'
        director1['crn'] = 'test-crn-1'
        director1['endpoint'] = {'address': '1.2.3.4', 'port': 12345}
        director2 = {}
        director2['name'] = 'test-name-2'
        director2['crn'] = 'test-crn-2'
        director2['endpoint'] = {'address': '2.3.4.5', 'port': 23456}
        response = self._create_response({'directors': [director1, director2]})
        operation_model = self.service_model.operation_model('describeDirectors')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self._validate_parsed_response(response, parsed_response)

    def test_response_with_missing_member(self):
        director = {}
        director['crn'] = 'test-crn'
        director['endpoint'] = {'address': '1.2.3.4', 'port': 12345}
        response = self._create_response({'director': director})
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self._validate_parsed_response(response, parsed_response)

    def test_response_with_none_member(self):
        director = {}
        director['name'] = None
        director['crn'] = 'test-crn'
        director['endpoint'] = {'address': '1.2.3.4', 'port': 12345}
        response = self._create_response({'director': director})
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        del director['name']
        response = self._create_response({'director': director})
        self._validate_parsed_response(response, parsed_response)

    def test_error(self):
        response = self._create_error_response(
            "YouMessedUpException",
            "You, sir, have messed up.")
        operation_model = self.service_model.operation_model('createDirector')
        parsed_response = self.parser.parse(response, operation_model.output_shape)
        self._validate_error_response(response, parsed_response)

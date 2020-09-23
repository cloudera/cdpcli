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

import os

from cdpcli.compat import six
from cdpcli.model import ServiceModel
from cdpcli.model import ShapeResolver
from cdpcli.paramfile import get_paramfile
from cdpcli.paramfile import ParamFileVisitor
from cdpcli.paramfile import ResourceLoadingError
import mock
from tests import FileCreator, unittest
from tests import skip_if_windows
import yaml

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'paramfile')


class TestParamFile(unittest.TestCase):
    def setUp(self):
        self.files = FileCreator()

    def tearDown(self):
        self.files.remove_all()

    def test_text_file(self):
        contents = 'This is a test'
        filename = self.files.create_file('foo', contents)
        prefixed_filename = 'file://' + filename
        data = get_paramfile(prefixed_filename)
        self.assertEqual(data, contents)
        self.assertIsInstance(data, six.string_types)

    def test_binary_file(self):
        contents = 'This is a test'
        filename = self.files.create_file('foo', contents)
        prefixed_filename = 'fileb://' + filename
        data = get_paramfile(prefixed_filename)
        self.assertEqual(data, b'This is a test')
        self.assertIsInstance(data, six.binary_type)

    @skip_if_windows('Binary content error only occurs '
                     'on non-Windows platforms.')
    def test_cannot_load_text_file(self):
        contents = b'\xbfX\xac\xbe'
        filename = self.files.create_file('foo', contents, mode='wb')
        prefixed_filename = 'file://' + filename
        with self.assertRaises(ResourceLoadingError):
            get_paramfile(prefixed_filename)

    def test_file_does_not_exist_raises_error(self):
        with self.assertRaises(ResourceLoadingError):
            get_paramfile('file://file/does/not/existsasdf.txt')

    def test_no_match_uris_returns_none(self):
        self.assertIsNone(get_paramfile('foobar://somewhere.bar'))

    def test_non_string_type_returns_none(self):
        self.assertIsNone(get_paramfile(100))


class TestHTTPBasedResourceLoading(unittest.TestCase):
    def setUp(self):
        self.requests_patch = mock.patch('cdpcli.paramfile.requests')
        self.requests_mock = self.requests_patch.start()
        self.response = mock.Mock(status_code=200)
        self.requests_mock.get.return_value = self.response

    def tearDown(self):
        self.requests_patch.stop()

    def test_resource_from_http(self):
        self.response.text = 'http contents'
        loaded = get_paramfile('http://foo.bar.baz')
        self.assertEqual(loaded, 'http contents')
        self.requests_mock.get.assert_called_with('http://foo.bar.baz')

    def test_resource_from_https(self):
        self.response.text = 'http contents'
        loaded = get_paramfile('https://foo.bar.baz')
        self.assertEqual(loaded, 'http contents')
        self.requests_mock.get.assert_called_with('https://foo.bar.baz')

    def test_non_200_raises_error(self):
        self.response.status_code = 500
        with self.assertRaisesRegexp(ResourceLoadingError, 'foo\\.bar\\.baz'):
            get_paramfile('https://foo.bar.baz')

    def test_connection_error_raises_error(self):
        self.requests_mock.get.side_effect = Exception("Connection error.")
        with self.assertRaisesRegexp(ResourceLoadingError, 'foo\\.bar\\.baz'):
            get_paramfile('https://foo.bar.baz')


class TestParamFileVisitor(unittest.TestCase):
    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')
        self.resolver = ShapeResolver(self.model['definitions'])
        self.files = FileCreator()

    def tearDown(self):
        self.files.remove_all()

    def test_visitor(self):
        contents = 'This is a test'
        filename = self.files.create_file('jobOne.hql', contents)
        # We have modified our test model to mark jobXml with x-no-paramfile.
        params = {'clusterName': u'foo',
                  'jobs': [{'hiveJob': {'script': 'file://' + filename,
                                        'jobXml': 'file://' + filename}}]}
        shape = self.resolver.get_shape_by_name(
            'submit-jobs-request', 'SubmitJobsRequest')
        visited = ParamFileVisitor().visit(params, shape)
        params['jobs'][0]['hiveJob']['script'] = contents
        self.assertEqual(params, visited)

    def test_ref_map_visitor(self):
        contents = 'This is a test'
        filename = self.files.create_file('jobOne.hql', contents)
        # We have modified our test model to mark jobXml with x-no-paramfile.
        params = {'jobs': {'job1': {'hiveJob': {'script': 'file://' + filename,
                                    'jobXml': 'file://' + filename}}}}
        shape = self.resolver.get_shape_by_name(
            'map-paramfile-test', 'RefMapParamFileTest')
        visited = ParamFileVisitor().visit(params, shape)
        params['jobs']['job1']['hiveJob']['script'] = contents
        self.assertEqual(params, visited)

    def test_explicit_map_visitor(self):
        contents = 'This is a test'
        filename = self.files.create_file('jobOne.hql', contents)
        # We have modified our test model to mark jobXml with x-no-paramfile.
        params = {'jobs': {'job1': {'script': 'file://' + filename,
                                    'jobXml': 'file://' + filename}}}
        shape = self.resolver.get_shape_by_name(
            'map-paramfile-test', 'ExplicitMapParamFileTest')
        visited = ParamFileVisitor().visit(params, shape)
        params['jobs']['job1']['script'] = contents
        self.assertEqual(params, visited)

    def test_blob_visitor(self):
        contents = b'This is a test'
        filename = self.files.create_file('jobOne.hql', contents, mode='wb')
        # We have modified our test model to mark jobXml with x-no-paramfile.
        params = {'jobs': {'job1': {'script': 'fileb://' + filename,
                                    'jobXml': 'fileb://' + filename}}}
        shape = self.resolver.get_shape_by_name(
            'blob-test', 'BlobParamFileTest')
        visited = ParamFileVisitor().visit(params, shape)
        params['jobs']['job1']['script'] = contents
        self.assertEqual(params, visited)

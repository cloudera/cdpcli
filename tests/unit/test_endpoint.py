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

import os

from cdpcli.compat import json
from cdpcli.compat import OrderedDict
from cdpcli.endpoint import DEFAULT_TIMEOUT, Endpoint
from cdpcli.endpoint import EndpointCreator
from cdpcli.endpoint import EndpointResolver
from cdpcli.exceptions import EndpointConnectionError
from cdpcli.retryhandler import create_retry_handler
from cdpcli.thirdparty.requests import ConnectionError
from cdpcli.translate import build_retry_config
from mock import Mock, patch
from tests import unittest

CLIENT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'client')


def request_dict():
    return {
        'headers': {},
        'body': '',
        'url_path': '/',
        'query_string': '',
        'method': 'POST',
        'url': 'https://example.com'
    }


class TestEndpointBase(unittest.TestCase):

    def setUp(self):
        self.op = Mock()
        self.op.metadata = {'protocol': 'json'}
        self.request_signer = Mock()
        self.factory_patch = patch(
            'cdpcli.parser.ResponseParserFactory')
        self.factory = self.factory_patch.start()
        self.retry_handler = Mock(wraps=self._create_retry_handler())
        self.endpoint = Endpoint('https://iamapi.us-west-1.cdp.cloudera.com/',
                                 retry_handler=self.retry_handler)
        self.http_session = Mock()
        self.http_session.send.return_value = Mock(
            status_code=200, headers={}, content=b'{"Foo": "bar"}',
        )
        self.endpoint.http_session = self.http_session

    def tearDown(self):
        self.factory_patch.stop()

    def _create_retry_handler(self):
        original_config = open(os.path.join(CLIENT_DIR, '_retry.json')).read()
        original_config = json.loads(original_config,
                                     object_pairs_hook=OrderedDict)
        config = build_retry_config(
            original_config['retry'],
            original_config.get('definitions', {}))
        return create_retry_handler(config)


class TestEndpointFeatures(TestEndpointBase):

    def test_timeout_can_be_specified(self):
        timeout_override = 120
        self.endpoint.timeout = timeout_override
        self.endpoint.make_request(self.op, request_dict(), self.request_signer)
        kwargs = self.http_session.send.call_args[1]
        self.assertEqual(kwargs['timeout'], timeout_override)

    def test_make_request_with_proxies(self):
        proxies = {'http': 'http://localhost:8888'}
        self.endpoint.proxies = proxies
        self.endpoint.make_request(self.op, request_dict(), self.request_signer)
        prepared_request = self.http_session.send.call_args[0][0]
        self.http_session.send.assert_called_with(
            prepared_request, verify=True, stream=False,
            proxies=proxies, timeout=DEFAULT_TIMEOUT)

    def test_make_request_with_no_auth(self):
        self.endpoint.auth = None
        self.endpoint.make_request(self.op, request_dict(), self.request_signer)

        # http_session should be used to send the request.
        self.assertTrue(self.http_session.send.called)
        prepared_request = self.http_session.send.call_args[0][0]
        self.assertNotIn('Authorization', prepared_request.headers)

    def test_make_request_no_signature_version(self):
        self.endpoint.make_request(self.op, request_dict(), self.request_signer)

        # http_session should be used to send the request.
        self.assertTrue(self.http_session.send.called)
        prepared_request = self.http_session.send.call_args[0][0]
        self.assertNotIn('Authorization', prepared_request.headers)

    def test_make_request_injects_better_dns_error_msg(self):
        fake_request = Mock(url='https://iamapi.us-west-1.altus.cloudera.com')
        self.http_session.send.side_effect = ConnectionError(
            "Fake gaierror(8, node or host not known)", request=fake_request)
        with self.assertRaisesRegexp(EndpointConnectionError,
                                     'Could not connect'):
            self.endpoint.make_request(self.op, request_dict(),
                                       self.request_signer)

    def test_make_request_injects_better_bad_status_line_error_msg(self):
        fake_request = Mock(url='https://iamapi.us-west-1.altus.cloudera.com')
        self.http_session.send.side_effect = ConnectionError(
            """'Connection aborted.', BadStatusLine("''",)""",
            request=fake_request)
        with self.assertRaisesRegexp(ConnectionError,
                                     'Connection aborted.'):
            self.endpoint.make_request(self.op, request_dict(),
                                       self.request_signer)


class TestRetryInterface(TestEndpointBase):
    def setUp(self):
        super(TestRetryInterface, self).setUp()
        self.retried_on_exception = None
        self.total_calls = 0

    def test_retry_is_attempted(self):
        op = Mock()
        op.name = 'DescribeCluster'
        op.metadata = {'protocol': 'query'}
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        call_args = self.endpoint._retry_handler.call_args
        self.assertEqual(call_args[1]['attempts'], 1)

    def test_retry_events_can_alter_behavior(self):
        op = Mock()
        op.name = 'DescribeCluster'
        op.metadata = {'protocol': 'json'}
        self.retry_handler.side_effect = [
            0,       # Check if retry needed. Retry needed.
            None     # Check if retry needed. Retry not needed.
        ]
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 2)

    def test_retry_on_socket_errors(self):
        op = Mock()
        op.name = 'DescribeClusters'
        self.retry_handler.side_effect = [
            0,       # Check if retry needed. Retry needed.
            None     # Check if retry needed. Retry not needed.
        ]
        self.http_session.send.side_effect = ConnectionError()
        with self.assertRaises(ConnectionError):
            self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 2)

    def test_retry_on_429(self):
        http_429_response = Mock()
        http_429_response.status_code = 429
        http_429_response.headers = []
        http_429_response.content = b'{}'
        op = Mock()
        op.name = 'DescribeClusters'
        self.http_session.send.return_value = http_429_response
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 3)

    def test_retry_on_503(self):
        http_503_response = Mock()
        http_503_response.status_code = 503
        http_503_response.headers = []
        http_503_response.content = b'{}'
        op = Mock()
        op.name = 'DescribeClusters'
        self.http_session.send.return_value = http_503_response
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 3)

    def test_retry_on_509(self):
        http_509_response = Mock()
        http_509_response.status_code = 509
        http_509_response.headers = []
        http_509_response.content = b'{}'
        op = Mock()
        op.name = 'DescribeClusters'
        self.http_session.send.return_value = http_509_response
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 3)

    def test_no_retry_on_success(self):
        http_200_response = Mock()
        http_200_response.status_code = 200
        http_200_response.headers = []
        http_200_response.content = b'{}'
        op = Mock()
        op.name = 'DescribeClusters'
        self.http_session.send.return_value = http_200_response
        self.endpoint.make_request(op, request_dict(), self.request_signer)
        self.assertEqual(self.retry_handler.call_count, 1)


class TestEndpointCreator(unittest.TestCase):
    def setUp(self):
        self.service_model = Mock(
            endpoint_name='iam',
            signature_version='v1',
            signing_name='iam')
        self.cdp_service_model = Mock(
            endpoint_name='datahub',
            endpoint_prefix='api',
            signature_version='v1',
            signing_name='datahub',
            products=['CDP'])
        self.environ = {}
        self.environ_patch = patch('os.environ', self.environ)
        self.environ_patch.start()
        self.creator = EndpointCreator(EndpointResolver())
        self.factory_patch = patch(
            'cdpcli.parser.ResponseParserFactory')
        self.factory = self.factory_patch.start()
        self.retry_handler = Mock()

    def tearDown(self):
        self.environ_patch.stop()

    def test_altus_endpoint(self):
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=None,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, 'https://iamapi.us-west-1.altus.cloudera.com:443')

    def test_cdp_endpoint(self):
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, 'https://api.us-west-1.cdp.cloudera.com:443')

    def test_cdp_endpoint_with_prefix(self):
        self.cdp_service_model.endpoint_prefix = 'prefix'
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host,
                         'https://prefix.us-west-1.cdp.cloudera.com:443')

    def test_creates_endpoint_with_configured_url(self):
        endpoint_url = 'https://endpoint.url'
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=endpoint_url,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_customized_timeout(self):
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url='https://example.com',
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.timeout, 123)

    def test_create_endpoint_with_config_url_altus(self):
        endpoint_url = 'https://endpoint-config.url'
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=None,
            scoped_config={"endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_config_url_cdp(self):
        endpoint_url = 'https://endpoint-config.url'
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={"cdp_endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_configured_url_substitution(self):
        endpoint_url = 'https://%s.endpoint.url'
        expected = endpoint_url % self.service_model.endpoint_name
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=endpoint_url,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, expected)

    def test_create_endpoint_with_configured_cdp_url_substitution(self):
        endpoint_url = 'https://%s.endpoint.url'
        self.cdp_service_model.endpoint_prefix = 'prefix'
        expected = endpoint_url % self.cdp_service_model.endpoint_prefix
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=endpoint_url,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, expected)

    def test_create_endpoint_with_config_url_substitution(self):
        endpoint_url = 'https://%s.endpoint-config.url'
        expected = endpoint_url % self.service_model.endpoint_name
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=None,
            scoped_config={"endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, expected)

    def test_create_endpoint_with_config_cdp_url_substitution(self):
        endpoint_url = 'https://%s.endpoint-config.url'
        self.cdp_service_model.endpoint_prefix = 'prefix'
        expected = endpoint_url % self.cdp_service_model.endpoint_prefix
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={"cdp_endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, expected)

    def test_create_endpoint_with_configured_url_bad_substitution(self):
        endpoint_url = 'https://%s.%s.endpoint.url'
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=endpoint_url,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_configured_cdp_url_bad_substitution(self):
        endpoint_url = 'https://%s.%s.endpoint.url'
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=endpoint_url,
            scoped_config={},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_config_url_bad_substitution(self):
        endpoint_url = 'https://%s.%s.endpoint-config.url'
        endpoint = self.creator.create_endpoint(
            self.service_model,
            endpoint_url=None,
            scoped_config={"endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_with_config_cdp_url_bad_substitution(self):
        endpoint_url = 'https://%s.%s.endpoint-config.url'
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={"cdp_endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_endpoint_for_internal_apis(self):
        endpoint_url = 'https://foo-api.internal.url'
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={"cdp_endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, endpoint_url)

    def test_create_wildcard_endpoint_for_internal_apis(self):
        endpoint_url = 'https://%s-api.internal.url'
        self.cdp_service_model.endpoint_prefix = 'prefix'
        expected = endpoint_url % self.cdp_service_model.endpoint_prefix
        endpoint = self.creator.create_endpoint(
            self.cdp_service_model,
            endpoint_url=None,
            scoped_config={"cdp_endpoint_url": endpoint_url,
                           "config1": "value1"},
            timeout=123,
            response_parser_factory=self.factory_patch,
            tls_verification=False,
            retry_handler=self.retry_handler)
        self.assertEqual(endpoint.host, expected)

# Copyright (c) 2021 Cloudera, Inc. All rights reserved.
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

from cdpcli.exceptions import RedirectExtensionError
from cdpcli.extensions.redirect import Redirect
from mock import Mock
from tests import unittest


class TestRedirectExtension(unittest.TestCase):

    def setUp(self):
        self.client = Mock()
        self.client.raise_error.side_effect = Exception('ClientError')
        self.client_creator = Mock(return_value=self.client)
        self.operation_model = Mock()
        self.operation_model.service_model = Mock(service_name='service-name')
        self.operation_model.name = 'operation'
        self.operation_model.http = {'method': 'post', 'requestUri': None}
        self.parsed_globals = Mock(output=None)

    def test_redirect(self):
        parsed_globals = Mock(output=None)
        redirect_http = Mock(status_code=308, is_redirect=True)
        redirect_http.headers = {'Location': 'https://test.com/path/uploadFlow'}
        self.client.make_api_call.return_value = (redirect_http, None)
        self.client.make_request.return_value = (Mock(status_code=200), {})

        parameters = {'name': 'foo'}
        redirect = Redirect()
        redirect.invoke(self.client_creator, self.operation_model,
                        parameters, None, parsed_globals)

        self.assertEqual(1, self.client.make_api_call.call_count)
        args, kwargs = self.client.make_api_call.call_args
        self.assertEqual('operation', args[0])
        self.assertEqual(parameters, args[1])
        self.assertFalse(kwargs['allow_redirects'])

        self.assertEquals('https://test.com/path/uploadFlow',
                          parsed_globals.endpoint_url)
        self.assertEquals('https://test.com/path/uploadFlow',
                          self.operation_model.http['requestUri'])

    def test_no_redirect(self):
        redirect_http = Mock(status_code=200, is_redirect=False)
        redirect_http.headers = {'Location': 'https://test.com/path/uploadFlow'}
        self.client.make_api_call.return_value = (redirect_http, None)

        parameters = {'name': 'foo'}
        redirect = Redirect()
        with self.assertRaisesRegex(RedirectExtensionError,
                                    'The redirect extension failed: '
                                    'Missing or incorrect redirect URL.'):
            redirect.invoke(self.client_creator, self.operation_model,
                            parameters, None, self.parsed_globals)
        self.assertEqual(1, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)
        self.assertIsNone(self.operation_model.http['requestUri'])

    def test_no_redirect_url(self):
        redirect_http = Mock(status_code=308, is_redirect=True)
        redirect_http.headers = {}
        self.client.make_api_call.return_value = (redirect_http, None)

        parameters = {'name': 'foo'}
        redirect = Redirect()
        with self.assertRaisesRegex(RedirectExtensionError,
                                    'The redirect extension failed: '
                                    'Missing or incorrect redirect URL.'):
            redirect.invoke(self.client_creator, self.operation_model,
                            parameters, None, self.parsed_globals)
        self.assertEqual(1, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)
        self.assertIsNone(self.operation_model.http['requestUri'])

    def test_incorrect_redirect_url(self):
        redirect_http = Mock(status_code=308, is_redirect=True)
        redirect_http.headers = {'Location': '/relative-path/flows'}
        self.client.make_api_call.return_value = (redirect_http, None)

        parameters = {'name': 'foo'}
        redirect = Redirect()
        with self.assertRaisesRegex(RedirectExtensionError,
                                    'The redirect extension failed: '
                                    'Missing or incorrect redirect URL.'):
            redirect.invoke(self.client_creator, self.operation_model,
                            parameters, None, self.parsed_globals)
        self.assertEqual(1, self.client.make_api_call.call_count)
        self.assertEqual(0, self.client.make_request.call_count)
        self.assertIsNone(self.operation_model.http['requestUri'])

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

import os
import re
import threading
import time
import urllib.parse
import urllib.request

from cdpcli import CDP_ACCESS_KEY_ID_KEY_NAME, CDP_PRIVATE_KEY_KEY_NAME
from cdpcli.exceptions import InteractiveLoginError
from cdpcli.extensions.configure import CREDENTIAL_FILE_COMMENT
from cdpcli.extensions.interactivelogin import LoginCommand
import mock
from tests import unittest
from tests.unit import FakeContext


class TestLoginCommand(unittest.TestCase):

    def setUp(self):
        self.writer = mock.Mock()
        self.open_new_browser = mock.Mock()
        self.context = FakeContext({'config_file': 'myconfigfile'})
        self.login = LoginCommand(config_writer=self.writer,
                                  open_new_browser=self.open_new_browser)

    def assert_login_url(self,
                         login_url,
                         expected_hostname,
                         expected_path=None,
                         expected_account_id=None,
                         expected_idp=None,
                         expected_return_url_port=None,
                         expected_extra_query=None):
        url_parsed = urllib.parse.urlsplit(login_url)
        url_params = urllib.parse.parse_qs(url_parsed.query)
        self.assertEqual(url_parsed.scheme, 'https')
        self.assertEqual(url_parsed.hostname, expected_hostname)
        self.assertEqual(url_parsed.path, expected_path)
        num_expected_queries = 2
        self.assertEqual(url_params.get('accountId'), [expected_account_id])
        if expected_idp is not None:
            num_expected_queries += 1
            self.assertEqual(url_params.get('idp'), [expected_idp])
        if expected_return_url_port is None:
            self.assertEqual(len(url_params.get('returnUrl')), 1)
            self.assertRegex(
                url_params.get('returnUrl')[0],
                'http://localhost:\\d+/interactiveLogin')
        else:
            self.assertEqual(len(url_params.get('returnUrl')), 1)
            self.assertEqual(
                url_params.get('returnUrl')[0],
                'http://localhost:%d/interactiveLogin' % expected_return_url_port)
        if expected_extra_query is not None:
            num_expected_queries += 1
            self.assertEqual(
                url_params.get(expected_extra_query.get('key'))[0],
                expected_extra_query.get('value'))
        self.assertEqual(len(url_params), num_expected_queries)

    def assert_credentials_file_updated_with(self,
                                             new_values,
                                             config_file_comment=None):
        called_args = self.writer.update_config.call_args_list
        credentials_file_call = called_args[0]
        expected_creds_file = os.path.expanduser('~/fake_credentials_filename')
        self.assertEqual(credentials_file_call,
                         mock.call(new_values,
                                   expected_creds_file,
                                   config_file_comment=config_file_comment))

    def mock_user_login(self, login_params, result, assert_func=None):
        i = 0
        while not self.open_new_browser.called:
            i += 1
            self.assertFalse(i > 20)
            time.sleep(0.1)
        url = self.open_new_browser.call_args[0][0]
        if assert_func is not None:
            assert_func(url)
        m = re.search('http%3A%2F%2Flocalhost%3A\\d+%2FinteractiveLogin', url)
        self.assertIsNotNone(m)
        return_url = urllib.parse.unquote(m.group())
        resp = urllib.request.urlopen(return_url + login_params)
        self.assertEqual(resp.getcode(), 200)
        result['succeeded'] = True

    def test_login_command_open_browser(self):
        try:
            args = ['--account-id', 'test-account-guid', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
        except InteractiveLoginError:
            pass
        self.assertEqual(self.open_new_browser.call_count, 1)
        args, kwargs = self.open_new_browser.call_args
        self.assertEqual(len(args), 1)
        self.assert_login_url(login_url=args[0],
                              expected_hostname='consoleauth.altus.cloudera.com',
                              expected_path='/login',
                              expected_account_id='test-account-guid')

    def test_login_command_succeeded(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?accessKeyId=foo&privateKey=bar', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
            self.assert_credentials_file_updated_with(
                {CDP_ACCESS_KEY_ID_KEY_NAME: 'foo',
                 CDP_PRIVATE_KEY_KEY_NAME: 'bar'},
                config_file_comment=CREDENTIAL_FILE_COMMENT)
        finally:
            thread.join()
        self.assertTrue(login_result.get('succeeded'))

    def test_login_command_succeeded_for_profile(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?accessKeyId=foo&privateKey=bar', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            parsed_globals.profile = 'myname'
            self.context.effective_profile = 'myname'
            self.login(self.context, args=args, parsed_globals=parsed_globals)
            self.assert_credentials_file_updated_with(
                {CDP_ACCESS_KEY_ID_KEY_NAME: 'foo',
                 CDP_PRIVATE_KEY_KEY_NAME: 'bar',
                 '__section__': 'myname'},
                config_file_comment=CREDENTIAL_FILE_COMMENT)
        finally:
            thread.join()
        self.assertTrue(login_result.get('succeeded'))

    def test_login_command_succeeded_for_profile_does_not_exist(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?accessKeyId=foo&privateKey=bar', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.context.profile_does_not_exist = True
            self.login(self.context, args=args, parsed_globals=parsed_globals)
            self.assert_credentials_file_updated_with(
                {CDP_ACCESS_KEY_ID_KEY_NAME: 'foo',
                 CDP_PRIVATE_KEY_KEY_NAME: 'bar'},
                config_file_comment=CREDENTIAL_FILE_COMMENT)
        finally:
            thread.join()
        self.assertTrue(login_result.get('succeeded'))

    def test_login_command_succeeded_for_assigned_port(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?accessKeyId=foo&privateKey=bar', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--port', '10100', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
            args, kwargs = self.open_new_browser.call_args
            self.assertEqual(len(args), 1)
            self.assertTrue('10100' in args[0])
        finally:
            thread.join()
        self.assertTrue(login_result.get('succeeded'))

    def test_login_command_timeout(self):
        with self.assertRaisesRegexp(InteractiveLoginError,
                                     'Login timeout'):
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
        self.assertFalse(self.writer.update_config.called)

    def test_login_command_failed_missing_access_key_id(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?privateKey=bar', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
        except InteractiveLoginError:
            pass
        finally:
            thread.join()
        self.assertFalse(login_result.get('succeeded'))

    def test_login_command_failed_missing_private_key(self):
        login_result = {'succeeded': False}
        thread = threading.Thread(
            target=TestLoginCommand.mock_user_login,
            args=(self, '?accessKeyId=foo', login_result))
        thread.start()
        try:
            args = ['--account-id', 'foobar', '--timeout', '1']
            parsed_globals = mock.Mock()
            self.login(self.context, args=args, parsed_globals=parsed_globals)
        except InteractiveLoginError:
            pass
        finally:
            thread.join()
        self.assertFalse(login_result.get('succeeded'))

    def test_find_unused_port(self):
        port = LoginCommand._find_unused_port()
        self.assertTrue(port > 0)

    def test_account_id_and_idp_from_login_url(self):
        parsed_args = mock.Mock()
        parsed_args.account_id = 'test'
        parsed_args.identity_provider = 'test'
        parsed_args.login_url = 'https://unit.test/auth/?accountId=foo&idp=bar'
        parsed_args.no_save_token = False
        parsed_globals = mock.Mock()
        config = {
            'account_id': 'test',
            'identity_provider': 'test'
        }
        login_url = self.login._resolve_login_url(
            parsed_args, parsed_globals, config, 10101)
        self.assert_login_url(login_url=login_url,
                              expected_hostname='unit.test',
                              expected_path='/auth/',
                              expected_account_id='foo',
                              expected_idp='bar',
                              expected_return_url_port=10101)

    def test_login_url_from_input_parameter(self):
        parsed_args = mock.Mock()
        parsed_args.account_id = 'foo'
        parsed_args.identity_provider = 'bar'
        parsed_args.login_url = 'https://unit.test/auth/?id=abc'
        parsed_args.no_save_token = False
        parsed_globals = mock.Mock()
        config = {
            'account_id': 'test',
            'identity_provider': 'test',
            'login_url': 'http://test'
        }
        login_url = self.login._resolve_login_url(
            parsed_args, parsed_globals, config, 10101)
        self.assert_login_url(login_url=login_url,
                              expected_hostname='unit.test',
                              expected_path='/auth/',
                              expected_account_id='foo',
                              expected_idp='bar',
                              expected_return_url_port=10101,
                              expected_extra_query={'key': 'id', 'value': 'abc'})

    def test_login_url_from_config(self):
        parsed_args = mock.Mock()
        parsed_args.account_id = None
        parsed_args.identity_provider = None
        parsed_args.login_url = None
        parsed_args.no_save_token = False
        parsed_globals = mock.Mock()
        config = {
            'account_id': 'foo',
            'identity_provider': 'bar',
            'login_url': 'https://unit.test/auth/?id=abc'
        }
        login_url = self.login._resolve_login_url(
            parsed_args, parsed_globals, config, 10101)
        self.assert_login_url(login_url=login_url,
                              expected_hostname='unit.test',
                              expected_path='/auth/',
                              expected_account_id='foo',
                              expected_idp='bar',
                              expected_return_url_port=10101,
                              expected_extra_query={'key': 'id', 'value': 'abc'})

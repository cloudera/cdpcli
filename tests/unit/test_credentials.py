# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
# Passed to session to keep it from finding default config file
import os
import tempfile

from cdpcli import credentials, DEFAULT_PROFILE_NAME
from cdpcli.client import Context
from cdpcli.credentials import EnvProvider
from cdpcli.exceptions import MalformedCredentialsError
from cdpcli.exceptions import NoCredentialsError
from cdpcli.exceptions import PartialCredentialsError
from cdpcli.exceptions import UnknownCredentialError
import mock
from tests import BaseEnvVar
from tests import unittest
from tests.unit import ED25519_KEY


TESTENVVARS = {'config_file': (None, 'CDP_CONFIG_FILE', None)}

raw_metadata = {
    'foobar': {
        'Code': 'Success',
        'LastUpdated': '2012-12-03T14:38:21Z',
        'AccessKeyId': 'foo',
        'PrivateKey': 'bar',
        'Expiration': '2012-12-03T20:48:03Z',
        'Type': 'CDP-HMAC'
    }
}
post_processed_metadata = {
    'role_name': 'foobar',
    'access_key_id': raw_metadata['foobar']['AccessKeyId'],
    'private_key': raw_metadata['foobar']['PrivateKey'],
    'expiry_time': raw_metadata['foobar']['Expiration'],
}
context = Context()


class TestCredentials(BaseEnvVar):
    def _ensure_credential_is_normalized_as_unicode(self, access, secret, token):
        c = credentials.Credentials(access_key_id=access,
                                    private_key=secret,
                                    access_token=token,
                                    method='test')
        self.assertTrue(isinstance(c.access_key_id, type(u'u')))
        self.assertTrue(isinstance(c.private_key, type(u'u')))
        self.assertTrue(isinstance(c.access_token, type(u'u')))

    def test_detect_nonascii_character(self):
        self._ensure_credential_is_normalized_as_unicode(
            'foo\xe2\x80\x99', 'bar\xe2\x80\x99', 'tea\xe2\x80\x99')

    def test_unicode_input(self):
        self._ensure_credential_is_normalized_as_unicode(
            u'foo', u'bar', u'tea')

    def test_frozen_credentials(self):
        cred = credentials.Credentials(access_key_id="key",
                                       private_key="secret",
                                       access_token='token',
                                       method="test")
        frozen_creds = cred.get_frozen_credentials()
        self.assertEqual("key", frozen_creds.access_key_id)
        self.assertEqual("secret", frozen_creds.private_key)
        self.assertEqual("token", frozen_creds.access_token)
        cred.access_key_id = "foobar"
        cred.private_key = "foo"
        cred.access_token = "bar"
        self.assertEqual("key", frozen_creds.access_key_id)
        self.assertEqual("secret", frozen_creds.private_key)
        self.assertEqual("token", frozen_creds.access_token)


class TestBaseCredentialProvider(BaseEnvVar):
    def test_load(self):
        cred_provider = credentials.CredentialProvider()
        self.assertTrue(cred_provider.load())


class TestEnvVar(BaseEnvVar):
    PEM = '----BEGIN MY SECRET---- SECRET ----END MY SECRET----'

    def setUp(self):
        BaseEnvVar.setUp(self)
        fd, self.pem_file = tempfile.mkstemp()
        os.write(fd, TestEnvVar.PEM.encode('utf8'))
        os.close(fd)

    def tearDown(self):
        os.remove(self.pem_file)

    def test_envvars_are_found(self):
        environ = {
            'CDP_ACCESS_KEY_ID': 'foo',
            'CDP_PRIVATE_KEY': self.pem_file,
        }
        provider = credentials.EnvProvider(environ)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_key_id, 'foo')
        self.assertEqual(creds.private_key, TestEnvVar.PEM)
        self.assertEqual(creds.method, 'env')

    def test_envvars_are_found_access_token(self):
        environ = {
            'CDP_ACCESS_TOKEN': 'tea'
        }
        provider = credentials.EnvProvider(environ)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_token, 'tea')
        self.assertEqual(creds.method, 'env')

    def test_envvars_not_found(self):
        provider = credentials.EnvProvider(environ={})
        cred = provider.load()
        self.assertTrue(cred is None)

    def test_can_override_env_var_mapping(self):
        # We can change the env var provider to
        # use our specified env var names.
        environ = {
            'FOO_ACCESS_KEY_id': 'foo',
            'FOO_PRIVATE_KEY': self.pem_file,
        }
        mapping = {
            'access_key_id': 'FOO_ACCESS_KEY_id',
            'private_key': 'FOO_PRIVATE_KEY',
        }
        provider = credentials.EnvProvider(
            environ, mapping
        )
        creds = provider.load()
        self.assertEqual(creds.access_key_id, 'foo')
        self.assertEqual(creds.private_key, TestEnvVar.PEM)

    def test_can_override_env_var_mapping_access_token(self):
        # We can change the env var provider to
        # use our specified env var names.
        environ = {
            'FOO_ACCESS_token': 'foo'
        }
        mapping = {
            'access_token': 'FOO_ACCESS_token'
        }
        provider = credentials.EnvProvider(
            environ, mapping
        )
        creds = provider.load()
        self.assertEqual(creds.access_token, 'foo')

    def test_can_override_partial_env_var_mapping(self):
        # Only changing the access key mapping.
        # The other 2 use the default values of
        # CDP_SECRET_ACCESS_KEY and CDP_SESSION_TOKEN
        # use our specified env var names.
        environ = {
            'FOO_ACCESS_KEY_ID': 'foo',
            'CDP_PRIVATE_KEY': self.pem_file,
        }
        provider = credentials.EnvProvider(
            environ, {'access_key_id': 'FOO_ACCESS_KEY_ID'}
        )
        creds = provider.load()
        self.assertEqual(creds.access_key_id, 'foo')
        self.assertEqual(creds.private_key, TestEnvVar.PEM)

    def test_partial_creds_is_an_error(self):
        # If the user provides an access key, they must also
        # provide a secret key.  Not doing so will generate an
        # error.
        environ = {
            'CDP_ACCESS_KEY_ID': 'foo',
        }
        provider = credentials.EnvProvider(environ)
        with self.assertRaises(PartialCredentialsError):
            provider.load()

    def test_credential_with_envprovider(self):
        # We can change the env var provider to
        # use our specified env var names.
        self.environ['CDP_ACCESS_KEY_ID'] = 'foo'
        self.environ['CDP_PRIVATE_KEY'] = self.pem_file
        creds = credentials.get_credentials(context)
        self.assertEqual(creds.access_key_id, 'foo')
        self.assertEqual(creds.private_key, TestEnvVar.PEM)

    def test_credential_with_envprovider_access_token(self):
        # We can change the env var provider to
        # use our specified env var names.
        self.environ['CDP_ACCESS_TOKEN'] = 'foo'
        creds = credentials.get_credentials(context)
        self.assertEqual(creds.access_token, 'foo')

    def test_raise_if_not_a_file(self):
        environ = {
            'CDP_ACCESS_KEY_ID': 'foo',
            'CDP_PRIVATE_KEY': self.pem_file + "dose_not_exist",
        }
        provider = credentials.EnvProvider(environ)
        with self.assertRaises(NoCredentialsError):
            provider.load()

    def test_ed25519_key_value(self):
        environ = {
            'CDP_ACCESS_KEY_ID': 'foo',
            'CDP_PRIVATE_KEY': ED25519_KEY,
        }
        provider = credentials.EnvProvider(environ)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_key_id, 'foo')
        self.assertEqual(creds.private_key, ED25519_KEY)
        self.assertEqual(creds.method, 'env')


class CredentialResolverTest(BaseEnvVar):
    def setUp(self):
        super(CredentialResolverTest, self).setUp()
        self.provider1 = mock.Mock()
        self.provider1.METHOD = 'provider1'
        self.provider2 = mock.Mock()
        self.provider2.METHOD = 'provider2'
        self.fake_creds = credentials.Credentials(access_key_id='a',
                                                  private_key='b',
                                                  method='test')

    def test_load_credentials_single_provider(self):
        self.provider1.load.return_value = self.fake_creds
        resolver = credentials.CredentialResolver(providers=[self.provider1])
        creds = resolver.load_credentials()
        self.assertEqual(creds.access_key_id, 'a')
        self.assertEqual(creds.private_key, 'b')

    def test_get_provider_by_name(self):
        resolver = credentials.CredentialResolver(providers=[self.provider1])
        result = resolver.get_provider('provider1')
        self.assertIs(result, self.provider1)

    def test_get_unknown_provider_raises_error(self):
        resolver = credentials.CredentialResolver(providers=[self.provider1])
        with self.assertRaises(UnknownCredentialError):
            resolver.get_provider('unknown-foo')

    def test_insert_before_unknown_provider_raises_error(self):
        resolver = credentials.CredentialResolver(providers=[self.provider1])
        with self.assertRaises(UnknownCredentialError):
            resolver.insert_before('unknown-foo', self.provider2)

    def test_first_credential_non_none_wins(self):
        self.provider1.load.return_value = None
        self.provider2.load.return_value = self.fake_creds
        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        creds = resolver.load_credentials()
        self.assertEqual(creds.access_key_id, 'a')
        self.assertEqual(creds.private_key, 'b')
        self.provider1.load.assert_called_with()
        self.provider2.load.assert_called_with()

    def test_no_creds_loaded(self):
        self.provider1.load.return_value = None
        self.provider2.load.return_value = None
        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        with self.assertRaises(NoCredentialsError):
            resolver.load_credentials()

    def test_inject_additional_providers_after_existing(self):
        self.provider1.load.return_value = None
        self.provider2.load.return_value = self.fake_creds
        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        # Now, if we were to call resolver.load() now, provider2 would
        # win because it's returning a non None response.
        # However we can inject a new provider before provider2 to
        # override this process.
        # Providers can be added by the METHOD name of each provider.
        new_provider = mock.Mock()
        new_provider.METHOD = 'new_provider'
        new_provider.load.return_value = credentials.Credentials(access_key_id='d',
                                                                 private_key='e',
                                                                 method='test')

        resolver.insert_after('provider1', new_provider)

        creds = resolver.load_credentials()
        self.assertIsNotNone(creds)

        self.assertEqual(creds.access_key_id, 'd')
        self.assertEqual(creds.private_key, 'e')
        # Provider 1 should have been called, but provider2 should
        # *not* have been called because new_provider already returned
        # a non-None response.
        self.provider1.load.assert_called_with()
        self.assertTrue(not self.provider2.called)

    def test_inject_provider_before_existing(self):
        new_provider = mock.Mock()
        new_provider.METHOD = 'override'
        new_provider.load.return_value = credentials.Credentials(access_key_id='x',
                                                                 private_key='y',
                                                                 method='test')

        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        resolver.insert_before(self.provider1.METHOD, new_provider)
        creds = resolver.load_credentials()
        self.assertEqual(creds.access_key_id, 'x')
        self.assertEqual(creds.private_key, 'y')

    def test_can_remove_providers(self):
        self.provider1.load.return_value = credentials.Credentials(access_key_id='a',
                                                                   private_key='b',
                                                                   method='test')
        self.provider2.load.return_value = credentials.Credentials(access_key_id='d',
                                                                   private_key='e',
                                                                   method='test')
        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        resolver.remove('provider1')
        creds = resolver.load_credentials()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_key_id, 'd')
        self.assertEqual(creds.private_key, 'e')
        self.assertTrue(not self.provider1.load.called)
        self.provider2.load.assert_called_with()

    def test_provider_unknown(self):
        resolver = credentials.CredentialResolver(providers=[self.provider1,
                                                             self.provider2])
        # No error is raised if you try to remove an unknown provider.
        resolver.remove('providerFOO')
        # But an error IS raised if you try to insert after an unknown
        # provider.
        with self.assertRaises(UnknownCredentialError):
            resolver.insert_after('providerFoo', None)

    def test_explicit_profile_ignores_env_provider(self):
        context = Context('explicit')
        resolver = credentials.create_credential_resolver(context)
        self.assertTrue(
            all(not isinstance(p, EnvProvider) for p in resolver.providers))


class TestCreateCredentialResolver(BaseEnvVar):
    def setUp(self):
        super(TestCreateCredentialResolver, self).setUp()
        self.fake_env_vars = {}

    def test_create_credential_resolver(self):
        resolver = credentials.create_credential_resolver(context)
        self.assertIsInstance(resolver, credentials.CredentialResolver)

    def test_no_profile_checks_env_provider(self):
        resolver = credentials.create_credential_resolver(context)
        # Then an EnvProvider should be part of our credential lookup chain.
        self.assertTrue(
            any(isinstance(p, EnvProvider) for p in resolver.providers))


def _run_test(content, method):
    path = None
    fd = None
    try:
        fd, path = tempfile.mkstemp()
        os.write(fd, content.encode('utf-8'))
        os.close(fd)
        fd = None
        method(path)
        path = None
    finally:
        if fd is not None:
            os.close(fd)
        if path is not None:
            os.remove(path)


class TestAuthConfigFile(unittest.TestCase):
    CONF = '{"access_key_id": "key_id", "private_key": "secret"}'

    def setUp(self):
        fd, self.conf_file = tempfile.mkstemp()
        os.write(fd, TestAuthConfigFile.CONF.encode('utf8'))
        os.close(fd)

    def tearDown(self):
        os.remove(self.conf_file)

    def test_config_is_found(self):
        provider = credentials.AuthConfigFile(self.conf_file)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_key_id, 'key_id')
        self.assertEqual(creds.private_key, 'secret')
        self.assertEqual(creds.method, 'auth_config_file')

    def test_config_is_found_access_token(self):
        def validate(path):
            provider = credentials.AuthConfigFile(path)
            cred = provider.load()
            self.assertIsNotNone(cred)
            self.assertEqual(cred.access_token, 'Bearer A.B.C')
            self.assertEqual(cred.method, 'auth_config_file')

        conf = '{"access_token": "Bearer A.B.C"}'
        _run_test(conf, validate)

    def test_config_not_found(self):
        with self.assertRaises(NoCredentialsError):
            provider = credentials.AuthConfigFile(
                self.conf_file + "_does_not_exist")
            provider.load()

    def test_bad_json(self):
        def validate(path):
            provider = credentials.AuthConfigFile(path)
            cred = provider.load()
            self.assertTrue(cred is None)

        conf = "{'access_key_id': 'key_id'}"
        _run_test(conf, validate)

    def test_partial_creds_is_an_error(self):
        def validate(path):
            with self.assertRaises(PartialCredentialsError):
                provider = credentials.AuthConfigFile(path)
                provider.load()

        conf = '{"access_key_id": "key_id"}'
        _run_test(conf, validate)

    def test_malformed_credentials_in_json(self):
        def validate(path):
            with self.assertRaises(MalformedCredentialsError):
                provider = credentials.AuthConfigFile(path)
                provider.load()

        conf = '{"some_key": "key_id"}'
        _run_test(conf, validate)

    def test_config_is_none(self):
        provider = credentials.AuthConfigFile(None)
        cred = provider.load()
        self.assertTrue(cred is None)


class TestSharedCredentialsProvider(unittest.TestCase):
    CONF = """
[default]
cdp_private_key = secret
cdp_access_key_id = the_key
"""

    def setUp(self):
        fd, self.conf_file = tempfile.mkstemp()
        os.write(fd, TestSharedCredentialsProvider.CONF.encode('utf8'))
        os.close(fd)

    def tearDown(self):
        os.remove(self.conf_file)

    def test_credential_file_exists_default_profile(self):
        provider = credentials.SharedCredentialProvider(
            creds_filename=self.conf_file, profile_name=DEFAULT_PROFILE_NAME)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_key_id, 'the_key')
        self.assertEqual(creds.private_key, 'secret')
        self.assertEqual(creds.method, 'shared-credentials-file')

    def test_partial_creds_missing_private_key_raise_error(self):
        def validate(path):
            provider = credentials.SharedCredentialProvider(
                creds_filename=path, profile_name=DEFAULT_PROFILE_NAME)
            with self.assertRaises(PartialCredentialsError):
                provider.load()

        conf = """
[default]
cdp_access_key_id = the_key
"""
        _run_test(conf, validate)

    def test_partial_creds_missing_key_id_raise_error(self):
        def validate(path):
            provider = credentials.SharedCredentialProvider(
                creds_filename=path, profile_name=DEFAULT_PROFILE_NAME)
            with self.assertRaises(PartialCredentialsError):
                provider.load()

        conf = """
[default]
cdp_private_key = secret
"""
        _run_test(conf, validate)

    def test_credentials_file_with_multiple_profiles(self):
        def validate(path):
            provider = credentials.SharedCredentialProvider(
                creds_filename=path, profile_name='dev')
            creds = provider.load()
            self.assertIsNotNone(creds)
            self.assertEqual(creds.access_key_id, 'admin_key')
            self.assertEqual(creds.private_key, 'admin')
            self.assertEqual(creds.method, 'shared-credentials-file')

        conf = """
[default]
cdp_private_key = secret
cdp_access_key_id = the_key

[dev]
cdp_private_key = admin
cdp_access_key_id = admin_key
"""
        _run_test(conf, validate)

    def test_credentials_file_does_not_exist_returns_none(self):
        # It's ok if the credentials file does not exist, we should
        # just catch the appropriate errors and return None.
        provider = credentials.SharedCredentialProvider(
            creds_filename='/does-not-exist', profile_name='dev')
        creds = provider.load()
        self.assertIsNone(creds)

    def test_newlines_handling(self):
        def validate(path):
            provider = credentials.SharedCredentialProvider(
                creds_filename=path, profile_name=DEFAULT_PROFILE_NAME)
            creds = provider.load()
            self.assertIsNotNone(creds)
            self.assertEqual(creds.access_key_id, 'the_key')
            self.assertEqual(creds.private_key,
                             'secret\nwith\na\nfew\nnewlines')
            self.assertEqual(creds.method, 'shared-credentials-file')

        conf = """
[default]
cdp_private_key = secret\\nwith\\na\\nfew\\nnewlines
cdp_access_key_id = the_key
"""
        _run_test(conf, validate)

    def test_credentials_file_access_token(self):
        def validate(path):
            provider = credentials.SharedCredentialProvider(
                creds_filename=path, profile_name=DEFAULT_PROFILE_NAME)
            creds = provider.load()
            self.assertIsNotNone(creds)
            self.assertEqual(creds.access_token, 'Bearer A.B.C')
            self.assertEqual(creds.method, 'shared-credentials-file')

        conf = """
[default]
cdp_access_token = Bearer A.B.C
"""
        _run_test(conf, validate)


class TestParamsProvider(unittest.TestCase):
    def test_no_param(self):
        provider = credentials.ParamsProvider(None)
        creds = provider.load()
        self.assertIsNone(creds)

    def test_no_access_token_param(self):
        params = {}
        provider = credentials.ParamsProvider(params)
        creds = provider.load()
        self.assertIsNone(creds)

    def test_empty_access_token_param(self):
        params = mock.Mock()
        params.access_token = ''
        provider = credentials.ParamsProvider(params)
        creds = provider.load()
        self.assertIsNone(creds)

    def test_access_token_param(self):
        access_token = 'Bearer A.B'
        params = mock.Mock()
        params.access_token = access_token
        provider = credentials.ParamsProvider(params)
        creds = provider.load()
        self.assertIsNotNone(creds)
        self.assertEqual(creds.access_token, access_token)
        self.assertEqual(creds.method, 'params')

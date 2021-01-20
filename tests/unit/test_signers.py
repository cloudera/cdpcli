# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import cdpcli
import cdpcli.auth
from cdpcli.credentials import Credentials
from cdpcli.exceptions import UnknownSignatureVersionError
from cdpcli.signers import RequestSigner
import mock
from tests import unittest


class BaseSignerTest(unittest.TestCase):
    def setUp(self):
        self.credentials = Credentials(access_key_id='key',
                                       private_key='secret',
                                       method='test')
        self.signer = RequestSigner('rsav1', self.credentials)
        self.fixed_credentials = self.credentials.get_frozen_credentials()


class TestSigner(BaseSignerTest):

    def test_signature_version(self):
        self.assertEqual(self.signer.signature_version, 'rsav1')

    def test_get_auth(self):
        auth_cls = mock.Mock()
        with mock.patch.dict(cdpcli.auth.AUTH_TYPE_MAPS,
                             {'rsav1': auth_cls}):
            auth = self.signer.get_auth_instance('rsav1',
                                                 extra_param='extra_param')

            self.assertEqual(auth, auth_cls.return_value)
            auth_cls.assert_called_with(
                credentials=self.fixed_credentials,
                extra_param='extra_param')

    def test_get_auth_signature_override(self):
        auth_cls = mock.Mock()
        with mock.patch.dict(cdpcli.auth.AUTH_TYPE_MAPS,
                             {'rsav1-custom': auth_cls}):
            auth = self.signer.get_auth_instance('rsav1-custom',
                                                 extra_param='extra_param')

            self.assertEqual(auth, auth_cls.return_value)
            auth_cls.assert_called_with(
                credentials=self.fixed_credentials,
                extra_param='extra_param')

    def test_get_auth_bad_override(self):
        with self.assertRaises(UnknownSignatureVersionError):
            self.signer.get_auth_instance('bad')

    def test_disable_signing(self):
        request = mock.Mock()
        auth = mock.Mock()
        self.signer = RequestSigner(cdpcli.UNSIGNED, self.credentials)
        with mock.patch.dict(cdpcli.auth.AUTH_TYPE_MAPS,
                             {'rsav1': auth}):
            self.signer.sign(request)
        auth.assert_not_called()

    def test_sign(self):
        request = mock.Mock()
        auth_cls = mock.Mock()
        with mock.patch.dict(cdpcli.auth.AUTH_TYPE_MAPS,
                             {'rsav1': auth_cls}):
            self.signer.sign(request)
            auth_cls.assert_called_with(credentials=self.fixed_credentials)

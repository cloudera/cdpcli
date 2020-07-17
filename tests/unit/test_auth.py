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
from base64 import urlsafe_b64decode
import datetime

import cdpcli.auth
from cdpcli.cdprequest import CdpRequest
from cdpcli.compat import HTTPHeaders
from cdpcli.compat import json
from cdpcli.compat import urlsplit
import cdpcli.credentials
from cdpcli.exceptions import NoCredentialsError
import mock
from tests import unittest
from tests.unit import ED25519_KEY, RSA_KEY


EXPECTED_RSA_SIG = \
    'VLlOczaMiHdAHfW7-0axYWAxpFPqHR2sR22XRh98AlVTBjj8QJTModpzNUQxb1N0F94pMP6U' \
    'BI-flm-rl3vHJaRBfcWbaDglD02YcuqD87CmOIpZ6Z3TUTbkOcxTsMSkgOaPqQkO1p49WRl3' \
    'P_v3Q9z5y6Mh7ZDbQeonQcagKhoIYQnCXYrEmAAHhTGwuxanuAsPu2y8svUBKNd9fXZ7stQ0' \
    'Pom2J2aQZnegBM6I_QJICP7ZEd0Roga0AcGoL1OsZo_fANkUV9eUvtw8CfTw11G2c1YS__pq' \
    'PVuW4iPSYONoUN5NrL6x3RtGOea0Xo__9B5ki0_TLsYMyhF37it6qA=='

EXPECTED_ED25519_SIG = \
    'et_Ueu_w3QuQbqvDdwy9aT8HLzsXBdJfRRRHe4fEK_RZ-qR-xvM35XG8J8q-YMz70GunK82JoSt5ztz0lAuCBg=='  # noqa


class BaseTestWithFixedDate(unittest.TestCase):
    def setUp(self):
        self.datetime_patch = mock.patch('cdpcli.auth.datetime')
        self.datetime_mock = self.datetime_patch.start()
        self.fixed_date = datetime.datetime(2014, 3, 10, 17, 2, 55, 0)
        self.datetime_mock.datetime.utcnow.return_value = self.fixed_date
        self.datetime_mock.datetime.strptime.return_value = self.fixed_date

    def tearDown(self):
        self.datetime_patch.stop()


class TestBaseSigner(unittest.TestCase):
    def test_base_signer_raises_error(self):
        base_signer = cdpcli.auth.BaseSigner()
        with self.assertRaises(NotImplementedError):
            base_signer.add_auth("pass someting")


class TestRSAV1(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        access_key_id = 'ABCD-EFGH-IJKL-MNOP-QRST'
        self.credentials = cdpcli.credentials.Credentials(
            access_key_id,
            RSA_KEY,
            'test')
        self.rsav1 = cdpcli.auth.RSAv1Auth(self.credentials)
        self.date_mock = mock.patch('cdpcli.auth.formatdate')
        self.formatdate = self.date_mock.start()
        self.formatdate.return_value = 'Thu, 17 Nov 2005 18:49:58 GMT'

    def tearDown(self):
        self.date_mock.stop()

    def test_put(self):
        http_headers = HTTPHeaders.from_dict({})
        split = urlsplit('/foo/bar')
        cs = self.rsav1._canonical_string('PUT', split, http_headers)
        expected_canonical = "PUT\n\nThu, 17 Nov 2005 18:49:58 GMT\n/foo/bar\nrsav1"
        self.assertEqual(expected_canonical, cs)
        sig = self.rsav1._get_signature('PUT', split, HTTPHeaders.from_dict({}))
        self.assertEqual(EXPECTED_RSA_SIG, sig)

    def test_duplicate_date(self):
        pairs = [('x-altus-date', 'Thu, 17 Nov 2015 18:49:58 GMT'),
                 ('X-Altus-Magic', 'abracadabra')]
        http_headers = HTTPHeaders.from_pairs(pairs)
        split = urlsplit('/foo/bar')
        with self.assertRaises(Exception):
            self.rsav1.get_signature('PUT', split, http_headers)

    def test_duplicate_auth_header(self):
        request = CdpRequest()
        request.headers = HTTPHeaders.from_dict({'x-altus-auth': 'signature'})
        request.method = 'PUT'
        request.url = 'https://altus.cloudera.com/service/op'
        with self.assertRaises(Exception):
            self.rsav1._inject_signature(request, 'new_signature')

    def test_resign_uses_most_recent_date(self):
        dates = [
            'Thu, 17 Nov 2005 18:49:58 GMT',
            'Thu, 17 Nov 2014 20:00:00 GMT',
        ]
        self.formatdate.side_effect = dates

        request = CdpRequest()
        request.headers['Content-Type'] = 'text/html'
        request.method = 'PUT'
        request.url = 'https://altus.cloudera.com/service/op'

        self.rsav1.add_auth(request)
        original_date = request.headers['x-altus-date']

        del request.headers['x-altus-date']
        del request.headers['x-altus-auth']
        self.rsav1.add_auth(request)
        modified_date = request.headers['x-altus-date']

        # Each time we sign a request, we make another call to formatdate()
        # so we should have a different date header each time.
        self.assertEqual(original_date, dates[0])
        self.assertEqual(modified_date, dates[1])

    def test_no_credentials_raises_error(self):
        rsav1 = cdpcli.auth.RSAv1Auth(None)
        with self.assertRaises(NoCredentialsError):
            rsav1.add_auth("pass someting")

    def test_auth_header_string(self):
        http_headers = HTTPHeaders.from_dict({})
        split = urlsplit('/foo/bar')
        sig = self.rsav1._get_signature('PUT', split, http_headers)
        self.assertEqual(EXPECTED_RSA_SIG, sig)

        auth_header_string = self.rsav1._get_signature_header(sig)
        expected_metadata = 'eyJhY2Nlc3Nfa2V5X2lkIjogIkFCQ0QtRUZHSC1JSktMLU1O' \
                            'T1AtUVJTVCIsICJhdXRoX21ldGhvZCI6ICJyc2F2MSJ9'
        metadata, sig = auth_header_string.split(".")
        self.assertEqual(expected_metadata, metadata)
        self.assertEqual(EXPECTED_RSA_SIG, sig)

        json_metadata = json.loads(
            urlsafe_b64decode(metadata.encode('utf-8')).decode('utf-8'))
        self.assertEqual(self.credentials.access_key_id,
                         json_metadata['access_key_id'])
        self.assertEqual("rsav1",
                         json_metadata['auth_method'])


class TestED25519V1(unittest.TestCase):
    """
    We're not retesting aspects that are identical to what RSA tests cover
    """

    def setUp(self):
        access_key_id = 'ABCD-EFGH-IJKL-MNOP-QRST'
        self.credentials = cdpcli.credentials.Credentials(
            access_key_id,
            ED25519_KEY,
            'test')
        self.ed25519v1 = cdpcli.auth.Ed25519v1Auth(self.credentials)
        self.date_mock = mock.patch('cdpcli.auth.formatdate')
        self.formatdate = self.date_mock.start()
        self.formatdate.return_value = 'Thu, 17 Nov 2005 18:49:58 GMT'

    def tearDown(self):
        self.date_mock.stop()

    def test_put(self):
        http_headers = HTTPHeaders.from_dict({})
        split = urlsplit('/foo/bar')
        cs = self.ed25519v1._canonical_string('PUT', split, http_headers)
        expected_canonical = "PUT\n\nThu, 17 Nov 2005 18:49:58 GMT\n/foo/bar\ned25519v1"
        self.assertEqual(expected_canonical, cs)
        sig = self.ed25519v1._get_signature('PUT', split, HTTPHeaders.from_dict({}))
        self.assertEqual(EXPECTED_ED25519_SIG, sig)

    def test_no_credentials_raises_error(self):
        ed25519v1 = cdpcli.auth.Ed25519v1Auth(None)
        with self.assertRaises(NoCredentialsError):
            ed25519v1.add_auth("pass someting")

    def test_auth_header_string(self):
        http_headers = HTTPHeaders.from_dict({})
        split = urlsplit('/foo/bar')
        sig = self.ed25519v1._get_signature('PUT', split, http_headers)
        self.assertEqual(EXPECTED_ED25519_SIG, sig)

        auth_header_string = self.ed25519v1._get_signature_header(sig)
        expected_metadata = 'eyJhY2Nlc3Nfa2V5X2lkIjogIkFCQ0QtRUZHSC1JSktMLU1OT1' \
                            'AtUVJTVCIsICJhdXRoX21ldGhvZCI6ICJlZDI1NTE5djEifQ=='
        metadata, sig = auth_header_string.split(".")
        self.assertEqual(expected_metadata, metadata)
        self.assertEqual(EXPECTED_ED25519_SIG, sig)

        json_metadata = json.loads(
            urlsafe_b64decode(metadata.encode('utf-8')).decode('utf-8'))
        self.assertEqual(self.credentials.access_key_id,
                         json_metadata['access_key_id'])
        self.assertEqual("ed25519v1",
                         json_metadata['auth_method'])

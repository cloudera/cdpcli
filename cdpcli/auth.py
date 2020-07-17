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

from base64 import b64decode, urlsafe_b64encode
from email.utils import formatdate
import logging

from asn1crypto import keys, pem
from cdpcli.compat import json
from cdpcli.compat import OrderedDict
from cdpcli.compat import urlsplit
from cdpcli.exceptions import NoCredentialsError
from pure25519 import eddsa
import rsa


LOG = logging.getLogger('cdpcli.auth')


class BaseSigner(object):
    def add_auth(self, request):
        raise NotImplementedError("add_auth")


class V1Signer(object):
    ERROR_MESSAGE = \
       "Failed to import private key from: '%s'. The private key is " \
       "corrupted or not in the right format. The private key " \
       "was extracted either from 'env' (environment variables), " \
       "'shared-credentials-file' (a profile in the shared " \
       "credential file, by default under ~/.cdp/credentials), or " \
       "'auth-config-file' (a file containing the credentials whose " \
       "location was supplied on the command line.)"

    def __init__(self, credentials, auth_method):
        self.credentials = credentials
        self.auth_method = auth_method

    def _raw_sign_string(self, string_to_sign):
        raise NotImplementedError("Implement _raw_sign_string")

    def _sign_string(self, string_to_sign):
        """
        Sign the supplied string using the credentials and return the base64
        encoded signature in UTF-8 format.
        :param string_to_sign: String to sign
        :return: Signature as string
        """
        signature = self._raw_sign_string(string_to_sign)
        return urlsafe_b64encode(signature).strip().decode('utf-8')

    def _canonical_standard_headers(self, headers):
        interesting_headers = ['content-type', 'x-altus-date']
        hoi = []
        if 'x-altus-date' in headers:
            raise Exception("x-altus-date found in headers!")
        headers['x-altus-date'] = self._get_date()
        for ih in interesting_headers:
            found = False
            for key in headers:
                lk = key.lower()
                if headers[key] is not None and lk == ih:
                    hoi.append(headers[key].strip())
                    found = True
            if not found:
                hoi.append('')
        return '\n'.join(hoi)

    def _canonical_string(self, method, split, headers):
        cs = method.upper() + '\n'
        cs += self._canonical_standard_headers(headers) + '\n'
        cs += split.path + '\n'
        cs += self.auth_method
        return cs

    def _get_signature(self, method, split, headers):
        string_to_sign = self._canonical_string(method, split, headers)
        LOG.debug('StringToSign:\n%s', string_to_sign)
        return self._sign_string(string_to_sign)

    def add_auth(self, request):
        if self.credentials is None:
            raise NoCredentialsError
        LOG.debug("Calculating signature using %s." % self.auth_method)
        LOG.debug('HTTP request method: %s', request.method)
        split = urlsplit(request.url)
        signature = self._get_signature(request.method,
                                        split,
                                        request.headers)
        self._inject_signature(request, signature)

    def _get_date(self):
        return formatdate(usegmt=True)

    def _inject_signature(self, request, signature):
        if 'x-altus-auth' in request.headers:
            raise Exception("x-altus-auth found in headers!")
        request.headers['x-altus-auth'] = self._get_signature_header(signature)

    def _get_signature_header(self, signature):
        auth_params = OrderedDict()
        auth_params['access_key_id'] = self.credentials.access_key_id
        auth_params['auth_method'] = self.auth_method
        encoded_auth_params = json.dumps(auth_params).encode('utf-8')
        return "%s.%s" % (
            urlsafe_b64encode(encoded_auth_params).strip().decode('utf-8'),
            signature)


class Ed25519v1Auth(V1Signer):
    """
    Ed25519 signing with a SHA-512 hash returning a base64 encoded signature.
    """
    AUTH_METHOD_NAME = 'ed25519v1'
    ED25519_SEED_LENGTH = 32
    ED25519_BASE64_SEED_LENGTH = 44

    def __init__(self, credentials):
        super(Ed25519v1Auth, self).__init__(credentials, self.AUTH_METHOD_NAME)

    @classmethod
    def detect_private_key(cls, key):
        return len(key) == cls.ED25519_BASE64_SEED_LENGTH

    def _raw_sign_string(self, string_to_sign):
        """
        Sign the supplied string using the credentials and return the raw signature.
        :param string_to_sign: String to sign
        :return: Raw signature as string
        """
        try:
            # We expect the private key to be a base64 formatted string.
            seed = b64decode(self.credentials.private_key)
            if len(seed) != self.ED25519_SEED_LENGTH:
                raise Exception('Not an Ed25519 private key: %s' %
                                self.credentials.private_key)

            pk = eddsa.publickey(seed)
            signature = eddsa.signature(string_to_sign.encode('utf-8'), seed, pk)
            return signature
        except Exception:
            message = self.ERROR_MESSAGE % self.credentials.method
            LOG.debug(message, exc_info=True)
            raise Exception(message)


class RSAv1Auth(V1Signer):
    """
    RSA signing with a SHA-256 hash returning a base64 encoded signature.
    """
    AUTH_METHOD_NAME = 'rsav1'

    def __init__(self, credentials):
        super(RSAv1Auth, self).__init__(credentials, self.AUTH_METHOD_NAME)

    def _raw_sign_string(self, string_to_sign):
        """
        Sign the supplied string using the credentials and return the raw signature.
        :param string_to_sign: String to sign
        :return: Raw signature as string
        """
        try:
            # We expect the private key to be the an PKCS8 pem formatted string.
            pem_bytes = self.credentials.private_key.encode('utf-8')
            if pem.detect(pem_bytes):
                _, _, der_bytes = pem.unarmor(pem_bytes)
                # In PKCS8 the key is wrapped in a container that describes it
                info = keys.PrivateKeyInfo.load(der_bytes, strict=True)
                # Directly unwrap the private key. The asn1crypto library stopped
                # offering an API call for this in their 1.0.0 release but their
                # official answer of using a separate native-code-dependent
                # library to do one line of work is unreasonable. Of course, this
                # line might break in the future...
                unwrapped = info['private_key'].parsed
                # The unwrapped key is equivalent to pkcs1 contents
                key = rsa.PrivateKey.load_pkcs1(unwrapped.dump(), 'DER')
            else:
                raise Exception('Not a PEM file')
        except Exception:
            message = self.ERROR_MESSAGE % self.credentials.method
            LOG.debug(message, exc_info=True)
            raise Exception(message)
        # We sign the hash.
        signature = rsa.sign(string_to_sign.encode('utf-8'), key, 'SHA-256')
        return signature


AUTH_TYPE_MAPS = {
    Ed25519v1Auth.AUTH_METHOD_NAME: Ed25519v1Auth,
    RSAv1Auth.AUTH_METHOD_NAME: RSAv1Auth,
}

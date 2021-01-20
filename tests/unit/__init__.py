# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2019 Cloudera, Inc. All rights reserved.
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

from cdpcli.exceptions import ProfileNotFound


RSA_KEY = \
    """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCsdjjYZKA3pqzj
XXZq2NidIg9UE3oGajYXftpiTVS3vJtfrK38202r3YbrMdAtzSm5BzVHU74C2XVX
RWrCZ9Js9Dh3RYta7uqh9b1PNdJIP+/fUBJZs5eJ+oMkTbmtgFznprUi7RcrU9Pb
k9ByIqnaJv6Dw/1SvsbwAIjbAyqNXP0WJvFc0hznHu5Ok3US8uz+fdIbalPSEKgN
6fmYmhabaGUbXmMrUevtIL9BRUY7/3vCR+vm+6UKfQqS4knMYLZN7NjIl4t/WvTX
RfuhObpgaD1c3S0LmFwFeTAmguG2tbSNDzkzG9tN3zuVxtL0+Sy/I3f+kHEVkDQW
CnnGRhBRAgMBAAECggEAbgILSJ0HRfhfl7hqbMVdhv3O4UZ7M9RUJLCaBNJnE7yP
L3wqj3wkE1j/Us83h7+yuX/LkG/uaErl+oEhFFi9dRpjWlFWDu8PY7goxXoDZGrE
S6H70pQoOa8+L84UfoO+v1UrfdfWS6DxJsMm12cdCTaOauZ9lGZ052qv4WQnpHt7
K9asyrsGSE4W1aKHIPUGbreOfwELDH/PkLLHsbVnC5PG6jpQaJHUEoebkQkVW7Ru
mG4AwH26HTNoVF5YPGSy0BRHab+0mrh4X2HHBTyzKEWQUYlWQlfW9iekmkdbrELr
adhqanguSwVUm6jrn4CRn6M2Wn55/Jv8LytlGUpBcQKBgQDAcPg74uwcz4Yoc6OM
uKVCxqwL5fOvxpVSc4+u07KCbX6tPsEXvCiPs30/a71xwcP42QBt9aLqRd8C+z99
ri36LNNLRc9/SH3TmezzXhuBK3nkX3pT6zJuiX+T5WrUueOyANUm0d9cvQsrH6uF
LtK9rXG4WKPH223QqBEPhv4yqwKBgQDla/mGm620PATZ6yUIfmJLLZF3fWiqekfk
2R6kyUhUm672mzqi1xTOByfobMjsel5UXW6gzwm9qkBG47OrvjINyq+ja/UOO4kq
ZtqlgNkL4nN5y2GTXbJrG7eabgsB3nq1+IvDC+dFTPqSuJoYfoZZtT58eMAHGTmh
Ka8ehzPo8wKBgExyFAomIMlpHtAe789M4klehqXLWTxwVI0GXwOCER2CxZmonigB
lNNQ5+YztHPmFyVZfrQvqeIKk4aprBUPBjClceIq/zx+3Y0bTmd28NIlJSy1SPDh
M415jXaA4ilTFsJ1Vjcvk91RM4iT8hzb9tdmeRBUFeuknUEQIobah0w1AoGBAKpW
f97sqYz/Xw65ozZqN+rfe3j/eP3SapzEhBcPh4+iQ8a/vEp5bO4HrB7K3meN94mm
EWR+NBpJVQ4NNDKYtas9ySiKGFmn5JDB6yckwoIrcVeFpP34fGdAHhMgDzYlDHEd
iA+aP+1ZWVYkj+0NzAzBIBLkyJa8qOg6/dWpxuX3AoGAYW0qU+s8bumhvxPEMbfQ
zU0IHNaprGxO+yZuMvdyfxcJVuFQf22mujPI8GluXyfA5i2IZjScNf5Awccu8HS5
mzJp12+5oAZ+tJE1scpVoaVdZloY8XA1u0aCt2BhZavjIizTyzY4L8clo2iGIO1x
yj5v7z/1wWTorT4w3/IHcwM=
-----END PRIVATE KEY-----"""

ED25519_KEY = 'VHswlNpwJnMpuXhIq7LYDfzs+R9pHvVLqgBGbhcnbSE='


class FakeContext(object):

    def __init__(self,
                 all_variables,
                 profile_does_not_exist=False,
                 config_file_vars={},
                 environment_vars={},
                 credentials=None):
        self.variables = all_variables
        self.profile_does_not_exist = profile_does_not_exist
        self.config = {}
        self.config_file_vars = config_file_vars
        self.environment_vars = environment_vars
        self._credentials = credentials
        self.profile = None
        self.effective_profile = None
        # This lets us use the FakeContext as both context and "client_creator"
        self.context = self

    def get_credentials(self, parsed_globals=None):
        return self._credentials

    def get_scoped_config(self):
        if self.profile_does_not_exist:
            raise ProfileNotFound(profile='foo')
        return self.config

    def get_config_variable(self, name, methods=None):
        if name == 'credentials_file':
            # The credentials_file var doesn't require a
            # profile to exist.
            return '~/fake_credentials_filename'
        if self.profile_does_not_exist and not name == 'config_file':
            raise ProfileNotFound(profile='foo')
        if methods is not None:
            if 'env' in methods:
                return self.environment_vars.get(name)
            elif 'config' in methods:
                return self.config_file_vars.get(name)
        else:
            return self.variables.get(name)

    def _build_profile_map(self):
        if self.full_config is None:
            return None
        return self.full_config['profiles']

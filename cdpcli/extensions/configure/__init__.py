# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

NOT_SET = '<not set>'
PREDEFINED_SECTION_NAMES = ('preview',)
CREDENTIAL_FILE_COMMENT = """Note on private key format.
We expect the private key to be in a modified PEM
format in which newlines are replaced with \\n."""


class ConfigValue(object):

    def __init__(self, value, source_type, source):
        self.value = value
        self.source_type = source_type
        self.source = source

    def mask_value(self):
        if self.value is NOT_SET:
            return
        self.value = mask_value(self.value)


def mask_value(current_value):
    if current_value is None:
        return 'None'
    else:
        return ('*' * 16) + current_value[-4:]

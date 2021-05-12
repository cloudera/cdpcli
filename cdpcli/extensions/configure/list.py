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
import sys

from cdpcli import CDP_ACCESS_KEY_ID_KEY_NAME,\
    CDP_ENDPOINT_URL_KEY_NAME,\
    CDP_PRIVATE_KEY_KEY_NAME,\
    ENDPOINT_URL_KEY_NAME,\
    FORM_FACTOR_KEY_NAME
from cdpcli.extensions.commands import BasicCommand

from . import ConfigValue, NOT_SET


class ConfigureListCommand(BasicCommand):
    NAME = 'list'
    DESCRIPTION = (
        'List CDP CLI configuration data. Each configuration item\'s name, '
        'value, source type, and source location are listed. For example, if '
        'you provide the CDP access key in an environment variable, this '
        'command will list the access key value and its source as the '
        'environment.\n'
    )
    SYNOPSIS = 'cdp configure list [--profile profile-name]'
    EXAMPLES = (
        'To show your current configuration values::\n'
        '\n'
        '  $ cdp configure list\n'
        '                Name                Value'
        '              Source Type    Source\n'
        '                ----                -----'
        '              -----------    ------\n'
        '             profile            <not set>'
        '                     None    None\n'
        '   cdp_access_key_id ****************ABCD  shared-credentials-file\n'
        '     cdp_private_key ****************EFGH  shared-credentials-file\n'
        '    cdp_endpoint_url            <not set>'
        '                     None    None\n'
        '        endpoint_url            <not set>'
        '                     None    None\n'
        '         form_factor            <not set>'
        '                     None    None\n'
        '\n'
    )

    def __init__(self, stream=sys.stdout):
        super(ConfigureListCommand, self).__init__()
        self._stream = stream

    def _run_main(self, client_creator, args, parsed_globals):
        context = client_creator.context

        self._display_config_value(ConfigValue('Value', 'Source Type', 'Source'),
                                   'Name')
        self._display_config_value(ConfigValue('-----', '-----------', '------'),
                                   '----')

        if context.profile is not None:
            profile = ConfigValue(context.profile, 'manual', '--profile')
        else:
            profile = self._lookup_config(context, 'profile')
        self._display_config_value(profile, 'profile')

        access_key, private_key = self._lookup_credentials(context)
        self._display_config_value(access_key, CDP_ACCESS_KEY_ID_KEY_NAME)
        self._display_config_value(private_key, CDP_PRIVATE_KEY_KEY_NAME)

        self._display_config_value(self._lookup_config(context,
                                                       CDP_ENDPOINT_URL_KEY_NAME),
                                   CDP_ENDPOINT_URL_KEY_NAME)
        self._display_config_value(self._lookup_config(context, ENDPOINT_URL_KEY_NAME),
                                   ENDPOINT_URL_KEY_NAME)
        self._display_config_value(self._lookup_config(context, FORM_FACTOR_KEY_NAME),
                                   FORM_FACTOR_KEY_NAME)

    def _display_config_value(self, config_value, config_name):
        truncated_value = (config_value.value[:27] + '...')\
            if len(config_value.value) > 30 else config_value.value
        self._stream.write('%20s %30s %24s    %s\n' % (
            config_name, truncated_value, config_value.source_type,
            config_value.source))

    def _lookup_credentials(self, context):
        # First try it with _lookup_config.  It's possible
        # that we don't find credentials this way.
        access_key = self._lookup_config(context, CDP_ACCESS_KEY_ID_KEY_NAME)
        if access_key.value is not NOT_SET:
            private_key = self._lookup_config(context, CDP_PRIVATE_KEY_KEY_NAME)
            access_key.mask_value()
            private_key.mask_value()
            return access_key, private_key
        else:
            # Otherwise we can try to use get_credentials().
            # This includes a few more lookup locations.
            credentials = context.get_credentials()
            if credentials is None:
                no_config = ConfigValue(NOT_SET, None, None)
                return no_config, no_config
            else:
                # For the ConfigValue, we don't track down the
                # config_variable because that info is not
                # visible from cdpcli.credentials.  I think
                # the credentials.method is sufficient to show
                # where the credentials are coming from.
                access_key = ConfigValue(credentials.access_key_id,
                                         credentials.method, '')
                private_key = ConfigValue(credentials.private_key,
                                          credentials.method, '')
                access_key.mask_value()
                private_key.mask_value()
                return access_key, private_key

    def _lookup_config(self, context, name):
        # First try to look up the variable in the env.
        value = context.get_config_variable(name, methods=('env',))
        if value is not None:
            return ConfigValue(value, 'env', context.context_var_map[name][1])
        # Then try to look up the variable in the config file.
        value = context.get_config_variable(name, methods=('config',))
        if value is not None:
            return ConfigValue(value, 'config-file',
                               context.get_config_variable('config_file'))
        else:
            return ConfigValue(NOT_SET, None, None)

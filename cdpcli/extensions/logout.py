# Copyright 2021 Cloudera, Inc. All rights reserved.
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

from cdpcli import CDP_ACCESS_KEY_ID_KEY_NAME, CDP_PRIVATE_KEY_KEY_NAME
from cdpcli.extensions.commands import BasicCommand
from cdpcli.extensions.configure import CREDENTIAL_FILE_COMMENT
from cdpcli.extensions.writer import ConfigFileWriter


class LogoutCommand(BasicCommand):
    """
    The 'cdp logout' command handler.
    """

    NAME = 'logout'
    DESCRIPTION = 'Logout CDP.'
    SYNOPSIS = 'cdp logout'
    EXAMPLES = (
        'To logout::\n'
        '\n'
        '    $ cdp logout\n'
    )
    SUBCOMMANDS = []
    ARG_TABLE = []

    def __init__(self, config_writer=None):
        super(LogoutCommand, self).__init__()
        if config_writer is None:
            config_writer = ConfigFileWriter()
        self._config_writer = config_writer

    def _run_main(self, client_creator, parsed_args, parsed_globals):
        # Called when invoked with no sub-command "cdp logout"
        context = client_creator.context
        self._clear_access_token(context)

    def _clear_access_token(self, context):
        credential_file_values = {CDP_ACCESS_KEY_ID_KEY_NAME: '',
                                  CDP_PRIVATE_KEY_KEY_NAME: ''}
        profile_name = context.effective_profile
        if profile_name is not None:
            credential_file_values['__section__'] = profile_name
        shared_credentials_filename = os.path.expanduser(
            context.get_config_variable('credentials_file'))
        self._config_writer.update_config(
            credential_file_values,
            shared_credentials_filename,
            config_file_comment=CREDENTIAL_FILE_COMMENT)

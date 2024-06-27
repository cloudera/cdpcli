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

import argparse
from difflib import get_close_matches
import logging
import os
import sys

from cdpcli import ARGPARSE_DASH_ENCODING


LOG = logging.getLogger('cdpcli.argparser')


class CLIArgParser(argparse.ArgumentParser):
    Formatter = argparse.RawTextHelpFormatter

    # When displaying invalid choice error messages, this controls how many
    # options to show per line.
    ChoicesPerLine = 2

    def _check_value(self, action, value):
        """
        It's probably not a great idea to override a "hidden" method but the default
        behavior is pretty ugly and there doesn't seem to be any other way to change
        it.
        """
        # converted value must be one of the choices (if specified)
        if action.choices is not None and value not in action.choices:
            choice_max_len = len(max(action.choices, key=len))
            column_width = max(choice_max_len, 40)
            msg = ['Invalid choice, valid choices are:\n']
            for i in range(len(action.choices))[::self.ChoicesPerLine]:
                current = []
                for choice in action.choices[i:i + self.ChoicesPerLine]:
                    current.append(('%-' + str(column_width) + 's') % choice)
                msg.append(' | '.join(current))
            possible = get_close_matches(value, action.choices, cutoff=0.8)
            if possible:
                extra = ['\n\nInvalid choice: %r, maybe you meant:\n' % value]
                for word in possible:
                    extra.append('  * %s' % word)
                msg.extend(extra)
            raise argparse.ArgumentError(action, '\n'.join(msg))

    def parse_known_args(self, args, namespace=None):
        parsed, remaining = super(CLIArgParser, self).parse_known_args(args, namespace)
        terminal_encoding = getattr(sys.stdin, 'encoding', 'utf-8')
        if terminal_encoding is None:
            # In some cases, sys.stdin won't have an encoding set,
            # (e.g if it's set to a StringIO).  In this case we just
            # default to utf-8.
            terminal_encoding = 'utf-8'
        for arg, value in vars(parsed).items():
            if isinstance(value, bytes):
                setattr(parsed,
                        arg,
                        self._decode_dash_if_necessary(
                            value.decode(terminal_encoding)))
            elif isinstance(value, str):
                setattr(parsed, arg, self._decode_dash_if_necessary(value))
            elif isinstance(value, list):
                encoded = []
                for v in value:
                    if isinstance(v, bytes):
                        encoded.append(self._decode_dash_if_necessary(
                            v.decode(terminal_encoding)))
                    else:
                        if isinstance(v, str):
                            v = self._decode_dash_if_necessary(v)
                        encoded.append(v)
                setattr(parsed, arg, encoded)
        LOG.debug("parse_known_args called with args: %s. Parsed args: %s, "
                  "remaining: %s" % (args, parsed, remaining))
        return parsed, remaining

    def _decode_dash_if_necessary(self, arg_value):
        # Python argparse has a bug in which '-' are not parsed correctly if
        # they appear # as values for other arguments, see:
        # http://bugs.python.org/issue9334 for more details. This defines
        # special encoding for dash that we will "decode" and replace with a
        # dash. The reason we are using \\ is that there is a non zero chance
        # that customers can discover this themselves.
        if arg_value.startswith(ARGPARSE_DASH_ENCODING):
            return arg_value.replace(ARGPARSE_DASH_ENCODING, '-', 1)
        return arg_value


class MainArgParser(CLIArgParser):
    Formatter = argparse.RawTextHelpFormatter

    def __init__(self,
                 command_table,
                 version_string,
                 description,
                 argument_table):
        super(MainArgParser, self).__init__(
            formatter_class=self.Formatter,
            add_help=False,
            conflict_handler='resolve',
            description=description,
            allow_abbrev=False)
        self._build(command_table, version_string, argument_table)
        self._command_table = command_table

    def _create_choice_help(self, choices):
        help_str = ''
        for choice in sorted(choices):
            help_str += '* %s\n' % choice
        return help_str

    def _build(self, command_table, version_string, argument_table):
        for argument_name in argument_table:
            argument = argument_table[argument_name]
            argument.add_to_parser(self)
        self.add_argument('--version',
                          action="version",
                          version=version_string,
                          help='Display the version of this tool')
        self.add_argument('command', choices=list(command_table.keys()))

    def parse_known_args(self, args, namespace=None):
        """
        Parses normally, but also adds the command_table to the parsed
        arguments, so that commands like refdoc may access it. This is a hack.
        """
        parsed, remaining = super(MainArgParser, self).parse_known_args(args, namespace)
        setattr(parsed, 'command_table', self._command_table)
        return parsed, remaining


class ArgTableArgParser(CLIArgParser):

    def __init__(self, argument_table, service_name=None, operation_name=None,
                 command_table=None):
        # command_table is an optional subcommand_table.  If it's passed
        # in, then we'll update the argparse to parse a 'subcommand' argument
        # and populate the choices field with the command table keys.
        if service_name and operation_name:
            progString = "{} {} {}".format(os.path.basename(sys.argv[0]), service_name,
                                           operation_name)
        else:
            progString = os.path.basename(sys.argv[0])
        super(ArgTableArgParser, self).__init__(
            prog=progString,
            formatter_class=self.Formatter,
            add_help=False,
            conflict_handler='resolve')
        if command_table is None:
            command_table = {}
        self._build(argument_table, command_table)

    def _build(self, argument_table, command_table):
        for arg_name in argument_table:
            argument = argument_table[arg_name]
            argument.add_to_parser(self)
        if command_table:
            self.add_argument('subcommand', choices=list(command_table.keys()),
                              nargs='?')

    def parse_known_args(self, args, namespace=None):
        if len(args) == 1 and (args[0] == 'help' or args[0] == '--help'):
            namespace = argparse.Namespace()
            namespace.help = 'help'
            return namespace, []
        else:
            return super(ArgTableArgParser, self).parse_known_args(
                args, namespace)


class ServiceArgParser(CLIArgParser):

    def __init__(self, operations_table, service_name):
        super(ServiceArgParser, self).__init__(
            prog="{} {}".format(os.path.basename(sys.argv[0]), service_name),
            formatter_class=argparse.RawTextHelpFormatter,
            add_help=False,
            conflict_handler='resolve')
        self._build(operations_table)
        self._service_name = service_name

    def _build(self, operations_table):
        self.add_argument('operation', choices=list(operations_table.keys()))

    def parse_known_args(self, args, namespace=None):
        if len(args) == 0 or (len(args) == 1 and args[0] == '--help'):
            args = ['help']
        return super(ServiceArgParser, self).parse_known_args(
            args, namespace)

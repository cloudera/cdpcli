# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2018 Cloudera, Inc. All rights reserved.
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

import difflib
import pprint

from cdpcli.arguments import BaseCLIArgument, CustomArgument
from cdpcli.clidriver import (
    CLICommand, CLIDriver, ServiceCommand, ServiceOperation)
from cdpcli.compat import OrderedDict
from cdpcli.completer import Completer
from cdpcli.help import ProviderHelpCommand
import mock
from tests import unittest


class BaseCompleterTest(unittest.TestCase):
    def setUp(self):
        self.clidriver_creator = MockCLIDriverFactory()

    def assert_completion(self, completer, cmdline, expected_results,
                          point=None):
        if point is None:
            point = len(cmdline)
        actual = set(completer.complete(cmdline, point))
        expected = set(expected_results)

        if not actual == expected:
            # Borrowed from assertDictEqual, though this doesn't
            # handle the case when unicode literals are used in one
            # dict but not in the other (and we want to consider them
            # as being equal).
            pretty_d1 = pprint.pformat(actual, width=1).splitlines()
            pretty_d2 = pprint.pformat(expected, width=1).splitlines()
            diff = ('\n' + '\n'.join(difflib.ndiff(pretty_d1, pretty_d2)))
            raise AssertionError("Results are not equal:\n%s" % diff)
        self.assertEqual(actual, expected)


class TestCompleter(BaseCompleterTest):
    def test_complete_services(self):
        commands = {
            'subcommands': {
                'foo': {},
                'bar': {
                    'subcommands': {
                        'baz': {}
                    }
                }
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp ', ['foo', 'bar'])

    def test_complete_partial_service_name(self):
        commands = {
            'subcommands': {
                'cloudfront': {},
                'cloudformation': {},
                'cloudhsm': {},
                'sts': {}
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp cloud', [
            'cloudfront', 'cloudformation', 'cloudhsm'])
        self.assert_completion(completer, 'cdp cloudf', [
            'cloudfront', 'cloudformation'])
        self.assert_completion(completer, 'cdp cloudfr', ['cloudfront'])
        self.assert_completion(completer, 'cdp cloudfront', [])

    def test_complete_on_invalid_service(self):
        commands = {
            'subcommands': {
                'foo': {},
                'bar': {
                    'subcommands': {
                        'baz': {}
                    }
                }
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp bin', [])

    def test_complete_top_level_args(self):
        commands = {
            'subcommands': {},
            'arguments': ['foo', 'bar']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp --', ['--foo', '--bar'])

    def test_complete_partial_top_level_arg(self):
        commands = {
            'subcommands': {},
            'arguments': ['foo', 'bar', 'foobar', 'fubar']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp --f', [
            '--foo', '--fubar', '--foobar'])
        self.assert_completion(completer, 'cdp --fo', [
            '--foo', '--foobar'])
        self.assert_completion(completer, 'cdp --foob', ['--foobar'])
        self.assert_completion(completer, 'cdp --foobar', [])

    def test_complete_top_level_arg_with_arg_already_used(self):
        commands = {
            'subcommands': {
                'baz': {}
            },
            'arguments': ['foo', 'bar']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp --foo --f', [])

    def test_complete_service_commands(self):
        commands = {
            'subcommands': {
                'foo': {
                    'subcommands': {
                        'bar': {
                            'arguments': ['bin']
                        },
                        'baz': {}
                    }
                }
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo ', ['bar', 'baz'])

    def test_complete_partial_service_commands(self):
        commands = {
            'subcommands': {
                'foo': {
                    'subcommands': {
                        'barb': {
                            'arguments': ['nil']
                        },
                        'baz': {},
                        'biz': {},
                        'foobar': {}
                    }
                }
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo b', ['barb', 'baz', 'biz'])
        self.assert_completion(completer, 'cdp foo ba', ['barb', 'baz'])
        self.assert_completion(completer, 'cdp foo bar', ['barb'])
        self.assert_completion(completer, 'cdp foo barb', [])

    def test_complete_service_arguments(self):
        commands = {
            'subcommands': {
                'foo': {}
            },
            'arguments': ['baz', 'bin']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo --', ['--baz', '--bin'])

    def test_complete_partial_service_arguments(self):
        commands = {
            'subcommands': {
                'biz': {}
            },
            'arguments': ['foo', 'bar', 'foobar', 'fubar']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp biz --f', [
            '--foo', '--fubar', '--foobar'])
        self.assert_completion(completer, 'cdp biz --fo', [
            '--foo', '--foobar'])
        self.assert_completion(completer, 'cdp biz --foob', ['--foobar'])

    def test_complete_service_arg_with_arg_already_used(self):
        commands = {
            'subcommands': {
                'baz': {}
            },
            'arguments': ['foo', 'bar']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp baz --foo --f', [])

    def test_complete_operation_arguments(self):
        commands = {
            'subcommands': {
                'foo': {'subcommands': {
                    'bar': {'arguments': ['baz']}
                }}
            },
            'arguments': ['bin']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo bar --', ['--baz', '--bin'])

    def test_complete_partial_operation_arguments(self):
        commands = {
            'subcommands': {
                'foo': {'subcommands': {
                    'bar': {'arguments': ['base', 'baz', 'air']}
                }}
            },
            'arguments': ['bin']
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo bar --b', [
            '--base', '--baz', '--bin'])
        self.assert_completion(completer, 'cdp foo bar --ba', [
            '--base', '--baz'])
        self.assert_completion(completer, 'cdp foo bar --bas', ['--base'])
        self.assert_completion(completer, 'cdp foo bar --base', [])

    def test_complete_operation_arg_when_arg_already_used(self):
        commands = {
            'subcommands': {
                'foo': {'subcommands': {
                    'bar': {'arguments': ['baz']}
                }}
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo bar --baz --b', [])

    def test_complete_positional_argument(self):
        commands = {
            'subcommands': {
                'foo': {'subcommands': {
                    'bar': {'arguments': [
                        'baz',
                        CustomArgument('bin', positional_arg=True)
                    ]}
                }}
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp foo bar --bin ', [])
        self.assert_completion(completer, 'cdp foo bar --bin blah --',
                               ['--baz'])

    def test_complete_undocumented_command(self):
        class UndocumentedCommand(CLICommand):
            _UNDOCUMENTED = True
        commands = {
            'subcommands': {
                'foo': {},
                'bar': UndocumentedCommand()
            },
            'arguments': []
        }
        completer = Completer(
            self.clidriver_creator.create_clidriver(commands))
        self.assert_completion(completer, 'cdp ', ['foo'])


class MockModel:
    is_hidden = False


class MockCLIDriverFactory(object):
    def create_clidriver(self, commands=None):
        clidriver = mock.Mock(spec=CLIDriver)
        clidriver._create_help_command.return_value = \
            self._create_top_level_help(clidriver, commands)

        return clidriver

    def _create_top_level_help(self, clidriver, commands):
        command_table = self.create_command_table(
            clidriver,
            commands.get('subcommands', {}), self._create_service_command)
        argument_table = self.create_argument_table(
            commands.get('arguments', []))
        return ProviderHelpCommand(
            command_table, argument_table, None, None, None)

    def _create_service_command(self, clidriver, name, command):
        command_table = self.create_command_table(
            clidriver,
            command.get('subcommands', {}), self._create_operation_command)
        service_command = ServiceCommand(clidriver, name)
        service_command._service_model = {}
        service_command._command_table = command_table
        return service_command

    def _create_operation_command(self, clidriver, name, command):
        argument_table = self.create_argument_table(
            command.get('arguments', []))
        operation = ServiceOperation(name, 'parent', None, MockModel())
        operation._arg_table = argument_table
        return operation

    def create_command_table(self, clidriver, commands, command_creator):
        if not commands:
            return OrderedDict()
        command_table = OrderedDict()
        for name, command in commands.items():
            if isinstance(command, CLICommand):
                # Already a valid command, no need to fake one
                command_table[name] = command
            else:
                command_table[name] = command_creator(clidriver, name, command)
        return command_table

    def create_argument_table(self, arguments):
        if not arguments:
            return OrderedDict()
        argument_table = OrderedDict()
        for arg in arguments:
            if isinstance(arg, BaseCLIArgument):
                argument_table[arg.name] = arg
            else:
                argument_table[arg] = BaseCLIArgument(arg)
        return argument_table

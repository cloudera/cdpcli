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

import os
import signal
import sys

from cdpcli.help import ExecutableNotFoundError
from cdpcli.help import HelpCommand, WindowsHelpRenderer
from cdpcli.help import PosixHelpRenderer
import mock
from tests import FileCreator
from tests import skip_if_windows
from tests import unittest


class HelpSpyMixin(object):
    def __init__(self):
        self.exists_on_path = {}
        self.popen_calls = []
        self.mock_popen = mock.Mock()

    def _exists_on_path(self, name):
        return self.exists_on_path.get(name)

    def _popen(self, *args, **kwargs):
        self.popen_calls.append((args, kwargs))
        return self.mock_popen


class FakePosixHelpRenderer(HelpSpyMixin, PosixHelpRenderer):
    def __init__(self, output_stream=sys.stdout):
        HelpSpyMixin.__init__(self)
        PosixHelpRenderer.__init__(self, output_stream)


class FakeWindowsHelpRenderer(HelpSpyMixin, WindowsHelpRenderer):
    def __init__(self):
        HelpSpyMixin.__init__(self)
        WindowsHelpRenderer.__init__(self)


class TestHelpPager(unittest.TestCase):

    def setUp(self):
        self.environ = {}
        self.environ_patch = mock.patch('os.environ', self.environ)
        self.environ_patch.start()
        self.renderer = PosixHelpRenderer()

    def tearDown(self):
        self.environ_patch.stop()

    def test_no_env_vars(self):
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         self.renderer.PAGER.split())

    def test_manpager(self):
        pager_cmd = 'foobar'
        os.environ['MANPAGER'] = pager_cmd
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         pager_cmd.split())

    def test_pager(self):
        pager_cmd = 'fiebaz'
        os.environ['PAGER'] = pager_cmd
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         pager_cmd.split())

    def test_both(self):
        os.environ['MANPAGER'] = 'foobar'
        os.environ['PAGER'] = 'fiebaz'
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         'foobar'.split())

    def test_manpager_with_args(self):
        pager_cmd = 'less -X'
        os.environ['MANPAGER'] = pager_cmd
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         pager_cmd.split())

    def test_pager_with_args(self):
        pager_cmd = 'less -X --clearscreen'
        os.environ['PAGER'] = pager_cmd
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         pager_cmd.split())

    def test_no_groff_or_mandoc_exists(self):
        renderer = FakePosixHelpRenderer()
        renderer.exists_on_path['groff'] = False
        renderer.exists_on_path['mandoc'] = False
        expected_error = 'Could not find executable named: groff or mandoc'
        with self.assertRaisesRegex(ExecutableNotFoundError, expected_error):
            renderer.render('foo')

    @skip_if_windows('Requires POSIX system.')
    def test_renderer_falls_back_to_mandoc(self):
        renderer = FakePosixHelpRenderer()
        renderer.exists_on_path['groff'] = False
        renderer.exists_on_path['mandoc'] = True
        renderer.mock_popen.communicate.return_value = ('rendered', '')
        renderer.render('foo')
        args, kargs = renderer.mock_popen.communicate.call_args_list[0]
        self.assertIn(b'foo', kargs.get('input'))

    def test_shlex_split_for_pager_var(self):
        pager_cmd = '/bin/sh -c "col -bx | vim -c \'set ft=man\' -"'
        os.environ['PAGER'] = pager_cmd
        self.assertEqual(self.renderer.get_pager_cmdline(),
                         ['/bin/sh', '-c', "col -bx | vim -c 'set ft=man' -"])

    def test_can_render_contents(self):
        renderer = FakePosixHelpRenderer()
        renderer.exists_on_path['groff'] = True
        renderer.mock_popen.communicate.return_value = ('rendered', '')
        renderer.render('foo')
        self.assertEqual(renderer.popen_calls[-1][0], (['less', '-R'],))

    def test_can_page_output_on_windows(self):
        renderer = FakeWindowsHelpRenderer()
        renderer.mock_popen.communicate.return_value = ('rendered', '')
        renderer.render('foo')
        self.assertEqual(renderer.popen_calls[-1][0], (['more'],))

    def test_render_environments_help(self):
        renderer = FakePosixHelpRenderer()
        renderer.exists_on_path['groff'] = True
        renderer.mock_popen.communicate.return_value = ('rendered', '')
        renderer.render('\n\n'
                        '.. _cli:cdp environments:\n\n\n'
                        '************\n'
                        'environments\n'
                        '************\n'
                        '\n\n\n'
                        ':subtitle: Cloudera Environments Service\n\n\n\n'
                        ':version: 0.9.36\n\n\n\n'
                        '===========\n'
                        'Description\n'
                        '===========\n'
                        '\n'
                        'CDP Environments Service description.\n'
                        '\n'
                        '=====================\n'
                        'Available Subcommands\n'
                        '=====================\n'
                        '\n\n\n\n'
                        '* create-environment\n\n')
        args, kargs = renderer.mock_popen.communicate.call_args_list[0]
        self.assertIn(b'Cloudera Environments Service', kargs.get('input'))
        self.assertIn(b'Generated by docutils manpage writer', kargs.get('input'))

    def test_render_environments_help_on_windows(self):
        renderer = FakeWindowsHelpRenderer()
        renderer.mock_popen.communicate.return_value = ('rendered', '')
        renderer.render('\n\n'
                        '.. _cli:cdp environments:\n\n\n'
                        '************\n'
                        'environments\n'
                        '************\n'
                        '\n\n\n'
                        ':subtitle: Cloudera Environments Service\n\n\n\n'
                        ':version: 0.9.36\n\n\n\n'
                        '===========\n'
                        'Description\n'
                        '===========\n'
                        '\n'
                        'CDP Environments Service description.\n'
                        '\n'
                        '=====================\n'
                        'Available Subcommands\n'
                        '=====================\n'
                        '\n\n\n\n'
                        '* create-environment\n\n')
        args, kargs = renderer.mock_popen.communicate.call_args
        self.assertEqual(kargs, {'input': b'\n'
                                          b'environments\n'
                                          b'^^^^^^^^^^^^\n\n\n'
                                          b'Description\n'
                                          b'***********\n\n'
                                          b'CDP Environments Service description.\n\n\n'
                                          b'Available Subcommands\n'
                                          b'*********************\n\n'
                                          b'* create-environment\n'})

    @skip_if_windows("Ctrl-C not valid on windows.")
    def test_can_handle_ctrl_c(self):
        class CtrlCRenderer(FakePosixHelpRenderer):
            def _popen(self, *args, **kwargs):
                if self._is_pager_call(args):
                    os.kill(os.getpid(), signal.SIGINT)
                return self.mock_popen

            def _is_pager_call(self, args):
                return 'less' in args[0]

        renderer = CtrlCRenderer()
        renderer.mock_popen.communicate.return_value = ('send to pager', '')
        renderer.exists_on_path['groff'] = True
        renderer.render('foo')
        last_call = renderer.mock_popen.communicate.call_args_list[-1]
        self.assertEqual(last_call, mock.call(input='send to pager'))


class TestHelpCommandBase(unittest.TestCase):
    def setUp(self):
        self.file_creator = FileCreator()

    def tearDown(self):
        self.file_creator.remove_all()


class TestHelpCommand(TestHelpCommandBase):
    """Test some of the deeper functionality of the HelpCommand

    We do this by subclassing from HelpCommand and ensure it is behaving
    as expected.
    """
    def setUp(self):
        super(TestHelpCommand, self).setUp()
        self.doc_generator_mock = mock.Mock()
        self.subcommand_mock = mock.Mock()
        self.renderer = mock.Mock()
        self.parsed_globals = mock.Mock(deprecated=False)

        class SampleHelpCommand(HelpCommand):
            GeneratorClass = self.doc_generator_mock

            @property
            def subcommand_table(sample_help_cmd_self):
                return {'mycommand': self.subcommand_mock}

        self.cmd = SampleHelpCommand(None, None, None)
        self.cmd.renderer = self.renderer

    def test_subcommand_call(self):
        self.cmd(None, ['mycommand'], self.parsed_globals)
        self.subcommand_mock.assert_called_with([], self.parsed_globals)
        self.assertFalse(self.doc_generator_mock.called)

    def test_regular_call(self):
        self.cmd(None, [], self.parsed_globals)
        self.assertFalse(self.subcommand_mock.called)
        self.doc_generator_mock.assert_called_with(self.cmd, show_hidden=False)
        self.assertTrue(self.renderer.render.called)

    def test_show_deprecated_call(self):
        parsed_globals = mock.Mock(deprecated=True)
        self.cmd(None, [], parsed_globals)
        self.assertFalse(self.subcommand_mock.called)
        self.doc_generator_mock.assert_called_with(self.cmd, show_hidden=True)
        self.assertTrue(self.renderer.render.called)

    def test_invalid_subcommand(self):
        with mock.patch('sys.stderr') as f:
            with self.assertRaises(SystemExit):
                self.cmd(None, ['no-exist-command'], self.parsed_globals)
        error_message = ''.join(arg[0][0] for arg in f.write.call_args_list)
        self.assertIn('Invalid choice', error_message)

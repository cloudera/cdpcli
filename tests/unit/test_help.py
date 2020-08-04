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
    pass


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

    def test_no_groff_exists(self):
        renderer = FakePosixHelpRenderer()
        renderer.exists_on_path['groff'] = False
        with self.assertRaisesRegexp(ExecutableNotFoundError,
                                     'Could not find executable named: groff'):
            renderer.render('foo')

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

        class SampleHelpCommand(HelpCommand):
            GeneratorClass = self.doc_generator_mock

            @property
            def subcommand_table(sample_help_cmd_self):
                return {'mycommand': self.subcommand_mock}

        self.cmd = SampleHelpCommand(None, None, None)
        self.cmd.renderer = self.renderer

    def test_subcommand_call(self):
        self.cmd(None, ['mycommand'], None)
        self.subcommand_mock.assert_called_with([], None)
        self.assertFalse(self.doc_generator_mock.called)

    def test_regular_call(self):
        self.cmd(None, [], None)
        self.assertFalse(self.subcommand_mock.called)
        self.doc_generator_mock.assert_called_with(self.cmd)
        self.assertTrue(self.renderer.render.called)

    def test_invalid_subcommand(self):
        with mock.patch('sys.stderr') as f:
            with self.assertRaises(SystemExit):
                self.cmd(None, ['no-exist-command'], None)
        error_message = ''.join(arg[0][0] for arg in f.write.call_args_list)
        self.assertIn('Invalid choice', error_message)

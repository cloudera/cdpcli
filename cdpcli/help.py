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
import platform
import shlex
from subprocess import PIPE
from subprocess import Popen

from cdpcli.argparser import ArgTableArgParser
from cdpcli.argprocess import ParamShorthand
from cdpcli.doc.restdoc import ReSTDocument
from cdpcli.docs import generate_doc
from cdpcli.docs import OperationDocumentGenerator
from cdpcli.docs import ProviderDocumentGenerator
from cdpcli.docs import ServiceDocumentGenerator
from cdpcli.exceptions import ExecutableNotFoundError
from cdpcli.textwriter import TextWriter
from cdpcli.utils import ignore_ctrl_c
from docutils.core import publish_string
from docutils.writers import manpage


def get_renderer():
    """
    Return the appropriate HelpRenderer implementation for the
    current platform.
    """
    if platform.system() == 'Windows':
        return WindowsHelpRenderer()
    else:
        return PosixHelpRenderer()


class PagingHelpRenderer(object):
    PAGER = None

    def get_pager_cmdline(self):
        pager = self.PAGER
        if 'MANPAGER' in os.environ:
            pager = os.environ['MANPAGER']
        elif 'PAGER' in os.environ:
            pager = os.environ['PAGER']
        return shlex.split(pager)

    def render(self, contents):
        """
        Each implementation of HelpRenderer must implement this
        render method.
        """
        converted_content = self._convert_doc_content(contents)
        self._send_output_to_pager(converted_content)

    def _send_output_to_pager(self, output):
        cmdline = self.get_pager_cmdline()
        p = self._popen(cmdline, stdin=PIPE)
        p.communicate(input=output)

    def _popen(self, *args, **kwargs):
        return Popen(*args, **kwargs)

    def _convert_doc_content(self, contents):
        return contents


class PosixHelpRenderer(PagingHelpRenderer):
    PAGER = 'less -R'

    def _convert_doc_content(self, contents):
        man_contents = publish_string(contents, writer=manpage.Writer())
        if not self._exists_on_path('groff'):
            raise ExecutableNotFoundError(executable_name='groff')
        cmdline = ['groff', '-man', '-T', 'ascii']
        p3 = self._popen(cmdline, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        groff_output = p3.communicate(input=man_contents)[0]
        return groff_output

    def _send_output_to_pager(self, output):
        cmdline = self.get_pager_cmdline()
        with ignore_ctrl_c():
            # We can't rely on the KeyboardInterrupt from
            # the CLIDriver being caught because when we
            # send the output to a pager it will use various
            # control characters that need to be cleaned
            # up gracefully.  Otherwise if we simply catch
            # the Ctrl-C and exit, it will likely leave the
            # users terminals in a bad state and they'll need
            # to manually run ``reset`` to fix this issue.
            # Ignoring Ctrl-C solves this issue.  It's also
            # the default behavior of less (you can't ctrl-c
            # out of a manpage).
            p = self._popen(cmdline, stdin=PIPE)
            p.communicate(input=output)

    def _exists_on_path(self, name):
        # Since we're only dealing with POSIX systems, we can
        # ignore things like PATHEXT.
        return any([os.path.exists(os.path.join(p, name))
                    for p in os.environ.get('PATH', '').split(os.pathsep)])


class WindowsHelpRenderer(PagingHelpRenderer):
    """Render help content on a Windows platform."""

    PAGER = 'more'

    def _convert_doc_content(self, contents):
        text_output = publish_string(contents,
                                     writer=TextWriter())
        return text_output

    def _popen(self, *args, **kwargs):
        # Also set the shell value to True.  To get any of the
        # piping to a pager to work, we need to use shell=True.
        kwargs['shell'] = True
        return Popen(*args, **kwargs)


class HelpCommand(object):
    GeneratorClass = None

    def __init__(self, obj, command_table, arg_table):
        self.obj = obj
        if command_table is None:
            command_table = {}
        self.command_table = command_table
        if arg_table is None:
            arg_table = {}
        self.arg_table = arg_table
        self._subcommand_table = {}
        self._related_items = []
        self.renderer = get_renderer()
        self.doc = ReSTDocument(target='man')

    @property
    def command_lineage(self):
        pass

    @property
    def name(self):
        pass

    @property
    def subcommand_table(self):
        return self._subcommand_table

    @property
    def related_items(self):
        return self._related_items

    def __call__(self, client_creator, args, parsed_globals):
        if args:
            subcommand_parser = ArgTableArgParser({}, command_table=self.subcommand_table)
            parsed, remaining = subcommand_parser.parse_known_args(args)
            if getattr(parsed, 'subcommand', None) is not None:
                return self.subcommand_table[parsed.subcommand](remaining,
                                                                parsed_globals)

        generate_doc(self.GeneratorClass(self), self)
        self.renderer.render(self.doc.getvalue())


class ProviderHelpCommand(HelpCommand):
    GeneratorClass = ProviderDocumentGenerator

    def __init__(self, command_table, arg_table, description, synopsis, usage):
        HelpCommand.__init__(self, None, command_table, arg_table)
        self.description = description
        self.synopsis = synopsis
        self.help_usage = usage

    @property
    def command_lineage(self):
        return 'cdp'

    @property
    def name(self):
        return 'cdp'


class ServiceHelpCommand(HelpCommand):
    GeneratorClass = ServiceDocumentGenerator

    def __init__(self, obj, command_table, arg_table, name, command_lineage):
        super(ServiceHelpCommand, self).__init__(obj, command_table, arg_table)
        self._name = name
        self._command_lineage = command_lineage

    @property
    def command_lineage(self):
        return self._command_lineage

    @property
    def name(self):
        return self._name


class OperationHelpCommand(HelpCommand):
    GeneratorClass = OperationDocumentGenerator

    def __init__(self, operation_model, arg_table, name, command_lineage):
        HelpCommand.__init__(self, operation_model, None, arg_table)
        self.param_shorthand = ParamShorthand()
        self._name = name
        self._command_lineage = command_lineage

    @property
    def command_lineage(self):
        return self._command_lineage

    @property
    def name(self):
        return self._name

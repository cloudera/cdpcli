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

from cdpcli import VERSION
from cdpcli.doc.restdoc import ReSTDocument
from cdpcli.extensions.commands import BasicCommand
from cdpcli.help import HelpCommand
from cdpcli.help import NullRenderer


class RefdocCommand(BasicCommand):
    """
    The 'cdp refdoc' command handler.
    """

    NAME = 'refdoc'
    DESCRIPTION = ('Generate reference documentation. Files formatted as '
                   'reStructuredText are written to the output directory. They '
                   'may be used as input to the Sphinx documentation generator '
                   'to produce a readable documentation site in any of several '
                   'formats.')
    EXAMPLES = (
        'To generate reference documentation::\n'
        '\n'
        '    $ cdp refdoc --output-directory sphinx/source\n'
    )
    SUBCOMMANDS = []
    ARG_TABLE = [
        {
            'name': 'output-directory',
            'help_text': 'Directory where generated files are written.',
            'action': 'store',
            'required': True,
            'cli_type_name': 'string'
        }
    ]

    def __init__(self):
        super(RefdocCommand, self).__init__()

    def _write_ref_docs(self, command, output_dir, client_creator, parsed_args,
                        parsed_globals):
        """
        Write all of the documents for one command. If the command has
        subcommands, then documents for those are generated recursively.
        """
        self._write_ref_doc(command, output_dir, client_creator, parsed_args,
                            parsed_globals)
        if hasattr(command, '_get_command_table'):
            subcommand_table = command._get_command_table()
        elif hasattr(command, 'subcommand_table'):
            subcommand_table = command.subcommand_table
        else:
            subcommand_table = None
        if subcommand_table:
            for subcommand in subcommand_table.values():
                self._write_ref_docs(subcommand, output_dir, client_creator,
                                     parsed_args, parsed_globals)

    def _write_ref_doc(self, command, output_dir, client_creator, parsed_args,
                       parsed_globals):
        """
        Write the document for a single command.
        """

        # Get the help command for the command to be documented.
        if isinstance(command, HelpCommand):
            help_command = command
        else:
            help_command = command.create_help_command()

        # Derive the path of the command's documentation file, relative to the
        # output directory.
        # - Top-level commands, and any other commands that have subcommands,
        #   get index.rst files in directories of their own. Examples:
        #   - command foo => foo/index.rst (whether it has subcommands or not)
        #   - subcommand bar, which itself has subcommands, of command foo =>
        #     foo/bar/index.rst
        # - Leaf subcommands get files named after themselves inside the
        #   directory of their parent command. Example:
        #   - subcommand baz of command foo => foo/baz.rst
        # - Commands with no lineage (just cdp!) go to index.rst. If there are
        #   ever multiple lineage-less commands, this needs to be changed!
        if hasattr(command, 'lineage'):
            lineage = command.lineage
        else:
            lineage = []
        if len(lineage) > 0:
            ref_doc_partial_path = '/'.join(map(lambda c: c.name, lineage))
            if (hasattr(command, '_get_command_table') and  # has subcommands
                command._get_command_table()) or \
               (hasattr(command, 'subcommand_table') and  # has subcommands
                command.subcommand_table) or \
               len(lineage) == 1:  # is a top-level command (force this case)
                ref_doc_path = '{}/index.rst'.format(ref_doc_partial_path)
            else:
                ref_doc_path = '{}.rst'.format(ref_doc_partial_path)
        else:
            ref_doc_path = 'index.rst'  # the cdp command itself
        print('- {}'.format(ref_doc_path))

        # Run the help command to generate a ReST document suitable for HTML
        # conversion.
        help_command.doc = ReSTDocument(target='html')
        help_command.renderer = NullRenderer()
        help_command.include_man_fields = False
        help_command(client_creator, [], parsed_globals)

        # Create the directory for the document, if it doesn't already exist.
        ref_doc_dir = os.path.join(output_dir, os.path.dirname(ref_doc_path))
        if not os.path.exists(ref_doc_dir):
            os.makedirs(ref_doc_dir)

        # Write out the doc "value", which is the ReST string, to the file.
        content = help_command.doc.getvalue()
        with open(os.path.join(output_dir, ref_doc_path), 'w') as ref_doc:
            ref_doc.write(str(content, encoding='utf-8'))

    def _run_main(self, client_creator, parsed_args, parsed_globals):
        if not os.path.exists(parsed_args.output_directory):
            os.makedirs(parsed_args.output_directory)

        # Write documents for every known command.
        command_table = parsed_globals.command_table  # uses argparser hack
        for command in command_table.values():
            self._write_ref_docs(command, parsed_args.output_directory,
                                 client_creator, parsed_args, parsed_globals)

        # Write a file with the version string. This may be passed as a Sphinx
        # configuration value for use in generated documentation.
        with open(os.path.join(parsed_args.output_directory,
                               '_version'), 'w') as version_file:
            version_file.write(VERSION)

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

from cdpcli import LIST_TYPE
from cdpcli import MAP_TYPE
from cdpcli import OBJECT_TYPE
from cdpcli import SCALAR_TYPES
from cdpcli import xform_name
from cdpcli.argprocess import ParamShorthandDocGen
from cdpcli.extensions.paginate import add_paging_description
from cdpcli.model import StringShape


def _is_argument_hidden(argument):
    if getattr(argument, '_UNDOCUMENTED', False):
        return True
    return False


def generate_doc(generator, help_command):
    generator.doc_title(help_command)
    generator.doc_description(help_command)
    generator.doc_synopsis_start(help_command)
    if help_command.arg_table:
        for arg_name in help_command.arg_table:
            generator.doc_synopsis_option(arg_name, help_command)
    generator.doc_synopsis_end(help_command)
    generator.doc_options_start(help_command)
    if help_command.arg_table:
        for arg_name in help_command.arg_table:
            if _is_argument_hidden(help_command.arg_table[arg_name]):
                continue
            generator.doc_option(arg_name, help_command)
            generator.doc_option_example(arg_name, help_command)
    generator.doc_options_end(help_command)
    generator.doc_subitems_start(help_command)
    if help_command.command_table:
        for command_name in sorted(help_command.command_table.keys()):
            if hasattr(help_command.command_table[command_name],
                       '_UNDOCUMENTED'):
                continue
            generator.doc_subitem(command_name, help_command)
    generator.doc_subitems_end(help_command)
    generator.doc_examples(help_command)
    generator.doc_output(help_command)
    generator.doc_relateditems_start(help_command)
    if help_command.related_items:
        for related_item in sorted(help_command.related_items):
            generator.doc_relateditem(help_command, related_item)


class CLIDocumentGenerator(object):

    def __init__(self, help_command):
        self.help_command = help_command
        self.help_command.doc.translation_map = self.build_translation_map()
        self._arg_groups = self._build_arg_table_groups(help_command)
        self._documented_arg_groups = []

    def _build_arg_table_groups(self, help_command):
        arg_groups = {}
        for name, arg in help_command.arg_table.items():
            if arg.group_name is not None:
                arg_groups.setdefault(arg.group_name, []).append(arg)
        return arg_groups

    def build_translation_map(self):
        return dict()

    # These are default doc handlers that apply in the general case.

    def doc_title(self, help_command):
        doc = help_command.doc
        doc.style.new_paragraph()
        reference = help_command.command_lineage.replace('.', ' ')
        if reference != 'cdp':
            reference = 'cdp ' + reference
        doc.writeln('.. _cli:%s:' % reference)
        doc.style.h1(help_command.name)

    def doc_description(self, help_command):
        doc = help_command.doc
        doc.style.h2('Description')
        doc.include_doc_string(help_command.description)
        doc.style.new_paragraph()

    def doc_synopsis_start(self, help_command):
        self._documented_arg_groups = []
        doc = help_command.doc
        doc.style.h2('Synopsis')
        doc.style.start_codeblock()
        doc.writeln('%s' % help_command.name)

    def doc_synopsis_option(self, arg_name, help_command):
        doc = help_command.doc
        argument = help_command.arg_table[arg_name]
        if _is_argument_hidden(argument):
            return
        if argument.group_name in self._arg_groups:
            if argument.group_name in self._documented_arg_groups:
                # This arg is already documented so we can move on.
                return
            option_str = ' | '.join(
                [a.cli_name for a in
                 self._arg_groups[argument.group_name]])
            self._documented_arg_groups.append(argument.group_name)
        elif argument.cli_type_name == 'blob':
            option_str = '%s <blob>' % argument.cli_name
        else:
            option_str = '%s <value>' % argument.cli_name
        if not argument.required:
            option_str = '[%s]' % option_str
        doc.writeln('%s' % option_str)

    def doc_synopsis_end(self, help_command):
        doc = help_command.doc
        doc.style.end_codeblock()
        # Reset the documented arg groups for other sections
        # that may document args (the detailed docs following
        # the synopsis).
        self._documented_arg_groups = []

    def doc_options_start(self, help_command):
        doc = help_command.doc
        doc.style.h2('Options')
        if not help_command.arg_table:
            doc.write('*None*\n')

    def doc_options_end(self, help_command):
        pass

    def doc_option(self, arg_name, help_command):
        doc = help_command.doc
        argument = help_command.arg_table[arg_name]
        if _is_argument_hidden(argument):
            return
        if argument.group_name in self._arg_groups:
            if argument.group_name in self._documented_arg_groups:
                # This arg is already documented so we can move on.
                return
            name = ' | '.join(
                ['``%s``' % a.cli_name for a in
                 self._arg_groups[argument.group_name]])
            self._documented_arg_groups.append(argument.group_name)
        else:
            name = '``%s``' % argument.cli_name
        doc.write('%s (%s)\n' % (name, argument.cli_type_name))
        doc.style.indent()
        doc.include_doc_string(argument.documentation)
        self._document_enums(argument, doc)
        doc.style.dedent()
        doc.style.new_paragraph()

    def doc_option_example(self, arg_name, help_command):
        pass

    def doc_relateditems_start(self, help_command):
        if help_command.related_items:
            doc = help_command.doc
            doc.style.h2('See Also')

    def doc_relateditem(self, help_command, related_item):
        doc = help_command.doc
        doc.write('* ')
        doc.style.sphinx_reference_label(
            label='cli:%s' % related_item,
            text=related_item
        )
        doc.write('\n')

    def _document_enums(self, argument, doc):
        if hasattr(argument, 'argument_model'):
            model = argument.argument_model
            if isinstance(model, StringShape):
                if model.enum:
                    self._write_possible_values(doc, model.enum)
                if model.supported_options:
                    self._write_possible_values(doc, model.supported_options)

    def _write_possible_values(self, doc, possible_values):
        doc.style.new_paragraph()
        doc.write('Possible values:')
        doc.style.start_ul()
        for value in possible_values:
            doc.style.li('``%s``' % value)
        doc.style.end_ul()

    def doc_subitems_start(self, help_command):
        pass

    def doc_subitems_end(self, help_command):
        pass

    def doc_examples(self, help_command):
        pass

    def doc_output(self, help_command):
        pass


class ProviderDocumentGenerator(CLIDocumentGenerator):

    def doc_synopsis_start(self, help_command):
        doc = help_command.doc
        doc.style.h2('Synopsis')
        doc.style.codeblock(help_command.synopsis)
        doc.include_doc_string(help_command.help_usage)

    def doc_synopsis_option(self, arg_name, help_command):
        pass

    def doc_synopsis_end(self, help_command):
        doc = help_command.doc
        doc.style.new_paragraph()

    def doc_options_start(self, help_command):
        doc = help_command.doc
        doc.style.h2('Options')

    def doc_option(self, arg_name, help_command):
        doc = help_command.doc
        argument = help_command.arg_table[arg_name]
        doc.writeln('``%s`` (%s)' % (argument.cli_name,
                                     argument.cli_type_name))
        doc.include_doc_string(argument.documentation)
        if argument.choices:
            doc.style.start_ul()
            for choice in argument.choices:
                doc.style.li(choice)
            doc.style.end_ul()

    def doc_subitems_start(self, help_command):
        doc = help_command.doc
        doc.style.h2('Available Commands')
        doc.style.toctree()

    def doc_subitem(self, command_name, help_command):
        doc = help_command.doc
        file_name = '%s/index' % command_name
        doc.style.tocitem(command_name, file_name=file_name)


class ServiceDocumentGenerator(CLIDocumentGenerator):

    def build_translation_map(self):
        d = {}
        service_model = self.help_command.obj
        for operation_name in service_model.operation_names:
            d[operation_name] = xform_name(operation_name, '-')
        return d

    # A service document has no synopsis.
    def doc_synopsis_start(self, help_command):
        pass

    def doc_synopsis_option(self, arg_name, help_command):
        pass

    def doc_synopsis_end(self, help_command):
        pass

    # A service document has no option section.
    def doc_options_start(self, help_command):
        pass

    def doc_option(self, arg_name, help_command):
        pass

    def doc_option_example(self, arg_name, help_command):
        pass

    def doc_options_end(self, help_command):
        pass

    def doc_description(self, help_command):
        doc = help_command.doc
        service_model = help_command.obj
        doc.style.h2('Description')
        # TODO: need a documentation attribute.
        doc.include_doc_string(service_model.documentation)

    def doc_subitems_start(self, help_command):
        doc = help_command.doc
        doc.style.h2('Available Subcommands')
        doc.style.toctree()

    def doc_subitem(self, command_name, help_command):
        doc = help_command.doc
        subcommand = help_command.command_table[command_name]
        subcommand_table = getattr(subcommand, 'subcommand_table', {})
        # If the subcommand table has commands in it,
        # direct the subitem to the command's index because
        # it has more subcommands to be documented.
        if (len(subcommand_table) > 0):
            file_name = '%s/index' % command_name
            doc.style.tocitem(command_name, file_name=file_name)
        else:
            doc.style.tocitem(command_name)


class OperationDocumentGenerator(CLIDocumentGenerator):

    def build_translation_map(self):
        operation_model = self.help_command.obj
        d = {}
        for cli_name, cli_argument in self.help_command.arg_table.items():
            if cli_argument.argument_model is not None:
                argument_name = cli_argument.argument_model.name
                if argument_name in d:
                    previous_mapping = d[argument_name]
                    # If the argument name is a boolean argument, we want the
                    # the translation to default to the one that does not start
                    # with --no-. So we check if the cli parameter currently
                    # being used starts with no- and if stripping off the no-
                    # results in the new proposed cli argument name. If it
                    # does, we assume we have the postive form of the argument
                    # which is the name we want to use in doc translations.
                    if cli_argument.cli_type_name == 'boolean' and \
                       previous_mapping.startswith('no-') and \
                       cli_name == previous_mapping[3:]:
                        d[argument_name] = cli_name
                else:
                    d[argument_name] = cli_name
        for operation_name in operation_model.service_model.operation_names:
            d[operation_name] = xform_name(operation_name, '-')
        return d

    def doc_description(self, help_command):
        doc = help_command.doc
        operation_model = help_command.obj
        doc.style.h2('Description')
        doc.include_doc_string(operation_model.documentation)
        add_paging_description(help_command)

    def _json_example_value_name(self, argument_model, include_enum_values=True):
        # If include_enum_values is True, then the valid enum values
        # are included as the sample JSON value.
        if isinstance(argument_model, StringShape):
            if argument_model.enum and include_enum_values:
                choices = argument_model.enum
                return '|'.join(['"%s"' % c for c in choices])
            elif argument_model.supported_options and include_enum_values:
                choices = argument_model.supported_options
                return '|'.join(['"%s"' % c for c in choices])
            else:
                return '"string"'
        elif argument_model.type_name == 'boolean':
            return 'true|false'
        else:
            return '%s' % argument_model.type_name

    def _json_example(self, doc, argument_model, stack):
        if argument_model.name in stack:
            # Document the recursion once, otherwise just
            # note the fact that it's recursive and return.
            if stack.count(argument_model.name) > 1:
                if argument_model.type_name == OBJECT_TYPE:
                    doc.write('{ ... recursive ... }')
                return
        stack.append(argument_model.name)
        try:
            self._do_json_example(doc, argument_model, stack)
        finally:
            stack.pop()

    def _do_json_example(self, doc, argument_model, stack):
        if argument_model.type_name == LIST_TYPE:
            doc.write('[')
            if argument_model.member.type_name in SCALAR_TYPES:
                doc.write('%s, ...' %
                          self._json_example_value_name(argument_model.member))
            else:
                doc.style.indent()
                doc.style.new_line()
                self._json_example(doc, argument_model.member, stack)
                doc.style.new_line()
                doc.write('...')
                doc.style.dedent()
                doc.style.new_line()
            doc.write(']')
        elif argument_model.type_name == OBJECT_TYPE:
            doc.write('{')
            doc.style.indent()
            doc.style.new_line()
            self._doc_input_structure_members(doc, argument_model, stack)
            doc.style.dedent()
            doc.style.new_line()
            doc.write('}')
        elif argument_model.type_name == MAP_TYPE:
            doc.write('{')
            doc.style.indent()
            key_string = self._json_example_value_name(argument_model.key)
            doc.write('%s: ' % key_string)
            if argument_model.value.type_name in SCALAR_TYPES:
                doc.write(self._json_example_value_name(argument_model.value))
            else:
                doc.style.indent()
                self._json_example(doc, argument_model.value, stack)
                doc.style.dedent()
            doc.style.new_line()
            doc.write('...')
            doc.style.dedent()
            doc.write('}')

    def _doc_input_structure_members(self, doc, argument_model, stack):
        need_comma = False
        members = argument_model.members
        for i, member_name in enumerate(members):
            member_model = members[member_name]
            member_type_name = member_model.type_name
            if member_type_name in SCALAR_TYPES:
                if need_comma:
                    doc.write(',')
                    doc.style.new_line()
                doc.write('"%s": %s' % (member_name,
                                        self._json_example_value_name(member_model)))
                need_comma = True
            elif member_type_name == OBJECT_TYPE:
                if need_comma:
                    doc.write(',')
                    doc.style.new_line()
                doc.write('"%s": ' % member_name)
                self._json_example(doc, member_model, stack)
                need_comma = True
            elif member_type_name == MAP_TYPE:
                if need_comma:
                    doc.write(',')
                    doc.style.new_line()
                doc.write('"%s": ' % member_name)
                self._json_example(doc, member_model, stack)
                need_comma = True
            elif member_type_name == LIST_TYPE:
                if need_comma:
                    doc.write(',')
                    doc.style.new_line()
                doc.write('"%s": ' % member_name)
                self._json_example(doc, member_model, stack)
                need_comma = True

    def doc_option_example(self, arg_name, help_command):
        doc = help_command.doc
        cli_argument = help_command.arg_table[arg_name]
        if _is_argument_hidden(cli_argument):
            return
        if cli_argument.group_name in self._arg_groups:
            if cli_argument.group_name in self._documented_arg_groups:
                # Args with group_names (boolean args) don't
                # need to generate example syntax.
                return
        argument_model = cli_argument.argument_model
        docgen = ParamShorthandDocGen()
        if docgen.supports_shorthand(cli_argument.argument_model):
            example_shorthand_syntax = docgen.generate_shorthand_example(
                cli_argument.cli_name, cli_argument.argument_model)
            if example_shorthand_syntax is None:
                # If the shorthand syntax returns a value of None,
                # this indicates to us that there is no example
                # needed for this param so we can immediately
                # return.
                return
            if example_shorthand_syntax:
                doc.style.new_paragraph()
                doc.write('Shorthand Syntax')
                doc.style.start_codeblock()
                for example_line in example_shorthand_syntax.splitlines():
                    doc.writeln(example_line)
                doc.style.end_codeblock()
        if argument_model is not None and \
           argument_model.type_name == LIST_TYPE and \
           argument_model.member.type_name in SCALAR_TYPES:
            # A list of scalars is special.  While you *can* use
            # JSON ( ["foo", "bar", "baz"] ), you can also just
            # use the argparse behavior of space separated lists.
            # "foo" "bar" "baz".  In fact we don't even want to
            # document the JSON syntax in this case.
            member = argument_model.member
            doc.style.new_paragraph()
            doc.write('Syntax')
            doc.style.start_codeblock()
            example_type = self._json_example_value_name(
                member, include_enum_values=False)
            doc.write('%s %s ...' % (example_type, example_type))
            if isinstance(member, StringShape):
                if member.enum:
                    self._write_possible_values(doc, member.enum)
                if member.supported_options:
                    self._write_possible_values(doc, member.supported_options)
            doc.style.end_codeblock()
            doc.style.new_paragraph()
        elif cli_argument.cli_type_name not in SCALAR_TYPES:
            doc.style.new_paragraph()
            doc.write('JSON Syntax')
            doc.style.start_codeblock()
            self._json_example(doc, argument_model, stack=[])
            doc.style.end_codeblock()
            doc.style.new_paragraph()

    def _write_possible_values(self, doc, possible_values):
        doc.style.new_paragraph()
        doc.write("Possible values:\n")
        for value in possible_values:
            doc.write("    %s\n" % value)
        doc.write("\n")

    def doc_output(self, help_command):
        doc = help_command.doc
        doc.style.h2('Output')
        operation_model = help_command.obj
        output_shape = operation_model.output_shape
        if output_shape is None:
            doc.write('None')
        else:
            for member_name, member_shape in output_shape.members.items():
                self._doc_member_for_output(doc, member_name, member_shape, stack=[])

    def _doc_member_for_output(self, doc, member_name, member_shape, stack):
        if member_shape.name in stack:
            # Document the recursion once, otherwise just
            # note the fact that it's recursive and return.
            if stack.count(member_shape.name) > 1:
                if member_shape.type_name == OBJECT_TYPE:
                    doc.write('( ... recursive ... )')
                return
        stack.append(member_shape.name)
        try:
            self._do_doc_member_for_output(doc, member_name,
                                           member_shape, stack)
        finally:
            stack.pop()

    def _do_doc_member_for_output(self, doc, member_name, member_shape, stack):
        docs = member_shape.documentation
        if member_name:
            doc.write('%s -> (%s)' % (member_name, member_shape.type_name))
        else:
            doc.write('(%s)' % member_shape.type_name)
        doc.style.indent()
        doc.style.new_paragraph()
        doc.include_doc_string(docs)
        doc.style.new_paragraph()
        member_type_name = member_shape.type_name
        if member_type_name == OBJECT_TYPE:
            for sub_name, sub_shape in member_shape.members.items():
                self._doc_member_for_output(doc, sub_name, sub_shape, stack)
        elif member_type_name == LIST_TYPE:
            self._doc_member_for_output(doc, '', member_shape.member, stack)
        elif member_type_name == MAP_TYPE:
            self._doc_member_for_output(doc, 'key', member_shape.key, stack)
            self._doc_member_for_output(doc, 'value', member_shape.value, stack)
        doc.style.dedent()
        doc.style.new_paragraph()

    def doc_examples(self, help_command):
        operation = help_command.command_lineage.replace('.', os.path.sep)
        doc_path = os.path.dirname(os.path.abspath(__file__))
        doc_path = os.path.join(doc_path, 'examples')
        doc_path = os.path.join(doc_path, operation) + '.rst'
        if os.path.isfile(doc_path):
            help_command.doc.style.h2('Examples')
            fp = open(doc_path)
            for line in fp.readlines():
                help_command.doc.write(line)

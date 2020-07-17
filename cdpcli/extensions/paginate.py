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
import logging

from cdpcli import model
from cdpcli import xform_name
from cdpcli.arguments import BaseCLIArgument
from cdpcli.exceptions import PaginationError


LOG = logging.getLogger(__name__)


MAX_ITEMS_ARG_NAME = 'max-items'
MAX_ITEMS_HELP_FORMAT = """
<p>The total number of items to return. If the total number of items available
is more than the value specified in max-items then a <code>nextToken</code> will
be provided in the output that you can use to resume pagination. This
<code>nextToken</code> response element should <b>not</b> be used directly
outside of the CDP CLI. This option cannnot be combined with the
<code>no-paginate</code> CLI option. If no max-items value is specified, then
a default value of %d is used.</p>
"""

STARTING_TOKEN_ARG_NAME = 'starting-token'
STARTING_TOKEN_HELP = """
<p> A token to specify where to start paginating. This is the
<code>nextToken</code> from a previously truncated response.</p>
"""

PAGE_SIZE_ARG_NAME = 'page-size'
PAGE_SIZE_HELP = """
<p>The size of each page. Generally this does not need to be set and the
default page size used is appropriate.<p>
"""

PAGING_ARG_NAMES = [STARTING_TOKEN_ARG_NAME,
                    MAX_ITEMS_ARG_NAME,
                    PAGE_SIZE_ARG_NAME]

NORMALIZED_PAGING_ARGS = [STARTING_TOKEN_ARG_NAME.replace('-', '_'),
                          MAX_ITEMS_ARG_NAME.replace('-', '_'),
                          PAGE_SIZE_ARG_NAME.replace('-', '_')]


def add_pagination_params(operation_model, argument_table):
    unify_paging_params(operation_model, argument_table)


def add_paging_description(help_command):
    # This customization is only applied to the description of
    # Operations, so we must filter out all other operation types.
    if not isinstance(help_command.obj, model.OperationModel):
        return
    operation_model = help_command.obj
    if not operation_model.can_paginate:
        return
    help_command.doc.style.new_paragraph()
    help_command.doc.writeln(
        ('``%s`` is a paginated operation. Multiple API calls may be issued '
         'in order to retrieve the entire data set of results. You can '
         'disable pagination by providing the ``--no-paginate`` argument.')
        % help_command.name)


def unify_paging_params(operation_model, argument_table):
    if not operation_model.can_paginate:
        return

    LOG.debug("Modifying paging parameters for operation: %s" %
              operation_model.name)
    _remove_existing_paging_arguments(argument_table, operation_model)

    arg = PageArgument(argument_table,
                       MAX_ITEMS_ARG_NAME,
                       MAX_ITEMS_HELP_FORMAT
                       % operation_model.paging_default_max_items,
                       parse_type='integer',
                       serialized_name='MaxItems')
    if MAX_ITEMS_ARG_NAME in argument_table:
        del argument_table[MAX_ITEMS_ARG_NAME]
    argument_table[MAX_ITEMS_ARG_NAME] = arg

    arg = PageArgument(argument_table,
                       STARTING_TOKEN_ARG_NAME,
                       STARTING_TOKEN_HELP,
                       parse_type='string',
                       serialized_name='StartingToken')
    if STARTING_TOKEN_ARG_NAME in argument_table:
        del argument_table[STARTING_TOKEN_ARG_NAME]
    argument_table[STARTING_TOKEN_ARG_NAME] = arg

    arg = PageArgument(argument_table,
                       PAGE_SIZE_ARG_NAME,
                       PAGE_SIZE_HELP,
                       parse_type='integer',
                       serialized_name='PageSize')
    if PAGE_SIZE_ARG_NAME in argument_table:
        del argument_table[PAGE_SIZE_ARG_NAME]
    argument_table[PAGE_SIZE_ARG_NAME] = arg


def check_should_enable_pagination(argument_table, operation_model, parsed_args,
                                   parsed_globals):
    for token in _get_all_cli_input_tokens(operation_model):
        py_name = token.replace('-', '_')
        if getattr(parsed_args, py_name) is not None and \
                py_name not in NORMALIZED_PAGING_ARGS:
            # The user has specified a manual (undocumented) pagination arg.
            # We need to automatically turn pagination off.
            LOG.debug("User has specified a manual pagination arg: %s. "
                      "Automatically setting --no-paginate." % token)
            parsed_globals.paginate = False

            # Because pagination is now disabled, there's a chance that
            # we were shadowing arguments.  For example, we inject a
            # --max-items argument in unify_paging_params().  If the
            # the operation also provides its own MaxItems (which we
            # expose as --max-items) then our custom pagination arg
            # was shadowing the customers arg.  When we turn pagination
            # off we need to put back the original argument which is
            # what we're doing here.
            for shadowed_arg in _get_all_shadowed_paging_args(argument_table):
                argument_table[shadowed_arg.name] = \
                    shadowed_arg.shadowed_argument

    if not parsed_globals.paginate:
        ensure_paging_params_not_set(argument_table, parsed_args)


def ensure_paging_params_not_set(argument_table, parsed_args):
    shadowed_params = []
    for shadowed_arg in _get_all_shadowed_paging_args(argument_table):
        shadowed_params.extend(shadowed_arg.name.replace('-', '_'))

    params_used = [p for p in NORMALIZED_PAGING_ARGS if
                   p not in shadowed_params and getattr(parsed_args, p, None)]

    if len(params_used) > 0:
        converted_params = ', '.join(
            ["--" + p.replace('_', '-') for p in params_used])
        raise PaginationError(
            message="Cannot specify --no-paginate along with pagination "
                    "arguments: %s" % converted_params)


def _get_all_shadowed_paging_args(argument_table):
    for paging_arg_name in PAGING_ARG_NAMES:
        paging_arg = argument_table.get(paging_arg_name, None)
        if paging_arg is not None and isinstance(paging_arg, PageArgument) and \
           paging_arg.shadowed_argument is not None:
            yield paging_arg


def _remove_existing_paging_arguments(argument_table, operation_model):
    for token in _get_all_cli_input_tokens(operation_model):
        argument_table[token]._UNDOCUMENTED = True


def _get_all_cli_input_tokens(operation_model):
    if operation_model.can_paginate:
        yield xform_name(operation_model.paging_input_token, '-')
        yield xform_name(operation_model.paging_page_size, '-')


class PageArgument(BaseCLIArgument):
    type_map = {
        'string': str,
        'integer': int,
    }

    def __init__(self, argument_table, name, documentation, parse_type,
                 serialized_name):
        super(PageArgument, self).__init__(name)
        shape_resolver = model.ShapeResolver({})
        self.argument_model = shape_resolver.get_shape('PageArgument',
                                                       {'type': 'string'})
        self._serialized_name = serialized_name
        self._documentation = documentation
        self._parse_type = parse_type
        self._required = False
        self._shadowed_argument = None
        if name in argument_table:
            # If there's already an entry in the arg table for this argument,
            # this means we're shadowing an argument for this operation.  We
            # need to store this later in case pagination is turned off because
            # we put these arguments back.
            # See the comment in check_should_enable_pagination() for more info.
            self._shadowed_argument = argument_table[name]

    @property
    def cli_name(self):
        return '--' + self._name

    @property
    def cli_type(self):
        return self.cli_type_name

    @property
    def cli_type_name(self):
        return self._parse_type

    @property
    def required(self):
        return self._required

    @required.setter
    def required(self, value):
        self._required = value

    @property
    def documentation(self):
        return self._documentation

    @property
    def shadowed_argument(self):
        return self._shadowed_argument

    def add_to_parser(self, parser):
        parser.add_argument(self.cli_name, dest=self.py_name,
                            type=self.type_map[self._parse_type])

    def add_to_params(self, parameters, value):
        if value is not None:
            pagination_config = parameters.get('PaginationConfig', {})
            pagination_config[self._serialized_name] = value
            parameters['PaginationConfig'] = pagination_config

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

from cdpcli import LIST_TYPE
from cdpcli import MAP_TYPE
from cdpcli import OBJECT_TYPE
from cdpcli.argprocess import ParamShorthand
from cdpcli.argprocess import unpack_cli_arg
from cdpcli.model import ShapeResolver
from cdpcli.schema import SchemaTransformer


def create_argument_model_from_schema(schema):
    # Given a JSON schema (described in schema.py), convert it
    # to a shape object from `cdp.model.Shape` that can be
    # used as the argument_model for the Argument classes below.
    transformer = SchemaTransformer()
    shapes_map = transformer.transform(schema)
    shape_resolver = ShapeResolver(shapes_map)
    # The SchemaTransformer guarantees that the top level shape
    # will always be named 'InputShape'.
    arg_shape = shape_resolver.get_shape_by_name('InputShape', 'InputShape')
    return arg_shape


class UnknownArgumentError(Exception):
    pass


def first_non_none_response(responses, default=None):
    for response in responses:
        if response[1] is not None:
            return response[1]
    return default


class BaseCLIArgument(object):

    def __init__(self, name):
        self._name = name

    def add_to_arg_table(self, argument_table):
        argument_table[self.name] = self

    def add_to_parser(self, parser):
        pass

    def add_to_params(self, parameters, value):
        pass

    @property
    def name(self):
        return self._name

    @property
    def cli_name(self):
        return '--' + self._name

    @property
    def cli_type_name(self):
        raise NotImplementedError("cli_type_name")

    @property
    def required(self):
        raise NotImplementedError("required")

    @property
    def documentation(self):
        raise NotImplementedError("documentation")

    @property
    def cli_type(self):
        raise NotImplementedError("cli_type")

    @property
    def py_name(self):
        return self._name.replace('-', '_')

    @property
    def choices(self):
        return None

    @property
    def synopsis(self):
        return ''

    @property
    def positional_arg(self):
        return False

    @property
    def nargs(self):
        return None

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def group_name(self):
        return None


class CustomArgument(BaseCLIArgument):

    def __init__(self,
                 name,
                 help_text='',
                 dest=None,
                 default=None,
                 action=None,
                 required=None,
                 choices=None,
                 nargs=None,
                 cli_type_name=None,
                 group_name=None,
                 positional_arg=False,
                 no_paramfile=False,
                 argument_model=None,
                 synopsis='',
                 hidden=False):
        self._name = name
        self._help = help_text
        self._dest = dest
        self._default = default
        self._action = action
        self._required = required
        self._nargs = nargs
        self._cli_type_name = cli_type_name
        self._group_name = group_name
        self._positional_arg = positional_arg
        if choices is None:
            choices = []
        self._choices = choices
        self._synopsis = synopsis
        self._UNDOCUMENTED = hidden

        # These are public attributes that are ok to access from external
        # objects.
        self.no_paramfile = no_paramfile
        if argument_model is None:
            argument_model = self._create_scalar_argument_model()
        self.argument_model = argument_model

        # If the top level element is a list then set nargs to
        # accept multiple values seperated by a space.
        if self.argument_model is not None and \
                self.argument_model.type_name == LIST_TYPE:
            self._nargs = '+'

    def _create_scalar_argument_model(self):
        if self._nargs is not None:
            # If nargs is not None then argparse will parse the value
            # as an array, so we don't create an argument_object so we don't
            # go through param validation.
            return None
        # If no argument model is provided, we create a basic
        # shape argument.
        type_name = self.cli_type_name
        return create_argument_model_from_schema({'type': type_name})

    @property
    def cli_name(self):
        if self._positional_arg:
            return self._name
        else:
            return '--' + self._name

    def add_to_parser(self, parser):
        cli_name = self.cli_name
        kwargs = {}
        if self._dest is not None:
            kwargs['dest'] = self._dest
        if self._action is not None:
            kwargs['action'] = self._action
        if self._default is not None:
            kwargs['default'] = self._default
        if self._choices:
            kwargs['choices'] = self._choices
        if self._required is not None:
            kwargs['required'] = self._required
        if self._nargs is not None:
            kwargs['nargs'] = self._nargs
        parser.add_argument(cli_name, **kwargs)

    @property
    def required(self):
        if self._required is None:
            return False
        return self._required

    @required.setter
    def required(self, value):
        self._required = value

    @property
    def documentation(self):
        return self._help

    @property
    def cli_type_name(self):
        if self._cli_type_name is not None:
            return self._cli_type_name
        elif self._action in ['store_true', 'store_false']:
            return 'boolean'
        else:
            # Default to 'string' type if we don't have any
            # other info.
            return 'string'

    @property
    def cli_type(self):
        cli_type = str
        if self._action in ['store_true', 'store_false']:
            cli_type = bool
        return cli_type

    @property
    def choices(self):
        return self._choices

    @property
    def group_name(self):
        return self._group_name

    @property
    def synopsis(self):
        return self._synopsis

    @property
    def positional_arg(self):
        return self._positional_arg

    @property
    def nargs(self):
        return self._nargs


class CLIArgument(BaseCLIArgument):
    """Represents a CLI argument that maps to a service parameter.

    """

    TYPE_MAP = {
        OBJECT_TYPE: str,
        MAP_TYPE: str,
        'timestamp': str,
        LIST_TYPE: str,
        'string': str,
        'float': float,
        'integer': str,
        'long': int,
        'boolean': bool,
        'double': float,
        'blob': str
    }

    def __init__(self,
                 name,
                 argument_model,
                 operation_model,
                 is_required=False,
                 serialized_name=None,
                 no_paramfile=False):
        self._name = name
        # This is the name we need to use when constructing the parameters
        # dict we send to CDP.  While we can change the .name attribute
        # which is the name exposed in the CLI, the serialized name we use
        # for CDP is invariant and should not be changed.
        if serialized_name is None:
            serialized_name = name
        self._serialized_name = serialized_name
        self.argument_model = argument_model
        self._required = is_required
        self._operation_model = operation_model
        self.no_paramfile = no_paramfile
        self._UNDOCUMENTED = self.argument_model.is_undocumented

    @property
    def py_name(self):
        return self._name.replace('-', '_')

    @property
    def required(self):
        return self._required

    @required.setter
    def required(self, value):
        self._required = value

    @property
    def documentation(self):
        return self.argument_model.documentation

    @property
    def cli_type_name(self):
        return self.argument_model.type_name

    @property
    def cli_type(self):
        return self.TYPE_MAP.get(self.argument_model.type_name, str)

    def add_to_parser(self, parser):
        cli_name = self.cli_name
        parser.add_argument(
            cli_name,
            help=self.documentation,
            type=self.cli_type,
            required=self.required)

    def add_to_params(self, parameters, value):
        if value is None:
            return
        else:
            # This is a two step process.  First is the process of converting
            # the command line value into a python value.  Normally this is
            # handled by argparse directly, but there are cases where extra
            # processing is needed.  For example, "--foo name=value" the value
            # can be converted from "name=value" to {"name": "value"}.  This is
            # referred to as the "unpacking" process.  Once we've unpacked the
            # argument value, we have to decide how this is converted into
            # something that can be consumed by CDP.  Many times this is
            # just associating the key and value in the params dict as down
            # below.  Sometimes this can be more complicated, and subclasses
            # can customize as they need.
            unpacked = self._unpack_argument(value)
            parameters[self._serialized_name] = unpacked

    def _unpack_argument(self, value):
        override = ParamShorthand()(self, value)
        if override is not None:
            return override
        return unpack_cli_arg(self, value)


class ListArgument(CLIArgument):

    def add_to_parser(self, parser):
        cli_name = self.cli_name
        parser.add_argument(cli_name,
                            nargs='*',
                            type=self.cli_type,
                            required=self.required)


class BooleanArgument(CLIArgument):

    def __init__(self,
                 name,
                 argument_model,
                 operation_model,
                 is_required=False,
                 action='store_true',
                 dest=None,
                 group_name=None,
                 default=None,
                 serialized_name=None,
                 no_paramfile=False):
        super(BooleanArgument, self).__init__(name,
                                              argument_model,
                                              operation_model,
                                              is_required,
                                              serialized_name=serialized_name,
                                              no_paramfile=no_paramfile)
        self._mutex_group = None
        self._action = action
        if dest is None:
            self._destination = self.py_name
        else:
            self._destination = dest
        if group_name is None:
            self._group_name = self.name
        else:
            self._group_name = group_name
        self._default = default

    def add_to_params(self, parameters, value):
        # If a value was explicitly specified (so value is True/False
        # but *not* None) then we add it to the params dict.
        # If the value was not explicitly set (value is None)
        # we don't add it to the params dict.
        if value is not None:
            parameters[self._serialized_name] = value

    def add_to_arg_table(self, argument_table):
        # Boolean parameters are a bit tricky.  For a single boolean parameter
        # we actually want two CLI params, a --foo, and a --no-foo.  To do this
        # we need to add two entries to the argument table.  So we can add
        # ourself as the positive option (--no), and then create a clone of
        # ourselves for the negative service.  We then insert both into the
        # arg table.
        argument_table[self.name] = self
        negative_name = 'no-%s' % self.name
        negative_version = self.__class__(negative_name,
                                          self.argument_model,
                                          self._operation_model,
                                          action='store_false',
                                          dest=self._destination,
                                          group_name=self.group_name,
                                          serialized_name=self._serialized_name)
        argument_table[negative_name] = negative_version

    def add_to_parser(self, parser):
        parser.add_argument(self.cli_name,
                            help=self.documentation,
                            action=self._action,
                            default=self._default,
                            dest=self._destination)

    @property
    def group_name(self):
        return self._group_name

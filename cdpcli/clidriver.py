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
import platform
import sys

from cdpcli import LIST_TYPE
from cdpcli import VERSION
from cdpcli import xform_name
from cdpcli.argparser import ArgTableArgParser
from cdpcli.argparser import MainArgParser
from cdpcli.argparser import ServiceArgParser
from cdpcli.argprocess import unpack_argument
from cdpcli.arguments import BooleanArgument
from cdpcli.arguments import CLIArgument
from cdpcli.arguments import CustomArgument
from cdpcli.arguments import ListArgument
from cdpcli.arguments import UnknownArgumentError
from cdpcli.clicommand import CLICommand
from cdpcli.client import ClientCreator
from cdpcli.client import Context
from cdpcli.compat import copy_kwargs
from cdpcli.compat import OrderedDict
from cdpcli.compat import six
from cdpcli.endpoint import EndpointCreator
from cdpcli.endpoint import EndpointResolver
from cdpcli.extensions.arguments import OverrideRequiredArgsArgument
from cdpcli.extensions.cliinputjson import add_cli_input_json
from cdpcli.extensions.configure.configure import ConfigureCommand
from cdpcli.extensions.generatecliskeleton import add_generate_skeleton
from cdpcli.extensions.paginate import add_pagination_params
from cdpcli.extensions.paginate import check_should_enable_pagination
from cdpcli.formatter import get_formatter
from cdpcli.help import OperationHelpCommand
from cdpcli.help import ProviderHelpCommand
from cdpcli.help import ServiceHelpCommand
from cdpcli.loader import Loader
from cdpcli.model import ServiceModel
from cdpcli.paramfile import ParamFileVisitor
from cdpcli.parser import ResponseParserFactory
from cdpcli.retryhandler import create_retry_handler
from cdpcli.translate import build_retry_config


LOG = logging.getLogger('cdpcli.clidriver')
ROOT_LOGGER = logging.getLogger('')
LOG_FORMAT = ('%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s')


def main():
    driver = CLIDriver()
    return driver.main()


class CLIDriver(object):

    def __init__(self):
        self._loader = Loader()
        self._endpoint_creator = EndpointCreator(EndpointResolver())
        self._user_agent_header = self._build_user_agent_header()
        self._response_parser_factory = ResponseParserFactory()
        self._cli_data = self._loader.load_json('cli.json')
        self._retryhandler = self._create_default_retryhandler()
        self._available_services = self._loader.list_available_services()
        self._command_table = self._build_command_table()
        self._argument_table = self._build_argument_table()
        self._client_creator = ClientCreator(self._loader,
                                             Context(),
                                             self._endpoint_creator,
                                             self._user_agent_header,
                                             self._response_parser_factory,
                                             self._retryhandler)

    def main(self, args=None):
        if args is None:
            args = sys.argv[1:]
        parser = self._create_parser()
        command_table = self._get_command_table()
        if len(args) == 0 or (len(args) == 1 and args[0] == '--help'):
            args = ['help']
        parsed_args, remaining = parser.parse_known_args(args)
        try:
            self._handle_top_level_args(parsed_args)
            return command_table[parsed_args.command](
                self._client_creator, remaining, parsed_args)
        except Exception as e:
            LOG.debug("Exception caught in main()", exc_info=True)
            sys.stderr.write("\n")
            sys.stderr.write("%s\n" % six.text_type(e))
            return 255

    def _get_loader(self):
        return self._loader

    def _get_cli_data(self):
        return self._cli_data

    def _get_command_table(self):
        return self._command_table

    def _build_user_agent_header(self):
        return 'CDPCLI/%s Python/%s %s/%s' % (VERSION,
                                              platform.python_version(),
                                              platform.system(),
                                              platform.release())

    def _build_command_table(self):
        commands = OrderedDict()
        services = self._get_available_services()
        for service_name in services:
            commands[service_name] = ServiceCommand(self, service_name)
        ConfigureCommand.add_command(commands)
        return commands

    def _get_argument_table(self):
        return self._argument_table

    def _build_argument_table(self):
        argument_table = OrderedDict()
        cli_data = self._get_cli_data()
        cli_arguments = cli_data.get('options', None)
        for option in cli_arguments:
            option_params = copy_kwargs(cli_arguments[option])
            cli_argument = self._create_cli_argument(option, option_params)
            cli_argument.add_to_arg_table(argument_table)
        return argument_table

    def _get_available_services(self):
        return self._available_services

    def get_service_model(self, service_name):
        service_data = self._loader.load_service_data(service_name)
        return ServiceModel(service_data, service_name=service_name)

    def _create_help_command(self):
        cli_data = self._get_cli_data()

        # We filter service aliases out of the service list at the bottom of the
        # top level help.
        commands = OrderedDict()
        for service_name, command in self._get_command_table().items():
            if not self._loader.is_service_alias(service_name):
                commands[service_name] = command

        return ProviderHelpCommand(commands,
                                   self._get_argument_table(),
                                   cli_data.get('description', None),
                                   cli_data.get('synopsis', None),
                                   cli_data.get('help_usage', None))

    def _create_parser(self):
        command_table = self._get_command_table()
        command_table['help'] = self._create_help_command()
        cli_data = self._get_cli_data()
        parser = MainArgParser(
          command_table,
          VERSION,
          cli_data.get('description', None),
          self._get_argument_table())
        return parser

    def _create_cli_argument(self, option_name, option_params):
        return CustomArgument(
          option_name,
          help_text=option_params.get('help', ''),
          dest=option_params.get('dest'),
          default=option_params.get('default'),
          action=option_params.get('action'),
          required=option_params.get('required'),
          choices=option_params.get('choices'),
          hidden=option_params.get('hidden', False))

    def _handle_top_level_args(self, args):
        if args.profile:
            self._client_creator.context.set_config_variable('profile',
                                                             args.profile)
        if args.auth_config:
            self._client_creator.context.set_config_variable('auth_config',
                                                             args.auth_config)
        if args.debug:
            self._setup_logger(logging.DEBUG)
            LOG.debug("CLI version: %s", self._user_agent_header)
            LOG.debug("Arguments entered to CLI: %s", sys.argv[1:])
        else:
            self._setup_logger(logging.ERROR)

    def _setup_logger(self, log_level):
        ROOT_LOGGER.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        formatter = logging.Formatter(LOG_FORMAT)
        ch.setFormatter(formatter)
        ROOT_LOGGER.addHandler(ch)

    def _create_default_retryhandler(self):
        # We create one retryhandler based on the __default__ configuration in
        # the _retry.json (in the 'data' directory). This retryhandler is used
        # by all services.
        config = self._load_retry_config()
        if not config:
            return
        LOG.info("Using retry config: %s" % config)
        return create_retry_handler(config)

    def _load_retry_config(self):
        original_config = self._loader.load_json('_retry.json')
        retry_config = build_retry_config(
            original_config['retry'],
            original_config.get('definitions', {}))
        return retry_config


class ServiceCommand(CLICommand):

    def __init__(self, clidriver, name):
        self._clidriver = clidriver
        self._name = name
        self._command_table = None
        self._lineage = [self]
        self._service_model = None

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def service_model(self):
        return self._get_service_model()

    @property
    def lineage(self):
        return self._lineage

    @lineage.setter
    def lineage(self, value):
        self._lineage = value

    def _get_command_table(self):
        if self._command_table is None:
            self._command_table = self._create_command_table()
        return self._command_table

    def _get_service_model(self):
        if self._service_model is None:
            self._service_model = self._clidriver.get_service_model(self._name)
        return self._service_model

    def __call__(self, client_creator, args, parsed_globals):
        # Once we know we're trying to call a service for this operation
        # we can go ahead and create the parser for it.  We
        # can also grab the Service object from botocore.
        service_parser = self._create_parser()
        parsed_args, remaining = service_parser.parse_known_args(args)
        command_table = self._get_command_table()
        return command_table[parsed_args.operation](
            client_creator, remaining, parsed_globals)

    def _create_command_table(self):
        command_table = OrderedDict()
        service_model = self._get_service_model()
        for operation_name in service_model.operation_names:
            cli_name = xform_name(operation_name, '-')
            operation_model = service_model.operation_model(operation_name)
            command_table[cli_name] = ServiceOperation(
                name=cli_name,
                parent_name=self._name,
                operation_model=operation_model,
                operation_caller=CLIOperationCaller())
        try:
            __import__('cdpcli.extensions.%s.register' % self._name,
                       fromlist=['register']).register(command_table)
        except ImportError as err:
            py3_err = "No module named 'cdpcli.extensions.%s'" % self._name
            py2_err = "No module named %s.register" % self._name
            if py2_err not in str(err) and py3_err not in str(err):
                # Looks like a different error than missing extensions.
                LOG.warn("Failed to import service (%s) extension: %s", self._name, err)
            pass
        self._add_lineage(command_table)
        return command_table

    def _add_lineage(self, command_table):
        for command in command_table:
            command_obj = command_table[command]
            command_obj.lineage = self.lineage + [command_obj]

    def create_help_command(self):
        command_table = OrderedDict()
        for command_name, command in self._get_command_table().items():
            command_table[command_name] = command
        return ServiceHelpCommand(obj=self._get_service_model(),
                                  command_table=command_table,
                                  arg_table=None,
                                  command_lineage='.'.join(self.lineage_names),
                                  name=self._name)

    def _create_parser(self):
        command_table = self._get_command_table()
        # Also add a 'help' command.
        command_table['help'] = self.create_help_command()
        return ServiceArgParser(
            operations_table=command_table, service_name=self._name)


class ServiceOperation(object):
    ARG_TYPES = {
        LIST_TYPE: ListArgument,
        'boolean': BooleanArgument,
    }
    DEFAULT_ARG_CLASS = CLIArgument

    def __init__(self, name, parent_name, operation_caller, operation_model):
        self._arg_table = None
        self._name = name
        # These is used so we can figure out what the proper event
        # name should be <parent name>.<name>.
        self._parent_name = parent_name
        # We can have more than one operation callers. They are called in
        # order and if any returns 'False' no other callers will be called.
        self._operation_callers = [operation_caller]
        self._lineage = [self]
        self._operation_model = operation_model

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def lineage(self):
        return self._lineage

    @lineage.setter
    def lineage(self, value):
        self._lineage = value

    @property
    def lineage_names(self):
        # Represents the lineage of a command in terms of command ``name``
        return [cmd.name for cmd in self.lineage]

    @property
    def arg_table(self):
        if self._arg_table is None:
            self._arg_table = self._create_argument_table()
        return self._arg_table

    def __call__(self, client_creator, args, parsed_globals):
        # We need to handle overriding required arguments before we create
        # the parser as the parser will parse the arguments and decide which
        # argument is required before we have a chance to modify the argument
        # table.
        self._handle_override_required_args(args)
        # Once we know we're trying to call a particular operation
        # of a service we can go ahead and load the parameters.
        operation_parser = self._create_operation_parser(self.arg_table)
        self._add_help(operation_parser)
        parsed_args, remaining = operation_parser.parse_known_args(args)
        if parsed_args.help == 'help':
            return self.create_help_command()(
                client_creator, remaining, parsed_globals)
        elif parsed_args.help:
            remaining.append(parsed_args.help)
        if remaining:
            raise UnknownArgumentError(
                "Unknown options: %s" % ', '.join(remaining))
        check_should_enable_pagination(self._arg_table,
                                       self._operation_model,
                                       parsed_args,
                                       parsed_globals)
        call_parameters = self._build_call_parameters(parsed_args,
                                                      self.arg_table)

        # The TLS verification value can be a boolean or a CA_BUNDLE path. This
        # is a little odd, but ultimately comes from the python HTTP requests
        # library we're using.
        tls_verification = parsed_globals.verify_tls
        ca_bundle = getattr(parsed_globals, 'ca_bundle', None)
        if parsed_globals.verify_tls and ca_bundle is not None:
            tls_verification = ca_bundle

        client = client_creator.create_client(
            self._operation_model.service_model.service_name,
            parsed_globals.endpoint_url,
            tls_verification,
            client_creator.context.get_credentials())
        return self._invoke_operation_callers(client,
                                              call_parameters,
                                              parsed_args,
                                              parsed_globals)

    def create_help_command(self):
        return OperationHelpCommand(
            operation_model=self._operation_model,
            arg_table=self.arg_table,
            name=self._name,
            command_lineage='.'.join(self.lineage_names))

    def _add_help(self, parser):
        # The 'help' output is processed a little differently from
        # the operation help because the arg_table has
        # CLIArguments for values.
        parser.add_argument('help', nargs='?')

    def _build_call_parameters(self, args, arg_table):
        # We need to convert the args specified on the command
        # line as valid **kwargs we can hand to botocore.
        service_params = {}
        # args is an argparse.Namespace object so we're using vars()
        # so we can iterate over the parsed key/values.
        parsed_args = vars(args)
        for arg_object in arg_table.values():
            py_name = arg_object.py_name
            if py_name in parsed_args:
                value = parsed_args[py_name]
                value = unpack_argument(arg_object, value)
                arg_object.add_to_params(service_params, value)
        # We run the ParamFileVisitor over the input data to resolve any
        # paramfile references in it.
        service_params = ParamFileVisitor().visit(
            service_params, self._operation_model.input_shape)
        return service_params

    def _create_argument_table(self):
        argument_table = OrderedDict()
        input_shape = self._operation_model.input_shape
        required_arguments = []
        arg_dict = {}
        if input_shape is not None:
            required_arguments = input_shape.required_members
            arg_dict = input_shape.members
        for arg_name, arg_shape in arg_dict.items():
            cli_arg_name = xform_name(arg_name, '-')
            arg_class = self.ARG_TYPES.get(arg_shape.type_name,
                                           self.DEFAULT_ARG_CLASS)
            is_required = arg_name in required_arguments
            arg_object = arg_class(
                name=cli_arg_name,
                argument_model=arg_shape,
                is_required=is_required,
                operation_model=self._operation_model,
                serialized_name=arg_name,
                no_paramfile=arg_shape.is_no_paramfile)
            arg_object.add_to_arg_table(argument_table)
        add_pagination_params(self._operation_model, argument_table)
        add_cli_input_json(self._operation_model, argument_table)
        add_generate_skeleton(self._operation_model, argument_table)
        return argument_table

    def _create_operation_parser(self, arg_table):
        return ArgTableArgParser(arg_table, service_name=self._parent_name,
                                 operation_name=self._name)

    def _handle_override_required_args(self, args):
        argument_table = self.arg_table
        for cli_name, cli_argument in argument_table.items():
            if isinstance(cli_argument, OverrideRequiredArgsArgument):
                cli_argument.override_required_args(argument_table, args)
                self._operation_callers.insert(0, cli_argument)

    def _invoke_operation_callers(self,
                                  client,
                                  call_parameters,
                                  parsed_args,
                                  parsed_globals):
        for operation_caller in self._operation_callers:
            if operation_caller.invoke(
                    client,
                    self._operation_model.name,
                    call_parameters,
                    parsed_args,
                    parsed_globals) is False:
                break
        return 0


class CLIOperationCaller(object):

    def invoke(self,
               client,
               operation_name,
               parameters,
               parsed_args,
               parsed_globals):
        py_operation_name = xform_name(operation_name)
        if client.can_paginate(py_operation_name) and parsed_globals.paginate:
            response = client.get_paginator(
                py_operation_name).paginate(**parameters)
        else:
            response = getattr(client, xform_name(operation_name))(**parameters)
        self._display_response(operation_name, response, parsed_globals)
        return True

    def _display_response(self, command_name, response, parsed_globals):
        output = parsed_globals.output
        if output is None:
            output = "json"
        formatter = get_formatter(output, parsed_globals)
        formatter(command_name, response)

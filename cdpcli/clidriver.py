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
import socket
import sys

from cdpcli import LIST_TYPE
from cdpcli import RELEASE
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
from cdpcli.config import Config
from cdpcli.endpoint import EndpointCreator
from cdpcli.endpoint import EndpointResolver
from cdpcli.exceptions import ExtensionImportError
from cdpcli.exceptions import InvalidConfiguredFormFactor
from cdpcli.exceptions import ProfileNotFound
from cdpcli.exceptions import WrongOpFormFactorError
from cdpcli.exceptions import WrongSvcFormFactorError
from cdpcli.extensions.arguments import OverrideRequiredArgsArgument
from cdpcli.extensions.cliinputjson import add_cli_input_json
from cdpcli.extensions.configure.classify import ClassifyDeployment
from cdpcli.extensions.configure.classify import DeploymentType
from cdpcli.extensions.configure.configure import ConfigureCommand
from cdpcli.extensions.generatecliskeleton import add_generate_skeleton
from cdpcli.extensions.interactivelogin import LoginCommand
from cdpcli.extensions.logout import LogoutCommand
from cdpcli.extensions.paginate import add_pagination_params
from cdpcli.extensions.paginate import check_should_enable_pagination
from cdpcli.extensions.refdoc import RefdocCommand
from cdpcli.formatter import get_formatter
from cdpcli.help import OperationHelpCommand
from cdpcli.help import ProviderHelpCommand
from cdpcli.help import ServiceHelpCommand
from cdpcli.loader import Loader
from cdpcli.model import ServiceModel
from cdpcli.paramfile import ParamFileVisitor
from cdpcli.paramformfactor import ParamFormFactorVisitor
from cdpcli.parser import ResponseParserFactory
from cdpcli.retryhandler import create_retry_handler
from cdpcli.translate import build_retry_config
from cdpcli.utils import get_extension_registers
import urllib3.util.connection as urllib3_connection

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
        self._context = Context()
        self._client_creator = ClientCreator(self._loader,
                                             self._context,
                                             self._endpoint_creator,
                                             self._user_agent_header,
                                             self._response_parser_factory,
                                             self._retryhandler)
        self._form_factor = None

    def main(self, args=None):
        if args is None:
            args = sys.argv[1:]
        parser = self._create_parser()
        command_table = self._get_command_table()
        if len(args) == 0 or (len(args) == 1 and args[0] == '--help'):
            args = ['help']
        parsed_args, remaining = parser.parse_known_args(args)
        try:
            self._form_factor = self._get_form_factor(parsed_args)
            self._handle_top_level_args(parsed_args)
            self._filter_command_table_for_form_factor()
            self._warn_for_old_python()
            self._warn_for_non_public_release()
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
        LoginCommand.add_command(commands)
        LogoutCommand.add_command(commands)
        RefdocCommand.add_command(commands)
        commands = OrderedDict(sorted(commands.items()))
        return commands

    def _get_form_factor(self, parsed_args):
        if parsed_args.command == 'refdoc':
            # Do not filter out any command if it is to generate help documents.
            return None

        # Find the form factor based on:
        # 1. the form factor explicitly specified by --form-factor, or else
        # 2. the configured form factor, or else
        # 3. the explicit endpoint URL, or else
        # 4. the configured CDP endpoint URL.
        if parsed_args.form_factor:
            form_factor = parsed_args.form_factor
        else:
            try:
                form_factor = self._context.get_scoped_config().get('form_factor', None)
            except ProfileNotFound:
                form_factor = None
            valid_form_factors = [dt.value for dt in list(DeploymentType)]
            if form_factor and form_factor not in valid_form_factors:
                raise InvalidConfiguredFormFactor(
                    form_factor=form_factor,
                    valid_form_factors=valid_form_factors)
            if not form_factor:
                endpoint_url = parsed_args.endpoint_url
                if not endpoint_url:
                    try:
                        endpoint_url = self._context.get_scoped_config(). \
                            get(EndpointResolver.CDP_ENDPOINT_URL_KEY_NAME, None)
                    except ProfileNotFound:
                        endpoint_url = None
                form_factor = \
                    ClassifyDeployment(endpoint_url).get_deployment_type().value
        LOG.debug("Current form factor is {}".format(form_factor))
        return form_factor

    def _filter_command_table_for_form_factor(self):
        """
        Replaces services and operations in the command table that do not apply
        to the current form factor with stubs that error out when called.
        """
        form_factor = self._form_factor

        if form_factor is None:
            # Do not filter out any command if form factor is None.
            # For example: 'refdoc' command.
            return

        for command in list(self._command_table.keys()):
            try:
                # If the service does not apply to the current form factor,
                # filter it out.
                service_model = self._command_table[command].service_model
                service_form_factors = service_model.form_factors
                if form_factor not in service_form_factors:
                    self._command_table[command] = \
                        FilteredServiceCommand(self, command, form_factor,
                                               service_form_factors)
                else:
                    for operation_name in service_model.operation_names:
                        # If the operation does not apply to the current form
                        # factor, filter it out.
                        operation_model = service_model.operation_model(operation_name)
                        operation_form_factors = operation_model.form_factors
                        if not operation_form_factors:
                            operation_form_factors = service_form_factors
                        if form_factor not in operation_form_factors:
                            self._command_table[command]. \
                                filter_operation(operation_name, form_factor,
                                                 operation_form_factors)

            except AttributeError:
                # not a service model, so available in all form factors
                pass

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
        service_data['paths'] = OrderedDict(sorted(service_data.get('paths', {}).items()))
        return ServiceModel(service_data, service_name=service_name)

    def get_form_factor(self):
        return self._form_factor

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
            cli_type_name=option_params.get('type'),
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
            self._setup_logger(logging.WARNING)

        if args.force_ipv4:
            # Based on SO /a/46972341
            LOG.debug("Forcing IPv4 connections only")

            def _allowed_gai_family():
                return socket.AF_INET
            urllib3_connection.allowed_gai_family = _allowed_gai_family

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

    def _warn_for_old_python(self):
        if sys.version_info[0] < 3 or \
                (sys.version_info[0] == 3 and sys.version_info[1] < 6):
            LOG.warn('You are running the CDP CLI under Python %s. The CDP CLI '
                     'now requires Python 3.6 or higher. Please upgrade now to '
                     'avoid CLI errors.', sys.version)

    def _warn_for_non_public_release(self):
        if RELEASE != 'PUBLIC':
            if RELEASE == 'INTERNAL':
                article = 'an'
            else:
                article = 'a'
            LOG.warn('You are running {0} {1} release of the CDP CLI, which '
                     'has different capabilities from the standard public '
                     'release. Find the public release at: '
                     'https://pypi.org/project/cdpcli/'.format(article, RELEASE))


class ServiceCommand(CLICommand):
    """
    A top-level CLI command, corresponding to an API service.
    """

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
                clidriver=self._clidriver,
                name=cli_name,
                parent_name=self._name,
                operation_model=operation_model,
                operation_caller=CLIOperationCaller())
        register_ext, register_cmd = get_extension_registers(self._name)
        if register_cmd is not None:
            register_cmd(self._clidriver, service_model, command_table)
        self._add_lineage(command_table)
        return command_table

    def filter_operation(self, operation_name, form_factor, operation_form_factors):
        """
        Replace the named operation in this command's command table with a
        filtered one.
        """
        command_table = self._get_command_table()
        cli_name = xform_name(operation_name, '-')
        command_table[cli_name] = FilteredServiceOperation(
            clidriver=self._clidriver,
            name=cli_name,
            parent_name=self._name,
            form_factor=form_factor,
            operation_form_factors=operation_form_factors)

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


class FilteredServiceCommand(ServiceCommand):
    """
    A stub service command that fails when run due to being under the wrong
    CLI form factor.
    """

    def __init__(self, clidriver, name, form_factor, service_form_factors):
        super().__init__(clidriver, name)
        self._clidriver = clidriver
        self._name = name
        self._form_factor = form_factor
        self._service_form_factors = service_form_factors

    def __call__(self, client_creator, args, parsed_globals):
        raise WrongSvcFormFactorError(
            service_name=self._name,
            form_factor=self._form_factor,
            service_form_factors=', '.join(self._service_form_factors))


class ServiceOperation(object):
    ARG_TYPES = {
        LIST_TYPE: ListArgument,
        'boolean': BooleanArgument,
    }
    DEFAULT_ARG_CLASS = CLIArgument

    def __init__(self, clidriver, name, parent_name, operation_caller, operation_model):
        self._clidriver = clidriver
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
        self._UNDOCUMENTED = self._operation_model.is_deprecated

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
        # Handle extensions first, so OverrideRequiredArgs (CliInputJson,
        # GenerateCliSkeleton, etc) could have a chance to run.
        self._handle_extensions()
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
                                                      self.arg_table,
                                                      self._clidriver.get_form_factor(),
                                                      parsed_globals)
        return self._invoke_operation_callers(client_creator,
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

    def _build_call_parameters(self, args, arg_table, form_factor, parsed_globals):
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
                value = unpack_argument(arg_object, value, parsed_globals)
                arg_object.add_to_params(service_params, value)
        # We run the ParamFormFactorVisitor over the input data to check
        # the form factor for arguments.
        ParamFormFactorVisitor(form_factor).visit(
            service_params, self._operation_model.input_shape)
        # We run the ParamFileVisitor over the input data to resolve any
        # paramfile references in it.
        service_params = ParamFileVisitor(parsed_globals).visit(
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

    def _handle_extensions(self):
        if self._operation_model.extensions:
            # Iterate in reversed order to keep the execution order:
            # First extension should run first.
            for ext_name in reversed(self._operation_model.extensions):
                register_ext, register_cmd = get_extension_registers(ext_name)
                if register_ext is None:
                    raise ExtensionImportError(ext_name=ext_name, err='Not Found')
                register_ext(self._operation_callers, self._operation_model)

    def _invoke_operation_callers(self,
                                  client_creator,
                                  call_parameters,
                                  parsed_args,
                                  parsed_globals):
        def _create_client(service_name):
            # The TLS verification value can be a boolean or a CA_BUNDLE path. This
            # is a little odd, but ultimately comes from the python HTTP requests
            # library we're using.
            tls_verification = parsed_globals.verify_tls
            ca_bundle = getattr(parsed_globals, 'ca_bundle', None)
            if parsed_globals.verify_tls and ca_bundle is not None:
                tls_verification = ca_bundle

            # Retrieve values passed for extra client configuration.
            config_kwargs = {}
            if parsed_globals.read_timeout is not None:
                config_kwargs['read_timeout'] = int(parsed_globals.read_timeout)
            if parsed_globals.connect_timeout is not None:
                config_kwargs['connect_timeout'] = int(parsed_globals.connect_timeout)
            config = Config(**config_kwargs)

            client = client_creator.create_client(
                service_name,
                parsed_globals.endpoint_url,
                parsed_globals.cdp_region,
                tls_verification,
                client_creator.context.get_credentials(parsed_globals),
                client_config=config)
            return client

        for operation_caller in self._operation_callers:
            # Create a new client for each operation_caller because parsed_args and
            # parsed_globals could be changed in each iteration.
            if operation_caller.invoke(
                    _create_client,
                    self._operation_model,
                    call_parameters,
                    parsed_args,
                    parsed_globals) is False:
                break
        return 0


class FilteredServiceOperation(ServiceOperation):
    """
    A stub service operation that fails when run due to being under the wrong
    CLI form factor.
    """

    def __init__(self, clidriver, name, parent_name, form_factor, operation_form_factors):
        super().__init__(clidriver, name, parent_name,
                         operation_caller=None, operation_model=None)
        self._form_factor = form_factor
        self._operation_form_factors = operation_form_factors

    def __call__(self, client_creator, args, parsed_globals):
        raise WrongOpFormFactorError(
            operation_name=self._name,
            service_name=self._parent_name,
            form_factor=self._form_factor,
            operation_form_factors=', '.join(self._operation_form_factors))


class CLIOperationCaller(object):

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        service_name = operation_model.service_model.service_name
        operation_name = operation_model.name
        client = client_creator(service_name)
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

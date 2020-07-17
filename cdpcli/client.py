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

import copy
import os
import uuid

from cdpcli import credentials, DEFAULT_PROFILE_NAME
from cdpcli import xform_name
from cdpcli.auth import Ed25519v1Auth
from cdpcli.auth import RSAv1Auth
from cdpcli.cdprequest import prepare_request_dict
from cdpcli.configloader import load_config, raw_config_parse
from cdpcli.endpoint import DEFAULT_TIMEOUT
from cdpcli.exceptions import ClientError, ConfigNotFound, ProfileNotFound
from cdpcli.exceptions import OperationNotPageableError
from cdpcli.model import ServiceModel
from cdpcli.paginate import Paginator
from cdpcli.serialize import create_serializer
from cdpcli.signers import RequestSigner
from cdpcli.utils import get_service_module_name

CDP_REQUEST_ID_HEADER = 'x-altus-request-id'


class ClientCreator(object):

    def __init__(self,
                 loader,
                 context,
                 endpoint_creator,
                 user_agent_header,
                 response_parser_factory,
                 retryhandler):
        self._loader = loader
        self._context = context
        self._endpoint_creator = endpoint_creator
        self._user_agent_header = user_agent_header
        self._response_parser_factory = response_parser_factory
        self._retryhandler = retryhandler

    @property
    def context(self):
        return self._context

    def create_client(self,
                      service_name,
                      endpoint_url,
                      tls_verification,
                      credentials):
        service_model = self._load_service_model(service_name)
        cls = self._create_client_class(service_name, service_model)
        client_args = self._get_client_args(service_model,
                                            endpoint_url,
                                            tls_verification,
                                            credentials)
        return cls(**client_args)

    def _load_service_model(self, service_name):
        service_data = self._loader.load_service_data(service_name)
        return ServiceModel(service_data, service_name=service_name)

    def _create_client_class(self, service_name, service_model):
        class_attributes = self._create_methods(service_model)
        py_name_to_operation_name = self._create_name_mapping(service_model)
        class_attributes['_PY_TO_OP_NAME'] = py_name_to_operation_name
        bases = [BaseClient]
        class_name = get_service_module_name(service_model)
        cls = type(str(class_name), tuple(bases), class_attributes)
        return cls

    def _get_client_args(self,
                         service_model,
                         endpoint_url,
                         tls_verification,
                         credentials):
        serializer = create_serializer()
        timeout = (DEFAULT_TIMEOUT, DEFAULT_TIMEOUT)
        endpoint = self._endpoint_creator.create_endpoint(
            service_model,
            endpoint_url,
            self._context.get_scoped_config(),
            self._response_parser_factory,
            tls_verification,
            timeout,
            self._retryhandler)
        if Ed25519v1Auth.detect_private_key(credentials.private_key):
            auth_method = Ed25519v1Auth.AUTH_METHOD_NAME
        else:
            auth_method = RSAv1Auth.AUTH_METHOD_NAME
        additional_headers = dict()
        request_headers_string = self._context.get_scoped_config().get(
            'request_headers', '')
        for header_string in request_headers_string.split(','):
            header_string = header_string.strip()
            if header_string == '':
                continue
            name, value = header_string.split('=', 1)
            if value == 'generate::uuid':
                value = str(uuid.uuid4())
            additional_headers[name] = value
        signer = RequestSigner(auth_method, credentials)
        return {
            'serializer': serializer,
            'service_model': service_model,
            'endpoint': endpoint,
            'loader': self._loader,
            'user_agent_header': self._user_agent_header,
            'additional_headers': additional_headers,
            'request_signer': signer
        }

    def _create_methods(self, service_model):
        op_dict = {}
        for operation_name in service_model.operation_names:
            py_operation_name = xform_name(operation_name)
            op_dict[py_operation_name] = self._create_api_method(
                py_operation_name, operation_name, service_model)
        return op_dict

    def _create_name_mapping(self, service_model):
        mapping = {}
        for operation_name in service_model.operation_names:
            py_operation_name = xform_name(operation_name)
            mapping[py_operation_name] = operation_name
        return mapping

    def _create_api_method(self, py_operation_name, operation_name, service_model):
        def _api_call(self, *args, **kwargs):
            if args:
                raise TypeError(
                    "%s() only accepts keyword arguments." % py_operation_name)
            return self._make_api_call(operation_name, kwargs)

        _api_call.__name__ = str(py_operation_name)
        return _api_call


class BaseClient(object):

    # This is actually reassigned with the py->op_name mapping when the client
    # creator creates the subclass.  This value is used because calls such as
    # client.get_paginator('list_objects') use the snake_case name, but we need
    # to know the ListObjects form. The xform_name() function does the forward
    # mapping for ListObjects->list_objects conversion, but this holds the
    # reverse mapping.
    _PY_TO_OP_NAME = {}

    def __init__(self,
                 serializer,
                 endpoint,
                 service_model,
                 loader,
                 user_agent_header,
                 additional_headers,
                 request_signer):
        self._serializer = serializer
        self._endpoint = endpoint
        self._loader = loader
        self._user_agent_header = user_agent_header
        self._additional_headers = additional_headers
        self._request_signer = request_signer
        self.meta = ClientMeta(service_model, endpoint.host, self._PY_TO_OP_NAME)

    def can_paginate(self, operation_name):
        actual_operation_name = self._PY_TO_OP_NAME[operation_name]
        return self._service_model.operation_model(actual_operation_name).can_paginate

    def get_paginator(self, operation_name):
        if not self.can_paginate(operation_name):
            raise OperationNotPageableError(operation_name=operation_name)
        actual_operation_name = self._PY_TO_OP_NAME[operation_name]
        operation_model = self._service_model.operation_model(actual_operation_name)
        method = getattr(self, operation_name)
        return Paginator(method, operation_model)

    @property
    def _service_model(self):
        return self.meta.service_model

    def _make_api_call(self, operation_name, api_params):
        operation_model = self._service_model.operation_model(operation_name)
        request_dict = self._convert_to_request_dict(
            api_params, operation_model)
        http, parsed_response = self._endpoint.make_request(
            operation_model, request_dict, self._request_signer)
        if http.status_code >= 300:
            raise ClientError(
                parsed_response,
                operation_name,
                self._service_model.service_name,
                http.status_code,
                http.headers.get(CDP_REQUEST_ID_HEADER, 'Unknown'))
        else:
            return parsed_response

    def _convert_to_request_dict(self, api_params, operation_model):
        request_dict = self._serializer.serialize_to_request(
            api_params, operation_model)
        prepare_request_dict(request_dict,
                             endpoint_url=self._endpoint.host,
                             user_agent_header=self._user_agent_header,
                             additional_headers=self._additional_headers)
        return request_dict


class ClientMeta(object):

    def __init__(self, service_model, endpoint_url, method_to_api_mapping):
        self._service_model = service_model
        self._endpoint_url = endpoint_url
        self._method_to_api_mapping = method_to_api_mapping

    @property
    def service_model(self):
        return self._service_model

    @property
    def endpoint_url(self):
        return self._endpoint_url

    @property
    def method_to_api_mapping(self):
        return self._method_to_api_mapping


class Context(object):
    """
    The Context object exposes configuration information and credentials into a
    single, easy-to-use object.

    """

    #: A default dictionary that maps the logical names for context variables
    #: to the specific environment variables and configuration file names
    #: that contain the values for these variables.
    #: When creating a new Context object, you can pass in your own dictionary
    #: to remap the logical names or to add new logical names.  You can then
    #: get the current value for these variables by using the
    #: ``get_config_variable`` method of the :class:`Context` #: class.
    #: These form the keys of the dictionary.  The values in the dictionary
    #: are tuples of (<config_name>, <environment variable>, <default value>,
    #: <conversion func>).
    #: The conversion func is a function that takes the configuration value
    #: as an argument and returns the converted value.  If this value is
    #: None, then the configuration value is returned unmodified.  This
    #: conversion function can be used to type convert config values to
    #: values other than the default values of strings.
    #: The ``profile`` and ``config_file`` variables should always have a
    #: None value for the first entry in the tuple because it doesn't make
    #: sense to look inside the config file for the location of the config
    #: file or for the default profile to use.
    #: The ``config_name`` is the name to look for in the configuration file,
    #: the ``env var`` is the OS environment variable (``os.environ``) to
    #: use, and ``default_value`` is the value to use if no value is otherwise
    #: found.
    CONTEXT_VARIABLES = {
        # logical:  config_file, env_var, default_value, conversion_func
        'profile': (None, ['CDP_DEFAULT_PROFILE', 'CDP_PROFILE'], None, None),
        'config_file': (None, 'CDP_CONFIG_FILE', '~/.cdp/config', None),
        'ca_bundle': ('ca_bundle', 'CDP_CA_BUNDLE', None, None),
        'api_versions': ('api_versions', None, {}, None),
        # This is the legacy json configuration file
        'auth_config': (None, None, None, None),
        # This is the shared credentials file.
        'credentials_file': (None, 'CDP_SHARED_CREDENTIALS_FILE',
                             '~/.cdp/credentials', None),
    }

    def __init__(self, profile=None):
        self.context_var_map = copy.copy(self.CONTEXT_VARIABLES)
        # The _profile attribute is just used to cache the value
        # of the current profile to avoid going through the normal
        # config lookup process each access time.
        self._profile = None
        self._config = None
        self._profile_map = None
        self._credentials = None
        # This is a dict that stores per context specific config variable
        # overrides via set_config_variable().
        self._context_instance_vars = {}
        if profile is not None:
            self._context_instance_vars['profile'] = profile
        self._client_config = None

    @property
    def profile(self):
        """
        Returns the profile if one was set. Can be None.
        """
        if self._profile is None:
            profile = self.get_config_variable('profile')
            self._profile = profile
        return self._profile

    @property
    def effective_profile(self):
        """
        Returns the effective profile, cannot be None. If no profile was
        set the default profile is returned.
        """
        profile_name = self.profile
        if profile_name is None:
            profile_name = DEFAULT_PROFILE_NAME
        return profile_name

    @property
    def full_config(self):
        """Return the parsed config file.
        The ``get_config`` method returns the config associated with the
        specified profile.  This property returns the contents of the
        **entire** config file.
        """
        if self._config is None:
            try:
                config_file = self.get_config_variable('config_file')
                self._config = load_config(config_file)
            except ConfigNotFound:
                self._config = {'profiles': {}}
            try:
                # Now we need to inject the profiles from the
                # credentials file.  We don't actually need the values
                # in the creds file, only the profile names so that we
                # can validate the user is not referring to a nonexistent
                # profile.
                cred_file = self.get_config_variable('credentials_file')
                cred_profiles = raw_config_parse(cred_file)
                for profile in cred_profiles:
                    cred_vars = cred_profiles[profile]
                    if profile not in self._config['profiles']:
                        self._config['profiles'][profile] = cred_vars
                    else:
                        self._config['profiles'][profile].update(cred_vars)
            except ConfigNotFound:
                pass
        return self._config

    def _build_profile_map(self):
        # This will build the profile map if it has not been created,
        # otherwise it will return the cached value.  The profile map
        # is a list of profile names, to the config values for the profile.
        if self._profile_map is None:
            self._profile_map = self.full_config['profiles']
        return self._profile_map

    def get_config_variable(self, logical_name,
                            methods=('instance', 'env', 'config')):
        """
        Retrieve the value associated with the specified logical_name
        from the environment or the config file.  Values found in the
        environment variable take precedence of values found in the
        config file.  If no value can be found, a None will be returned.

        :type logical_name: str
        :param logical_name: The logical name of the context variable
            you want to retrieve.  This name will be mapped to the
            appropriate environment variable name for this context as
            well as the appropriate config file entry.

        :type methods: tuple
        :param methods: Defines which methods will be used to find
            the variable value.  By default, all available methods
            are tried but you can limit which methods are used
            by supplying a different value to this parameter.
            Valid choices are: instance|env|config

        :returns: value of variable or None if not defined.

        """
        # Handle all the short circuit special cases first.
        if logical_name not in self.context_var_map:
            return
        # Do the actual lookups.  We need to handle
        # 'instance', 'env', and 'config' locations, in that order.
        value = None
        var_config = self.context_var_map[logical_name]
        if self._found_in_instance_vars(methods, logical_name):
            return self._context_instance_vars[logical_name]
        elif self._found_in_env(methods, var_config):
            value = self._retrieve_from_env(var_config[1], os.environ)
        elif self._found_in_config_file(methods, var_config):
            value = self.get_scoped_config()[var_config[0]]
        if value is None:
            value = var_config[2]
        if var_config[3] is not None:
            value = var_config[3](value)
        return value

    def _found_in_instance_vars(self, methods, logical_name):
        if 'instance' in methods:
            return logical_name in self._context_instance_vars
        return False

    def _found_in_env(self, methods, var_config):
        return (
            'env' in methods and
            var_config[1] is not None and
            self._retrieve_from_env(var_config[1], os.environ) is not None)

    def _found_in_config_file(self, methods, var_config):
        if 'config' in methods and var_config[0] is not None:
            return var_config[0] in self.get_scoped_config()
        return False

    def _retrieve_from_env(self, names, environ):
        # We need to handle the case where names is either
        # a single value or a list of variables.
        if not isinstance(names, list):
            names = [names]
        for name in names:
            if name in environ:
                return environ[name]
        return None

    def get_scoped_config(self):
        """
        Returns the config values from the config file scoped to the current
        profile.

        The configuration data is loaded **only** from the config file.
        It does not resolve variables based on different locations
        (e.g. first from the context instance, then from environment
        variables, then from the config file).  If you want this lookup
        behavior, use the ``get_config_variable`` method instead.

        Note that this configuration is specific to a single profile (the
        ``profile`` session variable).

        If the ``profile`` session variable is set and the profile does
        not exist in the config file, a ``ProfileNotFound`` exception
        will be raised.
        """
        profile_name = self.get_config_variable('profile')
        profile_map = self._build_profile_map()
        # If a profile is not explicitly set return the default
        # profile config or an empty config dict if we don't have
        # a default profile.
        if profile_name is None:
            return profile_map.get('default', {})
        elif profile_name not in profile_map:
            # Otherwise if they specified a profile, it has to
            # exist (even if it's the default profile) otherwise
            # we complain.
            raise ProfileNotFound(profile=profile_name)
        else:
            return profile_map[profile_name]

    def set_config_variable(self, logical_name, value):
        """Set a configuration variable to a specific value.

        By using this method, you can override the normal lookup
        process used in ``get_config_variable`` by explicitly setting
        a value.  Subsequent calls to ``get_config_variable`` will
        use the ``value``.  This gives you per-session specific
        configuration values.
        ::
            >>> # Assume logical name 'foo' maps to env var 'FOO'
            >>> os.environ['FOO'] = 'myvalue'
            >>> s.get_config_variable('foo')
            'myvalue'
            >>> s.set_config_variable('foo', 'othervalue')
            >>> s.get_config_variable('foo')
            'othervalue'
        """
        self._context_instance_vars[logical_name] = value

    def get_credentials(self):
        """
        Return the :class:`botocore.credential.Credential` object
        associated with this session.  If the credentials have not
        yet been loaded, this will attempt to load them.  If they
        have already been loaded, this will return the cached
        credentials.

        """
        if self._credentials is None:
            resolver = credentials.create_credential_resolver(self)
            return resolver.load_credentials()

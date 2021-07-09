# Copyright (c) 2021 Cloudera, Inc. All rights reserved.
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

from cdpcli.clidriver import ServiceOperation
from cdpcli.client import ClientCreator
from cdpcli.exceptions import ExtensionImportError
from cdpcli.model import ServiceModel
from cdpcli.parser import ResponseParserFactory
from mock import Mock, patch
from tests import unittest
import yaml


MODEL_DIR = os.path.dirname(os.path.abspath(__file__))


class TestOperationExtension(unittest.TestCase):

    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')
        self.loader = Mock()
        self.loader.load_service_data.return_value = self.model
        self.endpoint = Mock()
        self.endpoint.host = 'http://thunderhead.cloudera.altus.cloudera.com'
        self.endpoint.make_request.return_value = (Mock(status_code=200), {})
        self.endpoint_creator = Mock()
        self.endpoint_creator.create_endpoint.return_value = self.endpoint
        self.context = Mock()
        self.context.get_credentials.return_value = Mock(private_key="A private key")
        self.context.get_scoped_config.return_value = {}
        self.client_creator = ClientCreator(self.loader,
                                            self.context,
                                            self.endpoint_creator,
                                            'user-agent-header',
                                            ResponseParserFactory(),
                                            Mock())
        self.parsed_globals = Mock(connect_timeout=1, read_timeout=1)

    def test_invalid_operation_extension(self):
        operation_caller = Mock()
        service_operation = ServiceOperation(
            'create-director',
            'directors',
            operation_caller,
            self.service_model.operation_model('createDirector'))
        with self.assertRaises(ExtensionImportError) as context:
            service_operation(self.client_creator, [], self.parsed_globals)
        self.assertTrue(
            'Failed to import CLI extension \'invalid\': '
            'No module named \'cdpcli.extensions.invalid\'' in str(context.exception))
        self.assertFalse(operation_caller.invoke.called)

    def test_operation_replace_by_extension(self):
        extension_caller = Mock()
        extension_caller.invoke.return_value = False
        test_obj = Mock()
        test_obj.register = lambda operation_callers, operation_model: \
            operation_callers.insert(0, extension_caller)
        module_obj = {'cdpcli.extensions.test': test_obj}

        with patch('importlib.import_module', new=module_obj.get):
            operation_caller = Mock()
            service_operation = ServiceOperation(
                'upload-data',
                'directors',
                operation_caller,
                self.service_model.operation_model('uploadData'))
            service_operation(self.client_creator, [], self.parsed_globals)
            self.assertFalse(operation_caller.invoke.called)

        self.assertTrue(extension_caller.invoke.called)
        self.assertEquals(1, extension_caller.invoke.call_count)

    def test_operation_extensions_injected(self):
        extension_call_list = []
        extension_caller_1 = Mock()
        extension_caller_1.invoke.side_effect = lambda *args, **kwargs: \
            extension_call_list.append(1)
        extension_caller_1.invoke.return_value = True
        extension_caller_2 = Mock()
        extension_caller_2.invoke.side_effect = lambda *args, **kwargs: \
            extension_call_list.append(2)
        extension_caller_2.invoke.return_value = True
        test_obj_1 = Mock()
        test_obj_1.register = lambda operation_callers, operation_model: \
            operation_callers.insert(0, extension_caller_1)
        test_obj_2 = Mock()
        test_obj_2.register = lambda operation_callers, operation_model: \
            operation_callers.insert(0, extension_caller_2)
        module_obj = {'cdpcli.extensions.test1': test_obj_1,
                      'cdpcli.extensions.test2': test_obj_2}

        with patch('importlib.import_module', new=module_obj.get):
            operation_caller = Mock()
            service_operation = ServiceOperation(
                'inject-data',
                'directors',
                operation_caller,
                self.service_model.operation_model('injectData'))
            service_operation(self.client_creator, [], self.parsed_globals)
            self.assertTrue(operation_caller.invoke.called)
            self.assertEquals(1, operation_caller.invoke.call_count)

        self.assertTrue(extension_caller_1.invoke.called)
        self.assertEquals(1, extension_caller_1.invoke.call_count)
        self.assertTrue(extension_caller_2.invoke.called)
        self.assertEquals(1, extension_caller_2.invoke.call_count)
        self.assertEquals([1, 2], extension_call_list)

    def test_operation_extensions_update_parsed_globals(self):
        # noinspection PyShadowingNames
        def _invoke_to_update_parsed_globals(client_creator,
                                             service_name,
                                             operation_name,
                                             parameters,
                                             parsed_args,
                                             parsed_globals):
            client_creator(service_name)
            parsed_globals.endpoint_url = 'https://test.com/'
            parsed_globals.access_token = 'Bearer ABC'
            return True

        # noinspection PyShadowingNames
        def _mock_operation_caller_invoke(client_creator,
                                          service_name,
                                          operation_name,
                                          parameters,
                                          parsed_args,
                                          parsed_globals):
            client_creator(service_name)

        extension_caller = Mock()
        extension_caller.invoke = _invoke_to_update_parsed_globals
        test_obj = Mock()
        test_obj.register = lambda operation_callers, operation_model: \
            operation_callers.insert(0, extension_caller)
        module_obj = {'cdpcli.extensions.test': test_obj}

        with patch('importlib.import_module', new=module_obj.get):
            operation_caller = Mock()
            operation_caller.invoke.side_effect = _mock_operation_caller_invoke
            service_operation = ServiceOperation(
                'upload-data',
                'directors',
                operation_caller,
                self.service_model.operation_model('uploadData'))
            service_operation(self.client_creator, [], self.parsed_globals)
            self.assertTrue(operation_caller.invoke.called)
            self.assertEquals(1, operation_caller.invoke.call_count)
            args, kargs = operation_caller.invoke.call_args
            client_creator, service_name, operation_name, parameters, parsed_args, \
                parsed_globals = args
            self.assertEquals('uploadData', operation_name)
            self.assertEquals('https://test.com/', parsed_globals.endpoint_url)
            self.assertEquals('Bearer ABC', parsed_globals.access_token)
            args, kargs = self.endpoint_creator.create_endpoint.call_args
            service_model, endpoint_url, scoped_config, response_parser_factory, \
                tls_verification, timeout, retry_handler = args
            self.assertEquals('https://test.com/', endpoint_url)

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

from cdpcli.exceptions import WorkloadServiceDiscoveryError
from cdpcli.extensions.workload import register, WorkloadServiceDiscovery
from mock import Mock
from tests import unittest


class TestWorkloadExtension(unittest.TestCase):

    def test_workload_extension_register(self):
        operation_callers = [Mock()]
        register(operation_callers, None)
        self.assertEqual(2, len(operation_callers))
        self.assertIsInstance(operation_callers[0], WorkloadServiceDiscovery)

    def test_service_discovery_df(self):
        response = {'endpointUrl': 'workload-url', 'token': 'workload-access-token'}
        generate_workload_auth_token = Mock(return_value=response)
        iam_client = Mock()
        iam_client.generate_workload_auth_token = generate_workload_auth_token
        client_creator = Mock(return_value=iam_client)
        parameters = {'environmentCrn': 'mockCrn'}
        parsed_args = Mock()
        parsed_globals = Mock(endpoint_url=None, access_token=None)

        workload_service_discovery = WorkloadServiceDiscovery()
        ret = workload_service_discovery.invoke(client_creator,
                                                'df-workload',
                                                'mockOp',
                                                parameters,
                                                parsed_args,
                                                parsed_globals)
        self.assertTrue(ret)

        self.assertTrue(client_creator.called)
        args, kargs = client_creator.call_args
        service_name, = args
        self.assertEqual('iam', service_name)

        self.assertTrue(generate_workload_auth_token.called)
        args, kargs = generate_workload_auth_token.call_args
        self.assertEqual({'workloadName': 'DF', 'environmentCrn': 'mockCrn'}, kargs)

        self.assertEqual('workload-url', parsed_globals.endpoint_url)
        self.assertEqual('workload-access-token', parsed_globals.access_token)

    def test_service_discovery_no_response(self):
        iam_client = Mock()
        iam_client.generate_workload_auth_token = Mock(return_value={})
        client_creator = Mock(return_value=iam_client)
        parameters = {'environmentCrn': 'mockCrn'}
        parsed_args = Mock()
        parsed_globals = Mock(endpoint_url=None, access_token=None)

        workload_service_discovery = WorkloadServiceDiscovery()
        ret = workload_service_discovery.invoke(client_creator,
                                                'df-workload',
                                                'mockOp',
                                                parameters,
                                                parsed_args,
                                                parsed_globals)
        self.assertTrue(ret)
        self.assertTrue(iam_client.generate_workload_auth_token.called)
        self.assertIsNone(parsed_globals.endpoint_url)
        self.assertIsNone(parsed_globals.access_token)

    def test_service_discovery_skip_by_access_token(self):
        client_creator = Mock()
        parsed_globals = Mock()
        parsed_globals.endpoint_url = None
        parsed_globals.access_token = 'Bearer ABC'
        workload_service_discovery = WorkloadServiceDiscovery()
        workload_service_discovery.invoke(client_creator,
                                          'df-workload',
                                          'mockOp',
                                          {'environmentCrn': 'mockCrn'},
                                          Mock(),
                                          parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIsNone(parsed_globals.endpoint_url)

    def test_service_discovery_no_service_name(self):
        client_creator = Mock()
        parsed_globals = Mock(endpoint_url=None, access_token=None)
        workload_service_discovery = WorkloadServiceDiscovery()

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              None,
                                              'mockOp',
                                              {'environmentCrn': 'mockCrn'},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: Missing service name.',
                      str(context.exception))

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              '',
                                              'mockOp',
                                              {'environmentCrn': 'mockCrn'},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: Missing service name.',
                      str(context.exception))

    def test_service_discovery_unknown_service_name(self):
        client_creator = Mock()
        parsed_globals = Mock(endpoint_url=None, access_token=None)
        workload_service_discovery = WorkloadServiceDiscovery()

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              'invalid',
                                              'mockOp',
                                              {'environmentCrn': 'mockCrn'},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: '
                      'Unknown service name \'invalid\'.',
                      str(context.exception))

    def test_service_discovery_no_environment_crn(self):
        client_creator = Mock()
        parsed_globals = Mock(endpoint_url=None, access_token=None)
        workload_service_discovery = WorkloadServiceDiscovery()

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              'df-workload',
                                              'mockOp',
                                              {},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: Missing environment CRN.',
                      str(context.exception))

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              'df-workload',
                                              'mockOp',
                                              {'environmentCrn': ''},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: Missing environment CRN.',
                      str(context.exception))

        with self.assertRaises(WorkloadServiceDiscoveryError) as context:
            workload_service_discovery.invoke(client_creator,
                                              'df-workload',
                                              'mockOp',
                                              {'environmentCrn': None},
                                              Mock(),
                                              parsed_globals)
        self.assertFalse(client_creator.called)
        self.assertIn('Workload service-discovery error: Missing environment CRN.',
                      str(context.exception))

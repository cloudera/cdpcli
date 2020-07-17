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

import collections
import os

from cdpcli.model import ArrayShape
from cdpcli.model import DenormalizedStructureBuilder
from cdpcli.model import InvalidShapeError
from cdpcli.model import MapShape
from cdpcli.model import NoShapeFoundError
from cdpcli.model import ObjectShape
from cdpcli.model import OperationNotFoundError
from cdpcli.model import ServiceModel
from cdpcli.model import ShapeResolver
from tests import unittest
import yaml

MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'model')


class TestServiceModel(unittest.TestCase):

    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')

    def test_service_name(self):
        service_model = ServiceModel(self.model, 'myservice')
        self.assertEqual(service_model.service_name, 'myservice')

    def test_products(self):
        service_model = ServiceModel(self.model, 'myservice')
        self.assertEqual(service_model.products, ['ALTUS'])

    def test_endpoint_prefix(self):
        service_model = ServiceModel(self.model, 'myservice')
        self.assertEqual(service_model.endpoint_prefix, 'api')

    def test_operations(self):
        self.assertTrue(self.service_model.operation_model(
                'createDirector') is not None)
        self.assertTrue(self.service_model.operation_model(
                'deleteDirector') is not None)
        self.assertTrue(self.service_model.operation_model(
                'describeDirectors') is not None)
        with self.assertRaises(OperationNotFoundError):
            self.service_model.operation_model('NonExistentOperation')

    def test_documentation(self):
        self.assertTrue(self.service_model.documentation.startswith(
                'Cloudera CDP Test Service Description'))

    def test_operation_names(self):
        self.assertEquals(len(self.service_model.operation_names), 3)
        self.assertTrue('createDirector' in self.service_model.operation_names)
        self.assertTrue('deleteDirector' in self.service_model.operation_names)
        self.assertTrue('describeDirectors' in self.service_model.operation_names)


class TestOperationModel(unittest.TestCase):
    def setUp(self):
        self.model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.service_model = ServiceModel(self.model, 'servicename')

    def test_name(self):
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.name, 'createDirector')

    def test_pagination(self):
        operation_model = self.service_model.operation_model('describeDirectors')
        self.assertEqual(operation_model.can_paginate, True)
        self.assertEqual(operation_model.paging_input_token, 'startingToken')
        self.assertEqual(operation_model.paging_output_token, 'nextToken')
        self.assertEqual(operation_model.paging_result, 'directors')
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.can_paginate, False)
        self.assertEqual(operation_model.paging_input_token, None)
        self.assertEqual(operation_model.paging_output_token, None)
        self.assertEqual(operation_model.paging_result, None)

    def test_operation_input(self):
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.http['method'], 'post')
        self.assertEqual(operation_model.http['requestUri'], '/directors/createDirector')
        self.assertEqual(operation_model.input_shape.name, 'input')
        self.assertEqual(list(operation_model.input_shape.members), ['name'])

    def test_operation_output(self):
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.output_shape.name, 'output')
        self.assertEqual(list(operation_model.output_shape.members), ['director'])

    def test_has_documentation_property(self):
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.documentation, 'Creates a new Director.')

    def test_service_model_available_from_operation_model(self):
        operation_model = self.service_model.operation_model('createDirector')
        self.assertEqual(operation_model.service_model, self.service_model)


class TestShapeResolver(unittest.TestCase):
    def setUp(self):
        model = yaml.safe_load(open(os.path.join(MODEL_DIR, 'service.yaml')))
        self.resolver = ShapeResolver(model['definitions'])

    def _check_endpoint_shape(self, shape):
        self.assertEqual(shape.name, 'endpoint')
        self.assertEqual(shape.type_name, 'object')
        self.assertEqual(len(shape.required_members), 2)
        self.assertTrue('address' in shape.required_members)
        self.assertTrue('port' in shape.required_members)
        self.assertEqual(shape.members['address'].name, 'address')
        self.assertEqual(shape.members['address'].type_name, 'string')
        self.assertEqual(shape.members['port'].name, 'port')
        self.assertEqual(shape.members['port'].type_name, 'integer')

    def test_get_shape_by_name(self):
        shape = self.resolver.get_shape_by_name('endpoint', 'Endpoint')
        self._check_endpoint_shape(shape)

    def test_resolve_shape_reference(self):
        shape = self.resolver.resolve_shape_ref('endpoint', '#/definitions/Endpoint')
        self._check_endpoint_shape(shape)

    def test_min_and_max_length(self):
        shape = self.resolver.get_shape_by_name('shape', 'MinAndMaxLengthTest')
        self.assertEqual(shape.type_name, 'object')
        self.assertEqual(shape.members['stringparam'].min_length, 10)
        self.assertEqual(shape.members['stringparam'].max_length, 20)

    def test_minimum_and_maximum(self):
        shape = self.resolver.get_shape_by_name('shape', 'MinimumAndMaximumTest')
        self.assertEqual(shape.type_name, 'object')
        self.assertEqual(shape.members['intparam'].minimum, 15)
        self.assertEqual(shape.members['intparam'].maximum, 30)

    def test_list_shape(self):
        shape = self.resolver.get_shape_by_name('shape', 'DescribeDirectorsRequest')
        self.assertEqual(shape.members['names'].name, 'names')
        self.assertEqual(shape.members['names'].type_name, 'array')
        self.assertEqual(shape.members['names'].member.name, 'items')
        self.assertEqual(shape.members['names'].member.type_name, 'string')

    def test_string_shape_with_enum(self):
        shape = self.resolver.get_shape_by_name('shape', 'Director')
        self.assertEqual(shape.members['status'].name, 'status')
        self.assertEqual(shape.members['status'].type_name, 'string')
        self.assertEqual(shape.members['status'].enum, ['UNAVAILABLE',
                                                        'CREATING',
                                                        'STOPPED',
                                                        'STARTING',
                                                        'RUNNING',
                                                        'STOPPING',
                                                        'DESTROYING',
                                                        'UNKNOWN'])

    def test_no_paramfile(self):
        shape = self.resolver.get_shape_by_name('shape', 'NoParamfileTest')
        self.assertEqual(shape.type_name, 'object')
        self.assertEqual(shape.members['stringparam1'].is_no_paramfile, True)
        self.assertEqual(shape.members['stringparam2'].is_no_paramfile, False)

    def test_unknown_shape(self):
        with self.assertRaises(NoShapeFoundError):
            self.resolver.get_shape_by_name('unknown', 'Unknown')

    def test_invalid_shape_type(self):
        with self.assertRaises(InvalidShapeError):
            self.resolver.get_shape_by_name('invalid', 'MissingShapeType')
        with self.assertRaises(InvalidShapeError):
            self.resolver.get_shape_by_name('invalid', 'InvalidShapeType')

    def test_undocumented_param(self):
        shape = self.resolver.get_shape_by_name('shape', 'UndocumentedParmTest')
        self.assertEqual(shape.members['undocumentedParam'].is_undocumented, True)
        self.assertEqual(shape.members['documentedParam'].is_undocumented, False)

    def test_referenced_map(self):
        shape = self.resolver.get_shape_by_name('shape', 'ReferencedMapTest')
        self.assertEqual(shape.members['endpoints'].type_name, 'map')
        self.assertEqual(shape.members['endpoints'].key.type_name, 'string')
        self.assertEqual(shape.members['endpoints'].value.type_name, 'object')
        self.assertTrue(self._compare_lists(
            list(shape.members['endpoints'].value.members.keys()),
            ['port', 'address'])
        )

    def test_explicit_map(self):
        shape = self.resolver.get_shape_by_name('shape', 'ExplicitTypeMapTest')
        self.assertEqual(shape.members['endpoints'].type_name, 'map')
        self.assertEqual(shape.members['endpoints'].key.type_name, 'string')
        self.assertEqual(shape.members['endpoints'].value.type_name, 'object')
        self.assertTrue(self._compare_lists(
            list(shape.members['endpoints'].value.members.keys()),
            ['port', 'address'])
        )

    def test_no_freeform_map_dict_shape(self):
        with self.assertRaises(InvalidShapeError):
            shape = self.resolver.get_shape_by_name('invalid', 'NoFreeformMapDictTest')
            shape.members['endpoints'].value

    def test_no_freeform_map_true_shape(self):
        with self.assertRaises(InvalidShapeError):
            shape = self.resolver.get_shape_by_name('invalid', 'NoFreeformMapTrueTest')
            shape.members['endpoints'].value

    def _compare_lists(self, x, y):
        return collections.Counter(x) == collections.Counter(y)


class TestBuilders(unittest.TestCase):

    def test_structure_shape_builder_with_scalar_types(self):
        b = DenormalizedStructureBuilder()
        shape = b.with_members({
            'A': {'type': 'string'},
            'B': {'type': 'integer'},
        }).build_model()
        self.assertIsInstance(shape, ObjectShape)
        self.assertEqual(sorted(list(shape.members)), ['A', 'B'])
        self.assertEqual(shape.members['A'].type_name, 'string')
        self.assertEqual(shape.members['B'].type_name, 'integer')

    def test_structure_shape_with_object_type(self):
        b = DenormalizedStructureBuilder()
        shape = b.with_members({
            'A': {
                'type': 'object',
                'properties': {
                    'A-1': {'type': 'string'},
                }
            },
        }).build_model()
        self.assertIsInstance(shape, ObjectShape)
        self.assertEqual(list(shape.members), ['A'])
        self.assertEqual(shape.members['A'].type_name, 'object')
        self.assertEqual(list(shape.members['A'].members), ['A-1'])

    def test_structure_shape_with_list(self):
        b = DenormalizedStructureBuilder()
        shape = b.with_members({
            'A': {
                'type': 'array',
                'items': {
                    'type': 'string'
                }
            },
        }).build_model()
        self.assertIsInstance(shape.members['A'], ArrayShape)
        self.assertEqual(shape.members['A'].member.type_name, 'string')

    def test_structure_shape_with_map_type(self):
        b = DenormalizedStructureBuilder()
        shape = b.with_members({
            'A': {
                'type': 'object',
                'additionalProperties': {
                    'type': 'string'
                }
            }
        }).build_model()
        self.assertIsInstance(shape.members['A'], MapShape)
        map_shape = shape.members['A']
        self.assertEqual(map_shape.key.type_name, 'string')
        self.assertEqual(map_shape.value.type_name, 'string')

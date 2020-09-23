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

from collections import defaultdict

from cdpcli import ADDITIONAL_PROPERTIES
from cdpcli import LIST_TYPE
from cdpcli import MAP_TYPE
from cdpcli import OBJECT_TYPE
from cdpcli import REF_KEY
from cdpcli import REF_NAME_PREFIX
from cdpcli import TYPE_KEY
from cdpcli.compat import OrderedDict
from cdpcli.utils import CachedProperty
from cdpcli.utils import instance_cache


class NoShapeFoundError(Exception):
    pass


class InvalidShapeError(Exception):
    pass


class InvalidModelError(Exception):
    pass


class OperationNotFoundError(Exception):
    pass


def _get_shape_type(shape_data):
    shape_type = shape_data['type']
    if shape_type == 'string':
        if shape_data.get('format', None) == 'date-time':
            return 'datetime'
        if shape_data.get('format', None) == 'byte':
            return 'blob'
    if shape_type == 'number':
        return shape_data.get('format')
    if shape_type == 'object':
        if ADDITIONAL_PROPERTIES in shape_data:
            return 'map'
    return shape_type


class Shape(object):

    def __init__(self, name, shape_data, shape_resolver):
        self.name = name
        self.type_name = _get_shape_type(shape_data)
        # Array content definitions do not have description entries since the
        # array itself is documented.
        self.documentation = shape_data.get('description', "")
        self._shape_data = shape_data
        self._shape_resolver = shape_resolver
        self._cache = {}

    @CachedProperty
    def required_members(self):
        return self._shape_data.get('required', [])

    @CachedProperty
    def min_length(self):
        return self._shape_data.get('minLength', None)

    @CachedProperty
    def max_length(self):
        return self._shape_data.get('maxLength', None)

    @CachedProperty
    def minimum(self):
        return self._shape_data.get('minimum', None)

    @CachedProperty
    def maximum(self):
        return self._shape_data.get('maximum', None)

    @CachedProperty
    def is_paging_input_token(self):
        return self._shape_data.get('x-paging-input-token', False)

    @CachedProperty
    def is_paging_output_token(self):
        return self._shape_data.get('x-paging-output-token', False)

    @CachedProperty
    def is_paging_result(self):
        return self._shape_data.get('x-paging-result', False)

    @CachedProperty
    def is_page_size(self):
        return self._shape_data.get('x-paging-page-size', False)

    @CachedProperty
    def is_no_paramfile(self):
        return self._shape_data.get('x-no-paramfile', False)

    @CachedProperty
    def is_undocumented(self):
        return self._shape_data.get('x-deprecated', False)

    def _get_shape(self, name, shape_data):
        if REF_KEY in shape_data:
            # Reference value type
            # $ref: '#/definitions/SomeObject'
            return self._shape_resolver.resolve_shape_ref(name,
                                                          shape_data[REF_KEY])
        elif TYPE_KEY in shape_data:
            # Explicit value type, e.g., SomeShapeType
            return self._shape_resolver.get_shape(name, shape_data)
        else:
            raise InvalidShapeError("Unknown %s content: %s"
                                    % shape_data, self.name)


class ObjectShape(Shape):
    @CachedProperty
    def members(self):
        members = OrderedDict()
        for name, shape_data in self._shape_data.get('properties', {}).items():
            members[name] = self._get_shape(name, shape_data)
        return members


class MapShape(Shape):
    @CachedProperty
    def key(self):
        return self._get_shape('key', {'type': 'string'})

    @CachedProperty
    def value(self):
        additional_properties = self._shape_data.get(ADDITIONAL_PROPERTIES)
        # Map with Freeform object
        if type(additional_properties) == bool or not additional_properties:
            raise InvalidShapeError("Freeform maps are not supported: %s"
                                    % self.name)
        else:
            return self._get_shape('value', additional_properties)


class ArrayShape(Shape):
    @CachedProperty
    def member(self):
        return self._get_shape("items", self._shape_data['items'])


class StringShape(Shape):
    @CachedProperty
    def enum(self):
        return self._shape_data.get('enum', [])

    @CachedProperty
    def supported_options(self):
        return self._shape_data.get('x-supported-options', [])


class ShapeResolver(object):

    def __init__(self, definitions):
        self._definitions = definitions

    def get_shape(self, name, shape_data):
        try:
            shape_type = _get_shape_type(shape_data)
            if shape_type == OBJECT_TYPE:
                shape_cls = ObjectShape
            elif shape_type == 'map':
                shape_cls = MapShape
            elif shape_type == LIST_TYPE:
                shape_cls = ArrayShape
            elif shape_type == 'string':
                shape_cls = StringShape
            elif shape_type in ['integer',
                                'float',
                                'double',
                                'boolean',
                                'datetime',
                                'blob']:
                shape_cls = Shape
            else:
                raise InvalidShapeError("Unknown shape type: %s" % shape_type)
        except KeyError:
            raise InvalidShapeError("Shape is missing required key 'type': %s"
                                    % (shape_data))
        return shape_cls(name, shape_data, self)

    def get_shape_by_name(self, name, shape_name):
        try:
            shape_data = self._definitions[shape_name]
        except KeyError:
            raise NoShapeFoundError(shape_name)
        return self.get_shape(name, shape_data)

    def resolve_shape_ref(self, name, shape_ref):
        return self.get_shape_by_name(name, shape_ref[len('#/definitions/'):])


class OperationModel(object):

    def __init__(self,
                 operation_data,
                 service_model,
                 name,
                 http_method,
                 request_uri):
        self._operation_data = operation_data
        self._service_model = service_model
        self._name = name
        self.http = {
            'method': http_method,
            'requestUri': request_uri,
            }

    @CachedProperty
    def name(self):
        return self._name

    @property
    def service_model(self):
        return self._service_model

    @CachedProperty
    def documentation(self):
        return self._operation_data["description"]

    @CachedProperty
    def can_paginate(self):
        return self._operation_data.get("x-paginates", False)

    @CachedProperty
    def paging_input_token(self):
        if self.can_paginate:
            for name, shape in self.input_shape.members.items():
                if shape.is_paging_input_token:
                    return name
        return None

    @CachedProperty
    def paging_output_token(self):
        if self.can_paginate:
            for name, shape in self.output_shape.members.items():
                if shape.is_paging_output_token:
                    return name
        return None

    @CachedProperty
    def paging_result(self):
        if self.can_paginate:
            for name, shape in self.output_shape.members.items():
                if shape.is_paging_result:
                    return name
        return None

    @CachedProperty
    def paging_page_size(self):
        if self.can_paginate:
            for name, shape in self.input_shape.members.items():
                if shape.is_page_size:
                    return name
        return None

    @CachedProperty
    def paging_default_max_items(self):
        if self.can_paginate:
            return int(self._operation_data.get("x-paging-default-max-items"))
        return None

    @CachedProperty
    def input_shape(self):
        return self._service_model.resolve_shape_ref(
            "input", self._operation_data['parameters'][0]['schema'][REF_KEY])

    @CachedProperty
    def output_shape(self):
        return self._service_model.resolve_shape_ref(
            "output", self._operation_data['responses'][200]['schema'][REF_KEY])


class ServiceModel(object):

    def __init__(self, service_data, service_name):
        self._service_data = service_data
        self._service_name = service_name
        self._instance_cache = {}
        self._shape_resolver = ShapeResolver(service_data.get('definitions', {}))

    @CachedProperty
    def service_name(self):
        return self._service_name

    @CachedProperty
    def documentation(self):
        return self._service_data["info"]["description"]

    @instance_cache
    def operation_model(self, operation_name):
        for request_uri in self._service_data["paths"]:
            for http_method in self._service_data["paths"][request_uri]:
                operation_data = self._service_data["paths"][request_uri][http_method]
                if operation_data["operationId"] == operation_name:
                    return OperationModel(operation_data,
                                          self,
                                          operation_name,
                                          http_method,
                                          request_uri)
        raise OperationNotFoundError(operation_name)

    @CachedProperty
    def operation_names(self):
        operation_names = []
        for path in self._service_data["paths"]:
            for operation in self._service_data["paths"][path]:
                operation_names.append(
                    self._service_data["paths"][path][operation]["operationId"])
        return operation_names

    def resolve_shape_ref(self, name, shape_ref):
        return self._shape_resolver.resolve_shape_ref(name, shape_ref)

    @CachedProperty
    def endpoint_name(self):
        return self._service_data["x-endpoint-name"]

    @CachedProperty
    def endpoint_prefix(self):
        return self._service_data.get("x-endpoint-prefix", 'api')

    @CachedProperty
    def products(self):
        return self._service_data.get("x-products", "ALTUS").split(',')


class DenormalizedStructureBuilder(object):
    """Build a StructureShape from a denormalized model.

    This is a convenience builder class that makes it easy to construct
    ``StructureShape``s based on a denormalized model.

    It will handle the details of creating unique shape names and creating
    the appropriate shape map needed by the ``StructureShape`` class.

    Example usage::

        builder = DenormalizedStructureBuilder()
        shape = builder.with_members({
            'A': {
                'type': 'object',
                'properties': {
                    'B': {
                        'type': 'object',
                        'properties': {
                            'C': {
                                'type': 'string',
                            }
                        }
                    }
                }
            }
        }).build_model()
        # ``shape`` is now an instance of cdpcli.model.StructureShape
    """
    def __init__(self, name=None):
        self.members = OrderedDict()
        self._name_generator = ShapeNameGenerator()
        if name is None:
            self.name = self._name_generator.new_shape_name('object')

    def with_members(self, members):
        self._members = members
        return self

    def build_model(self):
        shapes = OrderedDict()
        denormalized = {
            'type': OBJECT_TYPE,
            'properties': self._members,
        }
        self._build_model(denormalized, shapes, self.name)
        resolver = ShapeResolver(shapes)
        return ObjectShape(name=self.name,
                           shape_data=shapes[self.name],
                           shape_resolver=resolver)

    def _build_model(self, model, shapes, shape_name):
        shape_type = _get_shape_type(model)
        if shape_type == OBJECT_TYPE:
            shapes[shape_name] = self._build_object(model, shapes)
        elif shape_type == MAP_TYPE:
            shapes[shape_name] = self._build_map(model, shapes)
        elif shape_type == LIST_TYPE:
            shapes[shape_name] = self._build_array(model, shapes)
        elif shape_type in ['string',
                            'integer',
                            'float',
                            'double',
                            'boolean',
                            'datetime',
                            'blob']:
            shapes[shape_name] = self._build_scalar(model)
        else:
            raise InvalidShapeError("Unknown shape type: %s" % model['type'])

    def _build_object(self, model, shapes):
        members = OrderedDict()
        shape = self._build_initial_shape(model)
        shape['properties'] = members

        for name, member_model in model['properties'].items():
            member_shape_name = self._get_shape_name(member_model)
            members[name] = {REF_KEY: REF_NAME_PREFIX + member_shape_name}
            self._build_model(member_model, shapes, member_shape_name)
        return shape

    def _build_array(self, model, shapes):
        member_shape_name = self._get_shape_name(model['items'])
        shape = self._build_initial_shape(model)
        shape['items'] = {REF_KEY: REF_NAME_PREFIX + member_shape_name}
        self._build_model(model['items'], shapes, member_shape_name)
        return shape

    def _build_map(self, model, shapes):
        value_shape_name = self._get_shape_name(model[ADDITIONAL_PROPERTIES])
        shape = self._build_initial_shape(model)
        shape[ADDITIONAL_PROPERTIES] = {REF_KEY: REF_NAME_PREFIX + value_shape_name}
        self._build_model(model[ADDITIONAL_PROPERTIES], shapes, value_shape_name)
        return shape

    def _build_initial_shape(self, model):
        shape = {
            'type': model['type'],
        }
        if 'documentation' in model:
            shape['documentation'] = model['documentation']
        if 'enum' in model:
            shape['enum'] = model['enum']
        if 'x-supported-options' in model:
            shape['x-supported-options'] = model['x-supported-options']
        if 'x-no-paramfile' in model:
            shape['x-no-paramfile'] = model['x-no-paramfile']
        return shape

    def _build_scalar(self, model):
        return self._build_initial_shape(model)

    def _get_shape_name(self, model):
        if 'shape_name' in model:
            return model['shape_name']
        else:
            return self._name_generator.new_shape_name(model['type'])


class ShapeNameGenerator(object):
    """Generate unique shape names for a type.

    This class can be used in conjunction with the DenormalizedStructureBuilder
    to generate unique shape names for a given type.

    """
    def __init__(self):
        self._name_cache = defaultdict(int)

    def new_shape_name(self, type_name):
        """Generate a unique shape name.

        This method will guarantee a unique shape name each time it is
        called with the same type.

        ::

            >>> s = ShapeNameGenerator()
            >>> s.new_shape_name('object')
            'ObjectType1'
            >>> s.new_shape_name('object')
            'ObjectType2'
            >>> s.new_shape_name('array')
            'ListType1'
            >>> s.new_shape_name('array')
            'ListType2'


        :type type_name: string
        :param type_name: The type name (object, list, map, string, etc.)

        :rtype: string
        :return: A unique shape name for the given type

        """
        self._name_cache[type_name] += 1
        current_index = self._name_cache[type_name]
        return '%sType%s' % (type_name.capitalize(),
                             current_index)

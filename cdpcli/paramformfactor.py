# Copyright 2022 Cloudera, Inc. All rights reserved.
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

from cdpcli.exceptions import WrongArgFormFactorError


class ParamFormFactorVisitor(object):
    """
    This visitor's visit method will walk the input params object of the input
    shape, visiting all fields and recursing through complex fields. Any field
    encountered will be checked for form factors.
    """

    def __init__(self, form_factor):
        self._form_factor = form_factor

    def visit(self, params, shape):
        if self._form_factor is None:
            # No need to filter out any parameter if form factor is None.
            # For example: 'refdoc' command.
            return
        else:
            return self._visit(params, shape, name='')

    def _visit(self, params, shape, name):
        # self._form_factor won't be None because it is checked in visit()
        if shape.form_factors is not None and self._form_factor not in shape.form_factors:
            raise WrongArgFormFactorError(
                arg_name=name,
                form_factor=self._form_factor,
                arg_form_factors=', '.join(shape.form_factors))
        return getattr(self, '_visit_%s' % shape.type_name)(params, shape, name)

    def _visit_object(self, params, shape, name):
        visited = dict()
        for param in params:
            if param not in shape.members:
                visited[param] = params[param]
                continue
            visited[param] = self._visit(params[param],
                                         shape.members[param],
                                         '%s.%s' % (name, param))
        return visited

    def _visit_map(self, params, shape, name):
        visited = dict()
        for param in params:
            # does not support visiting key, as it is supposed to be string
            visited[param] = self._visit(params[param],
                                         shape.value,
                                         '%s.%s' % (name, param))
        return visited

    def _visit_array(self, param, shape, name):
        visited = []
        for i, item in enumerate(param):
            visited.append(self._visit(item, shape.member, '%s[%s]' % (name, i)))
        return visited

    def _visit_boolean(self, param, shape, name):
        return param

    def _visit_integer(self, param, shape, name):
        return param

    def _visit_float(self, param, shape, name):
        return param

    def _visit_double(self, param, shape, name):
        return param

    def _visit_string(self, param, shape, name):
        return param

    def _visit_datetime(self, param, shape, name):
        return param

    def _visit_blob(self, param, shape, name):
        return param

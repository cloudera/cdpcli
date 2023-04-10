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

import os

from cdpcli.compat import compat_open
from cdpcli.compat import six
import requests


class ResourceLoadingError(Exception):
    pass


def get_paramfile(path, parsed_globals):
    """Load parameter based on a resource URI.

    It is possible to pass parameters to operations by referring
    to files or URI's.  If such a reference is detected, this
    function attempts to retrieve the data from the file or URI
    and returns it.  If there are any errors or if the ``path``
    does not appear to refer to a file or URI, a ``None`` is
    returned.

    """
    data = None
    if isinstance(path, six.string_types):
        for prefix, function_spec in PREFIX_MAP.items():
            if path.startswith(prefix):
                function, kwargs = function_spec
                kwargs['parsed_globals'] = parsed_globals
                data = function(prefix, path, **kwargs)
    return data


def get_file(prefix, path, mode, parsed_globals):
    file_path = os.path.expandvars(os.path.expanduser(path[len(prefix):]))
    try:
        with compat_open(file_path, mode) as f:
            return f.read()
    except UnicodeDecodeError:
        raise ResourceLoadingError(
            'Unable to load paramfile (%s), text contents could '
            'not be decoded.  If this is a binary file, please use the '
            'fileb:// prefix instead of the file:// prefix.' % file_path)
    except (OSError, IOError) as e:
        raise ResourceLoadingError('Unable to load paramfile %s: %s' % (
            path, e))


def get_uri(prefix, uri, parsed_globals):
    try:
        # The TLS verification value can be a boolean or a CA_BUNDLE path. This
        # is a little odd, but ultimately comes from the python HTTP requests
        # library we're using.
        tls_verification = getattr(parsed_globals, 'verify_tls', True)
        ca_bundle = getattr(parsed_globals, 'ca_bundle', None)
        if tls_verification and ca_bundle is not None:
            tls_verification = ca_bundle
        r = requests.get(uri, verify=tls_verification)
        if r.status_code == 200:
            return r.text
        else:
            raise ResourceLoadingError(
                "received non 200 status code of %s" % (
                    r.status_code))
    except Exception as e:
        raise ResourceLoadingError('Unable to retrieve %s: %s' % (uri, e))


PREFIX_MAP = {
    'file://': (get_file, {'mode': 'r'}),
    'fileb://': (get_file, {'mode': 'rb'}),
    'http://': (get_uri, {}),
    'https://': (get_uri, {}),
}


class ParamFileVisitor(object):
    def __init__(self, parsed_globals):
        self._parsed_globals = parsed_globals

    """
    This visitor's visit method will walk the input params object of the input
    shape, visiting all fields and recursing through complex fields. Any string
    field encountered will get paramfile resolution unless it is marked in the
    model as being x-no-paramfile.
    """

    def visit(self, params, shape):
        return self._visit(params, shape, name='')

    def _visit(self, params, shape, name):
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

    def _visit_boolean(self, param, shape, name):
        return param

    def _visit_integer(self, param, shape, name):
        return param

    def _visit_float(self, param, shape, name):
        return param

    def _visit_double(self, param, shape, name):
        return param

    def _visit_string(self, param, shape, name):
        if not getattr(self._parsed_globals, 'expand_param', True):
            return param
        if shape.is_no_paramfile:
            return param
        override = get_paramfile(param, self._parsed_globals)
        if override is not None:
            return override
        return param

    def _visit_array(self, param, shape, name):
        visited = []
        for i, item in enumerate(param):
            visited.append(self._visit(item, shape.member, '%s[%s]' % (name, i)))
        return visited

    def _visit_datetime(self, param, shape, name):
        return param

    def _visit_blob(self, param, shape, name):
        if not getattr(self._parsed_globals, 'expand_param', True):
            return param
        if shape.is_no_paramfile:
            return param
        override = get_paramfile(param, self._parsed_globals)
        if override is not None:
            return override
        return param

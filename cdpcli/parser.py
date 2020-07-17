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

from cdpcli.compat import json
import dateutil.parser


class ResponseParserFactory(object):

    def create_parser(self):
        return ResponseParser()


class ResponseParser(object):
    DEFAULT_ENCODING = 'utf-8'

    def parse(self, response, shape):
        if response['status_code'] >= 301:
            return self._parse_error(response)
        elif shape is None:
            return {}
        else:
            return self._parse_shape(shape, self._decode_body(response))

    def _decode_body(self, response):
        body = response['body']
        if not body:
            return {}
        return json.loads(body.decode(self.DEFAULT_ENCODING))

    def _parse_error(self, response):
        try:
            body = self._decode_body(response)
        except Exception:
            # In the case that we hit a completely dead endpoint, etc, the
            # response might not be valid JSON, but we still want to return
            # a structured error to the caller.
            message = response['body'].decode(self.DEFAULT_ENCODING).strip()
            body = dict(code='UNKNOWN_ERROR', message=message)
        error = {}
        error['code'] = body.get('code', '')
        error['message'] = body.get('message', '')
        return {'error': error}

    def _parse_shape(self, shape, value):
        handle_method_name = '_handle_%s' % shape.type_name
        method = getattr(self, handle_method_name, self._handle_default)
        return method(shape, value)

    def _handle_array(self, shape, value):
        parsed = []
        for item in value:
            parsed.append(self._parse_shape(shape.member, item))
        return parsed

    def _handle_object(self, shape, value):
        parsed = {}
        for member_name in shape.members:
            member_shape = shape.members[member_name]
            member_value = value.get(member_shape.name)
            if member_value is not None:
                parsed[member_name] = self._parse_shape(
                    shape.members[member_name], member_value)
        return parsed

    def _handle_map(self, shape, values):
        parsed = {}
        for key, value in values.items():
            actual_key = self._parse_shape(shape.key, key)
            actual_value = self._parse_shape(shape.value, value)
            parsed[actual_key] = actual_value
        return parsed

    # Handles both ISO8601 format and RFC822 format.
    # However, server can only parse ISO8601 format as of now.
    def _handle_datetime(self, shape, value):
        try:
            return dateutil.parser.parse(value)
        except (TypeError, ValueError) as e:
            raise ValueError('Invalid timestamp "%s": %s' % (value, e))

    def _handle_default(self, shape, value):
        return value

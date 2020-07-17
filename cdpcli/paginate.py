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

from copy import copy
import logging

from cdpcli.exceptions import PaginationError
from cdpcli.model import OperationModel

LOG = logging.getLogger('cdpcli.paginate')


class PageIterator(object):
    def __init__(self,
                 method,
                 operation_model,
                 max_items,
                 starting_token,
                 page_size,
                 op_kwargs):
        self._op_kwargs = op_kwargs
        # copy only those op model values that we do use
        self._method = method
        self._input_token_key = operation_model.paging_input_token
        self._output_token_key = operation_model.paging_output_token
        self._result_key = operation_model.paging_result
        self._page_size_key = operation_model.paging_page_size
        self._page_size_maximum = \
            operation_model.input_shape.members[self._page_size_key].maximum
        self._max_items = max_items if max_items is not None else \
            operation_model.paging_default_max_items
        self._page_size = page_size if page_size is not None \
            else self._max_items
        self._previous_next_token = starting_token

    def __iter__(self):
        previous_next_token = self._previous_next_token
        if previous_next_token is not None:
            self._op_kwargs[self._input_token_key] = previous_next_token
        items_returned = 0

        while True:
            # use explicit page sizes when --max-items is specified
            # the size of the last page we fetch must be such that we get
            # exactly max_items number of items
            self._op_kwargs[self._page_size_key] = min(
                self._page_size,
                self._page_size_maximum,
                self._max_items - items_returned
            )

            response = self._method(**self._op_kwargs)
            # yield a page of results to accumulator
            yield response

            # grab the result so we can update the returned item counter
            result = response.get(self._result_key, [])
            # if returned object is not a list, then we don't count
            # Note: this code and tests are likely dead, see THUN-637
            items_returned += len(result) if isinstance(result, list) else 0
            # grab continuation token from response presence of which means more
            # pages
            next_token = response.get(self._output_token_key, None)

            # validate runtime state, raise errors instead of producing possibly
            # incorrect results or hammering the service in unexpected ways
            if previous_next_token is not None and \
                    previous_next_token == next_token:
                msg = "The same next token was received twice: %s"
                raise PaginationError(message=msg % next_token)
            if items_returned > self._max_items:
                msg = "Received more items than expected. Expected: %d; " \
                      "Returned: %d" % (self._max_items, items_returned)
                raise PaginationError(message=msg)

            # exit if we reached the last page
            if next_token is None:
                break
            # exit if we returned the request max number of items
            if self._max_items == items_returned:
                # We should do better than this and return the CLI next token.
                # but that will come in a follow up change.
                LOG.warn("Max items received. Refine your filter or use "
                         "advance pagination.")
                break

            # advance to the next page
            self._op_kwargs[self._input_token_key] = \
                previous_next_token = next_token

    # builds a full operation result object by iterating over pages which are
    # fetched using multiple requests to service
    def build_full_result(self):
        result = {}
        # iterate over service responses, one response per page of results
        for response in self:
            response_value = response.get(self._result_key, None)
            if response_value is None:
                continue
            existing_value = result.get(self._result_key, None)
            if existing_value is None:
                result[self._result_key] = copy(response_value)
            elif isinstance(response_value, list):
                existing_value.extend(response_value)
            else:
                result[self._result_key] = existing_value + response_value

        # expose continuation token in the response, this should only happen if
        # not all items were fetched due to logic above, such as when
        # --max-items is used
        next_token = response.get(self._output_token_key, None)
        if next_token:
            result['nextToken'] = next_token

        return result


class Paginator(object):
    def __init__(self,
                 method,
                 operation_model):
        def assert_check(condition, message):
            if not condition:
                raise AssertionError(message)
        assert_check(callable(method), "method is callable")
        assert_check(isinstance(operation_model, OperationModel),
                     "operation_model has type OperationModel")
        self._method = method
        self._operation_model = operation_model

    def paginate(self, **kwargs):
        paging_params = self._extract_paging_params(kwargs)
        return PageIterator(self._method,
                            self._operation_model,
                            paging_params['MaxItems'],
                            paging_params['StartingToken'],
                            paging_params['PageSize'],
                            kwargs)

    def _extract_paging_params(self, kwargs):
        pagination_config = kwargs.pop('PaginationConfig', {})
        max_items = pagination_config.get('MaxItems', None)
        if max_items is not None:
            max_items = int(max_items)
        page_size = pagination_config.get('PageSize', None)
        if page_size is not None:
            page_size = int(page_size)
        return {
            'MaxItems': max_items,
            'StartingToken': pagination_config.get('StartingToken', None),
            'PageSize': page_size,
        }

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
import threading
import time

from cdpcli.cdprequest import create_request_object
from cdpcli.compat import six
from cdpcli.exceptions import EndpointConnectionError
from cdpcli.parser import ResponseParserFactory
from cdpcli.thirdparty.requests.exceptions import ConnectionError
from cdpcli.thirdparty.requests.sessions import Session
from cdpcli.thirdparty.requests.utils import get_environ_proxies


DEFAULT_TIMEOUT = 60
LOG = logging.getLogger('cdpcli.endpoint')


def convert_to_response_dict(http_response, operation_model):
    response_dict = {
        'headers': http_response.headers,
        'status_code': http_response.status_code,
    }
    if response_dict['status_code'] >= 300:
        response_dict['body'] = http_response.content
    else:
        response_dict['body'] = http_response.content
    return response_dict


class PreserveAuthSession(Session):

    def rebuild_auth(self, prepared_request, response):
        pass


class EndpointResolver(object):
    ENDPOINT_URL_KEY_NAME = 'endpoint_url'
    CDP_ENDPOINT_URL_KEY_NAME = 'cdp_endpoint_url'

    def _construct_altus_endpoint(self, service_name, scheme, port):
        return "%s://%sapi.us-west-1.altus.cloudera.com:%d" % (scheme, service_name, port)

    def _construct_cdp_endpoint(self, scheme, prefix, port):
        return "%s://%s.us-west-1.cdp.cloudera.com:%d" % (scheme, prefix, port)

    def _substitute_custom_endpoint(self, endpoint_url, value):
        return endpoint_url % (value)

    def resolve(self, service_name, prefix, products, endpoint_url, config, scheme, port):
        if endpoint_url is not None:
            if endpoint_url.count('%s') == 1:
                if products == ['CDP']:
                    return self._substitute_custom_endpoint(endpoint_url, prefix)
                else:
                    return self._substitute_custom_endpoint(endpoint_url, service_name)
            else:
                return endpoint_url

        if products == ['CDP']:
            endpoint_from_config = config.get(EndpointResolver.CDP_ENDPOINT_URL_KEY_NAME)
            if endpoint_from_config is not None:
                if endpoint_from_config.count('%s') == 1:
                    return self._substitute_custom_endpoint(endpoint_from_config,
                                                            prefix)
                else:
                    return endpoint_from_config
            return self._construct_cdp_endpoint(scheme, prefix, port)
        else:
            endpoint_from_config = config.get(EndpointResolver.ENDPOINT_URL_KEY_NAME)
            if endpoint_from_config is not None:
                if endpoint_from_config.count('%s') == 1:
                    return self._substitute_custom_endpoint(endpoint_from_config,
                                                            service_name)
                else:
                    return endpoint_from_config
            return self._construct_altus_endpoint(service_name, scheme, port)


class EndpointCreator(object):

    def __init__(self, endpoint_resolver):
        self._endpoint_resolver = endpoint_resolver

    def create_endpoint(self,
                        service_model,
                        endpoint_url,
                        scoped_config,
                        response_parser_factory,
                        tls_verification,
                        timeout,
                        retry_handler):
        endpoint_url = \
            self._endpoint_resolver.resolve(endpoint_url=endpoint_url,
                                            config=scoped_config,
                                            service_name=service_model.endpoint_name,
                                            prefix=service_model.endpoint_prefix,
                                            products=service_model.products,
                                            scheme='https',
                                            port=443)
        proxies = self._get_proxies(endpoint_url)
        return Endpoint(
            endpoint_url,
            proxies=proxies,
            tls_verification=tls_verification,
            timeout=timeout,
            response_parser_factory=response_parser_factory,
            retry_handler=retry_handler)

    def _get_proxies(self, url):
        return get_environ_proxies(url)


class Endpoint(object):

    def __init__(self,
                 host,
                 proxies=None,
                 tls_verification=True,
                 timeout=DEFAULT_TIMEOUT,
                 response_parser_factory=None,
                 retry_handler=None):
        self.host = host
        self.tls_verification = tls_verification
        if proxies is None:
            proxies = {}
        self.proxies = proxies
        self.http_session = PreserveAuthSession()
        self.timeout = timeout
        self._lock = threading.Lock()
        if response_parser_factory is None:
            response_parser_factory = ResponseParserFactory()
        self._response_parser_factory = response_parser_factory
        self._retry_handler = retry_handler

    def __repr__(self):
        return '%s' % (self.host)

    def make_request(self, operation_model, request_dict, request_signer):
        return self._send_request(request_dict, operation_model, request_signer)

    def create_request(self, params, operation_model, request_signer):
        request = create_request_object(params)
        request_signer.sign(request)
        return self.prepare_request(request)

    def _encode_headers(self, headers):
        # In place encoding of headers to utf-8 if they are unicode.
        for key, value in headers.items():
            if isinstance(value, six.text_type):
                # We have to do this because request.headers is not
                # normal dictionary.  It has the (unintuitive) behavior
                # of aggregating repeated setattr calls for the same
                # key value.  For example:
                # headers['foo'] = 'a'; headers['foo'] = 'b'
                # list(headers) will print ['foo', 'foo'].
                del headers[key]
                headers[key] = value.encode('utf-8')

    def prepare_request(self, request):
        self._encode_headers(request.headers)
        return request.prepare()

    def _send_request(self, request_dict, operation_model, request_signer):
        attempts = 1
        request = self.create_request(request_dict, operation_model, request_signer)
        success_response, exception = self._get_response(
            request, operation_model, attempts)
        while self._needs_retry(attempts, operation_model,
                                success_response, exception):
            attempts += 1
            # If there is a stream associated with the request, we need
            # to reset it before attempting to send the request again.
            # This will ensure that we resend the entire contents of the
            # body.
            request.reset_stream()
            # Create a new request when retried (including a new signature).
            request = self.create_request(
                request_dict,
                operation_model=operation_model,
                request_signer=request_signer)
            success_response, exception = self._get_response(
                request, operation_model, attempts)
        if exception is not None:
            raise exception
        else:
            return success_response

    def _get_response(self, request, operation_model, attempts):
        # This will return a tuple of (success_response, exception)
        # and success_response is itself a tuple of
        # (http_response, parsed_dict).
        # If an exception occurs then the success_response is None.
        # If no exception occurs then exception is None.
        try:
            http_response = self.http_session.send(
                request,
                verify=self.tls_verification,
                stream=False,
                proxies=self.proxies,
                timeout=self.timeout)
        except ConnectionError as e:
            # For a connection error, if it looks like it's a DNS
            # lookup issue, 99% of the time this is due to a misconfigured
            # region/endpoint so we'll raise a more specific error message
            # to help users.
            if self._looks_like_dns_error(e):
                endpoint_url = e.request.url
                better_exception = EndpointConnectionError(
                    endpoint_url=endpoint_url, error=e)
                return (None, better_exception)
            else:
                return (None, e)
        except Exception as e:
            return (None, e)
        # This returns the http_response and the parsed_data.
        response_dict = convert_to_response_dict(http_response,
                                                 operation_model)
        parser = self._response_parser_factory.create_parser()
        return ((http_response, parser.parse(response_dict,
                                             operation_model.output_shape)),
                None)

    def _looks_like_dns_error(self, e):
        return 'gaierror' in str(e) and e.request is not None

    def _needs_retry(self,
                     attempts,
                     operation_model,
                     response=None,
                     caught_exception=None):
        if self._retry_handler is None:
            return False
        handler_response = \
            self._retry_handler(attempts=attempts,
                                response=response,
                                caught_exception=caught_exception)
        if handler_response is None:
            return False
        else:
            # Request needs to be retried, and we need to sleep
            # for the specified number of times.
            LOG.info("Response indicates retry is needed, sleeping for "
                     "%s seconds", handler_response)
            time.sleep(handler_response)
            return True

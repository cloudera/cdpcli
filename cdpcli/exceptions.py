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


class CdpCLIError(Exception):
    """
    The base exception class for CDP CLI exceptions.
    """
    fmt = 'An unspecified error occured'

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs


class ValidationError(CdpCLIError):
    """
    An exception occurred validating parameters.
    """
    fmt = "Invalid value ('{value}') for param {param} of type {type_name}"


class ParamValidationError(CdpCLIError):
    fmt = 'Parameter validation failed:\n{report}'


class DataNotFoundError(CdpCLIError):
    """
    The data associated with a particular path could not be loaded.
    """
    fmt = 'Unable to load data for: {data_path}'


class ExecutableNotFoundError(CdpCLIError):
    """
    The executable was not found.
    """
    fmt = 'Could not find executable named: {executable_name}'


class OperationNotPageableError(CdpCLIError):
    fmt = 'Operation cannot be paginated: {operation_name}'


class ClientError(Exception):
    MSG_TEMPLATE = (
        'An error occurred: {error_message} ('
        'Status Code: {http_status_code}; '
        'Error Code: {error_code}; '
        'Service: {service_name}; '
        'Operation: {operation_name}; '
        'Request ID: {request_id};)')

    def __init__(self, error_response, operation_name, service_name,
                 http_status_code, request_id):
        msg = self.MSG_TEMPLATE.format(
            error_code=error_response['error'].get('code', 'Unknown'),
            error_message=error_response['error'].get('message', 'Unknown'),
            operation_name=operation_name,
            service_name=service_name,
            http_status_code=http_status_code,
            request_id=request_id)
        super(ClientError, self).__init__(msg)
        self.response = error_response


class UnseekableStreamError(CdpCLIError):
    """
    Need to seek a stream, but stream does not support seeking.
    """
    fmt = ('Need to rewind the stream {stream_object}, but stream '
           'is not seekable.')


class EndpointConnectionError(CdpCLIError):
    fmt = (
        'Could not connect to the endpoint URL: "{endpoint_url}"')


class IncompleteReadError(CdpCLIError):
    """
    HTTP response did not return expected number of bytes.
    """
    fmt = ('{actual_bytes} read, but total bytes '
           'expected is {expected_bytes}.')


class PaginationError(CdpCLIError):
    fmt = 'Error during pagination: {message}'


class UnknownSignatureVersionError(CdpCLIError):
    """
    Requested Signature Version is not known.
    """
    fmt = 'Unknown Signature Version: {signature_version}.'


class UnsupportedSignatureVersionError(CdpCLIError):
    """
    Error when trying to access a method on a client that does not exist.
    """
    fmt = 'Signature version is not supported: {signature_version}'


class NoCredentialsError(CdpCLIError):
    """
    No credentials could be found.
    """
    fmt = 'Unable to locate CDP credentials'


class UnknownCredentialError(CdpCLIError):
    """
    Tried to insert before/after an unregistered credential type.
    """
    fmt = 'Credential named {name} not found.'


class MalformedCredentialsError(CdpCLIError):
    """
    Malformed credentials found.
    """
    fmt = 'Malformed credentials found in {provider}, missing: {cred_var}'


class PartialCredentialsError(CdpCLIError):
    """
    Only partial credentials were found.
    """
    fmt = 'Partial credentials found in {provider}, missing: {cred_var}'


class BaseEndpointResolverError(CdpCLIError):
    """
    Base error for endpoint resolving errors.

    Should never be raised directly, but clients can catch
    this exception if they want to generically handle any errors
    during the endpoint resolution process.

    """


class NoRegionError(BaseEndpointResolverError):
    """
    No region was specified.
    """
    fmt = 'You must specify a region.'


class ProfileNotFound(CdpCLIError):
    """
    The specified configuration profile was not found in the
    configuration file.

    """
    fmt = 'The config profile ({profile}) could not be found'


class ConfigNotFound(CdpCLIError):
    """
    The specified configuration file could not be found.
    """
    fmt = 'The specified config file ({path}) could not be found.'


class ConfigParseError(CdpCLIError):
    """
    The configuration file could not be parsed.
    """
    fmt = 'Unable to parse config file: {path}'


class ClusterTerminatingError(CdpCLIError):

    """
    The cluster is terminating or has already terminated.
    """
    fmt = 'Cluster {cluster_name} is terminating.'


class ClusterStartingError(CdpCLIError):

    """
    The cluster is starting.
    """
    fmt = 'Cluster {cluster_name} is starting.'


class ClusterFailedError(CdpCLIError):

    """
    The cluster failed to start.
    """
    fmt = 'Cluster {cluster_name} failed to start.'


class ClusterStatusNotFound(CdpCLIError):

    """
    Unable to find cluster status.
    """
    fmt = 'Unable to find {cluster_name}\'s status.'


class ClusterEndpointNotFound(CdpCLIError):

    """
    Unable to find cluster's Cloudera Manager Endpoint.
    """
    fmt = 'Unable to find {cluster_name}\'s Cloudera Manager Endpoint.'


class SSHNotFoundError(CdpCLIError):

    """
    SSH or Putty not available.
    """
    fmt = 'SSH or Putty not available.'


class WrongPuttyKeyError(CdpCLIError):

    """
    A wrong key has been used with a compatible program.
    """
    fmt = 'Key file file format is incorrect. Putty expects a ppk file.'


class MissingArgumentError(CdpCLIError):

    """
    The following argument is required.
    """
    fmt = 'The following argument is required: {arg_name}.'


class InteractiveLoginError(CdpCLIError):

    """
    Login failed.
    """
    fmt = 'Login failed: {err_msg}.'

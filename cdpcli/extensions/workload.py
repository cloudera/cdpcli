# Copyright 2021 Cloudera, Inc. All rights reserved.
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
from urllib.parse import urljoin

from cdpcli.exceptions import WorkloadServiceDiscoveryError


LOG = logging.getLogger('cdpcli.extensions.workload')


def register(operation_callers, operation_model):
    """
    Register an extension to run before or after the CLI command.
    To replace the original CLI caller:
    * operation_callers.insert(0, ReplacementCaller())
    * return False by the ReplacementCaller.invoke(...)
    """
    operation_callers.insert(0, WorkloadServiceDiscovery())


def set_workload_access_token(iam_client,
                              parsed_globals,
                              workload_name,
                              environment_crn):
    """
    Set global Access Token and Endpoint URL for Workload and Environment CRN
    """
    req_params = {
        'workloadName': workload_name,
        'environmentCrn': environment_crn
    }
    response = iam_client.generate_workload_auth_token(**req_params)

    workload_url = response.get('endpointUrl', None)
    workload_access_token = response.get('token', None)
    LOG.debug('Workload service-discovery succeeded. '
              'endpoint_url=%s, access_token=%s...',
              workload_url,
              workload_access_token[0:16] if workload_access_token else None)

    # remove path from the workload URL
    if workload_url is not None:
        workload_url = urljoin(workload_url, '/')
    # add 'Bearer' prefix to access token
    if workload_access_token is not None and \
       not workload_access_token.startswith('Bearer '):
        workload_access_token = 'Bearer ' + workload_access_token

    # Set the workload URL and access-token in parsed_globals, so the
    # next CLI operation caller will use them to make the connection to workload.
    parsed_globals.endpoint_url = workload_url
    parsed_globals.access_token = workload_access_token


class WorkloadServiceDiscovery(object):
    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        if parsed_globals.access_token:
            # access-token was explicitly set by user,
            # skip the workload service-discovery
            LOG.debug('Skip workload service-discovery')
            return True

        LOG.debug('Run workload service-discovery')
        service_name = operation_model.service_model.service_name
        if not service_name:
            raise WorkloadServiceDiscoveryError(err_msg='Missing service name')

        if service_name == 'dfworkload':
            workload_name = 'DF'
            environment_crn = parameters.get('environmentCrn', None)
            if not environment_crn:
                raise WorkloadServiceDiscoveryError(err_msg='Missing environment CRN')
        else:
            raise WorkloadServiceDiscoveryError(
                err_msg='Unknown service name \'%s\'' % service_name)

        iam_client = client_creator('iam')
        set_workload_access_token(iam_client, parsed_globals, workload_name,
                                  environment_crn)

        # The command processing is not finished, continue with other operation callers.
        return True

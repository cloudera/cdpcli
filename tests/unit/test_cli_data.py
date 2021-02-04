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

from cdpcli.compat import json
from nose.tools import assert_equal
from tests import executing_in_container


def test_cli_options_match_service_model_validator():
    # We skip this test if we're in a container since the CLI data is not simple
    # to find. This is fine, since no additional coverage is offered by running
    # this on different OSes with different versions of python.
    if executing_in_container():
        return

    cli_data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                 '../../cdpcli/data/cli.json')
    with open(cli_data_path, 'r') as fp:
        cli_data = json.load(fp)
    # The service model validator makes sure that no request object has a property
    # that collides with a top level CLI option since this confuses the CLI parser.
    # The validator does this using a hardcoded list of the top level CLI options,
    # and that list is duplicated here. The source of truth for the CLI options is
    # the cli.json file. We believe these options change infrequently, and so we
    # avoid actually hooking up some build dependency between the validator and the
    # CLI and instead have this unit test which is meant to catch any divergence.
    #
    # So... if you add a CLI option, and this test fails. Update the CLI_OPTIONS
    # list in: services/libs/protocols-yaml-parent/scripts/validate_service_models.py
    # and then update this list here.
    validated_cli_options = ['version',
                             'debug',
                             'no-verify-tls',
                             'ca-bundle',
                             'endpoint-url',
                             'cdp-endpoint-url',
                             'force-ipv4',
                             'access-token',
                             'output',
                             'color',
                             'cli-read-timeout',
                             'cli-connect-timeout',
                             'no-paginate',
                             'auth-config',
                             'profile']
    assert_equal(sorted(cli_data['options'].keys()), sorted(validated_cli_options))

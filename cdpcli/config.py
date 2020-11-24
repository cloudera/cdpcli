# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Modifications made by Cloudera are:
#     Copyright (c) 2020 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import copy

from cdpcli.compat import OrderedDict
from cdpcli.endpoint import DEFAULT_TIMEOUT


class Config(object):
    """Advanced configuration for CDP CLI clients.

    :type connect_timeout: float or int
    :param connect_timeout: The time in seconds till a timeout exception is
        thrown when attempting to make a connection. The default is 60
        seconds.
    :type read_timeout: float or int
    :param read_timeout: The time in seconds till a timeout exception is
        thrown when attempting to read from a connection. The default is
        60 seconds.
    """

    OPTION_DEFAULTS = OrderedDict([
        ('connect_timeout', DEFAULT_TIMEOUT),
        ('read_timeout', DEFAULT_TIMEOUT),
    ])

    def __init__(self, *args, **kwargs):
        self._user_provided_options = self._record_user_provided_options(
            args, kwargs)

        # Merge the user_provided options onto the default options
        config_vars = copy.copy(self.OPTION_DEFAULTS)
        config_vars.update(self._user_provided_options)

        # Set the attributes based on the config_vars
        for key, value in config_vars.items():
            setattr(self, key, value)

    def _record_user_provided_options(self, args, kwargs):
        option_order = list(self.OPTION_DEFAULTS)
        user_provided_options = {}

        # Iterate through the kwargs passed through to the constructor and
        # map valid keys to the dictionary
        for key, value in kwargs.items():
            if key in self.OPTION_DEFAULTS:
                user_provided_options[key] = value
            # The key must exist in the available options
            else:
                raise TypeError(
                    'Got unexpected keyword argument \'%s\'' % key)

        # The number of args should not be longer than the allowed
        # options
        if len(args) > len(option_order):
            raise TypeError(
                'Takes at most %s arguments (%s given)' % (
                    len(option_order), len(args)))

        # Iterate through the args passed through to the constructor and map
        # them to appropriate keys.
        for i, arg in enumerate(args):
            # If it a kwarg was specified for the arg, then error out
            if option_order[i] in user_provided_options:
                raise TypeError(
                    'Got multiple values for keyword argument \'%s\'' % (
                        option_order[i]))
            user_provided_options[option_order[i]] = arg

        return user_provided_options

    def merge(self, other_config):
        """Merges the config object with another config object
        This will merge in all non-default values from the provided config
        and return a new config object
        :type other_config: botocore.config.Config
        :param other config: Another config object to merge with. The values
            in the provided config object will take precedence in the merging
        :returns: A config object built from the merged values of both
            config objects.
        """
        # Make a copy of the current attributes in the config object.
        config_options = copy.copy(self._user_provided_options)

        # Merge in the user provided options from the other config
        config_options.update(other_config._user_provided_options)

        # Return a new config object with the merged properties.
        return Config(**config_options)

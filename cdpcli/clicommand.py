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


class CLICommand(object):

    @property
    def name(self):
        raise NotImplementedError("name")

    @name.setter
    def name(self, value):
        raise NotImplementedError("name")

    @property
    def lineage(self):
        return [self]

    @property
    def lineage_names(self):
        return [cmd.name for cmd in self.lineage]

    def __call__(self, client_creator, args, parsed_globals):
        pass

    def create_help_command(self):
        return None

    @property
    def arg_table(self):
        return {}

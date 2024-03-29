# Copyright 2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import json
import sys

from cdpcli.extensions.arguments import OverrideRequiredArgsArgument
from cdpcli.utils import ArgumentGenerator


def add_generate_skeleton(operation_model, argument_table):
    generate_cli_skeleton_argument = GenerateCliSkeletonArgument(
        operation_model)
    generate_cli_skeleton_argument.add_to_arg_table(argument_table)


class GenerateCliSkeletonArgument(OverrideRequiredArgsArgument):
    """This argument writes a generated JSON skeleton to stdout

    The argument, if present in the command line, will prevent the intended
    command from taking place. Instead, it will generate a JSON skeleton and
    print it to standard output. This JSON skeleton then can be filled out
    and can be used as input to ``--input-cli-json`` in order to run the
    command with the filled out JSON skeleton.
    """
    ARG_DATA = {
        'name': 'generate-cli-skeleton',
        'help_text': 'Prints a sample input JSON to standard output. Note the '
                     'specified operation is not run if this argument is '
                     'specified. The sample input can be used as an argument '
                     'for ``--cli-input-json``.',
        'action': 'store_true',
        'group_name': 'generate_cli_skeleton',
    }

    def __init__(self, operation_model):
        super(GenerateCliSkeletonArgument, self).__init__()
        self._operation_model = operation_model

    def invoke(self,
               client_creator,
               operation_model,
               parameters,
               parsed_args,
               parsed_globals):
        return self._generate_json_skeleton(parsed_args)

    def _generate_json_skeleton(self, parsed_args):

        # Only perform the method if the ``--generate-cli-skeleton`` was
        # included in the command line.
        if getattr(parsed_args, 'generate_cli_skeleton', False):

            # Obtain the model of the operation
            operation_model = self._operation_model

            # Generate the skeleton based on the ``input_shape``.
            argument_generator = ArgumentGenerator()
            operation_input_shape = getattr(operation_model, "input_shape", None)
            # If the ``input_shape`` is ``None``, generate an empty
            # dictionary.
            if operation_input_shape is None:
                skeleton = {}
            else:
                skeleton = argument_generator.generate_skeleton(
                    operation_input_shape)

            # Write the generated skeleton to standard output.
            sys.stdout.write(json.dumps(skeleton, indent=4))
            sys.stdout.write('\n')
            return False
        return True

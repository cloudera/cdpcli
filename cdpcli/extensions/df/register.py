# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from cdpcli.extensions.df import DfExtension
from cdpcli.extensions.df.createdeployment import CreateDeployment
from cdpcli.extensions.df.createdeployment import OPERATION_CLI_NAME \
    as CREATE_DEPLOYMENT_OPERATION_CLI_NAME


def register_extension(operation_callers, operation_model):
    """
    Register an extension to run before or after the CLI command.
    To replace the original CLI caller:
    * operation_callers.insert(0, ReplacementCaller())
    * return False by the ReplacementCaller.invoke(...)
    """
    operation_callers.insert(0, DfExtension())


def register_command(clidriver, service_model, command_table):
    """
    Register an additional command to run.
    """
    command_table[CREATE_DEPLOYMENT_OPERATION_CLI_NAME] = \
        CreateDeployment(clidriver, service_model)

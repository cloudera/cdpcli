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

DEPLOYMENT_FLOW_PARAMETER_GROUP = {
    'type': 'object',
    'description': 'Parameter groups for NiFi flow deployment.',
    'required': ['name', 'parameters'],
    'properties': {
        'name': {
            'type': 'string',
            'description': 'Name for parameter group.'
        },
        'parameters': {
            'type': 'array',
            'description': 'Parameters for the parameter group.',
            'items': {
                '$ref': '#/definitions/DeploymentFlowParameter'
            }
        },
        'inheritedParameterGroups': {
            'type': 'array',
            'description': 'The ids of the inherited parameter groups',
            'items': {
                '$ref': '#/definitions/VersionedParameterGroupReference'
            }
        }
    }
}

DEPLOYMENT_FLOW_PARAMETER = {
    'type': 'object',
    'description': 'Parameter object for the NiFi flow deployment.',
    'required': ['name'],
    'properties': {
        'name': {
            'type': 'string',
            'description': 'Name for the parameter.'
        },
        'value': {
            'type': 'string',
            'description': 'Value for the named parameter.',
            'x-no-paramfile': 'true'
        },
        'assetReferences': {
            'type': 'array',
            'description': 'Local paths of the assets for the named parameter.',
            'items': {
                'type': 'string'
            }
        },
        'sourceParameterGroupId': {
            'type': 'string',
            'description': 'Id of the shared parameter group where '
                           'the value should come from.'
        }
    }
}

DEPLOYMENT_FLOW_PARAMETER_FOR_UPDATE = {
    'type': 'object',
    'description': 'Parameter object for the NiFi flow deployment.',
    'required': ['name'],
    'properties': {
        'name': {
            'type': 'string',
            'description': 'Name for the parameter.'
        },
        'value': {
            'type': 'string',
            'description': 'Value for the named parameter.',
            'x-no-paramfile': 'true'
        },
        'assetReferences': {
            'type': 'array',
            'description': 'Asset references for the named parameter.',
            'items': {
                '$ref': '#/definitions/AssetReference'
            }
        },
        'sourceParameterGroupId': {
            'type': 'string',
            'description': 'Id of the shared parameter group where '
                           'the value should come from.'
        }
    }
}

DEPLOYMENT_FLOW_PARAMETER_ASSET_REFERENCE = {
    'type': 'object',
    'description': 'A reference to an asset used in a flow parameter.',
    'properties': {
        'name': {
            'type': 'string',
            'description': 'The name of the asset. This should be provided if there '
                           'are no updates to the asset.'
        },
        'path': {
            'type': 'string',
            'description': 'The path of the asset. This should be provided if there '
                           'are updates to the asset.'
        }
    }
}

DEPLOYMENT_KEY_PERFORMANCE_INDICATOR = {
    'type': 'object',
    'description': 'Key Performance Indicators for the deployment.',
    'required': ['metricId'],
    'properties': {
        'metricId': {
            'type': 'string',
            'description': 'Unique identifier for the metric object.'
        },
        'componentId': {
            'type': 'string',
            'description': 'Identifier for the NiFi component.'
        },
        'alert': {
            '$ref': '#/definitions/DeploymentAlert'
        }
    }
}

DEPLOYMENT_ALERT = {
    'type': 'object',
    'properties': {
        'thresholdMoreThan': {
            '$ref': '#/definitions/DeploymentAlertThreshold',
            'description': 'The threshold above which alerts should be triggered.'
        },
        'thresholdLessThan': {
            '$ref': '#/definitions/DeploymentAlertThreshold',
            'description': 'The threshold below which alerts should be triggered.'
        },
        'frequencyTolerance': {
            '$ref': '#/definitions/DeploymentFrequencyTolerance'
        }
    }
}

DEPLOYMENT_ALERT_THRESHOLD = {
    'type': 'object',
    'properties': {
        'unitId': {
            'type': 'string',
            'description': 'The unit identifier for the alert threshold.'
        },
        'value': {
            'type': 'double',
            'description': 'The numeric value for the alert threshold.'
        }
    }
}

DEPLOYMENT_FREQUENCY_TOLERANCE = {
    'type': 'object',
    'description': 'The frequency tolerance for the Key Performance Indicator.',
    'properties': {
        'value': {
            'type': 'double',
            'description': 'The amount of time before generating an alert.'
        },
        'unit': {
            'type': 'object',
            'description': 'The time unit for associated value number.',
            'properties': {
                'id': {
                    'type': 'string',
                    'enum': [
                        'SECONDS',
                        'MINUTES',
                        'HOURS',
                        'DAYS'
                    ]
                }
            }
        }
    }
}

LISTEN_COMPONENT = {
    'type': 'object',
    'required': ['protocol', 'port'],
    'properties': {
        'protocol': {
            'type': 'string',
            'description': 'Inbound protocol.',
            'enum': ['TCP', 'UDP']
        },
        'port': {
            'type': 'string',
            'description': 'Inbound port.'
        }
    },
    'description': 'Provides subset of metadata of a Listen* component.'
}

AWS_NODE_STORAGE_PROFILE = {
    'type': 'object',
    'properties': {
      'repoSize': {
        'type': 'integer',
        'description': 'The size of the repository in GB.'
      },
      'iops': {
        'type': 'string',
        'description': 'The IOPS of the repository.'
      },
      'throughput': {
        'type': 'string',
        'description': 'The throughput of the repository.'
      }
    },
    'description': 'Custom AWS node storage parameters.'
}

PARAMETER_GROUP_REFERENCES = {
    'type': 'object',
    'description': 'A reference to the latest version of a shared parameter group',
    'required': ['groupId'],
    'properties': {
        'groupId': {
            'type': 'string',
            'description': 'The id of the parameter group'
        }
    }
}

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

import copy


SAMPLE_DESCRIBE_CLUSTER = {
    "cluster":
        {
            "status": "CREATED",
            "serviceType": "HIVE_ON_SPARK",
            "networkSettingsName": "net-settings-2",
            "workersGroupSize": 4,
            "clusterName": "cloudtrail-analyze-final",
            "crn": "crn:cws:dataeng:us-west-1:"
                   "id1:cluster:"
                   "cloudtrail-analyze-final/"
                   "id2",
            "cloudAccessAccountName": "caa2",
            "instanceSettingsName": "instance-settings-2",
            "creationDate": "2016-10-21T20:39:35.446000+00:00",
            "clouderaManagerEndpoint": {
                "port": 7180,
                "privateIpAddress": "162.88.88.88",
                "publicIpAddress": "10.88.88.88",

            },
            "instanceType": "c4.2xlarge",
            "cdhVersion": "CDH58"
        }
}

SAMPLE_DESCRIBE_SECURE_CLUSTER = copy.deepcopy(SAMPLE_DESCRIBE_CLUSTER)
SAMPLE_DESCRIBE_SECURE_CLUSTER["cluster"]["clouderaManagerEndpoint"]["port"] = 7183

# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

import enum
import re


class DeploymentType(enum.Enum):
    PRIVATE_CLOUD = 'private-cloud'
    PUBLIC_CLOUD = 'public-cloud'


class ClassifyDeployment:

    def __init__(self, endpoint_url=None):
        self._endpoint_url = endpoint_url
        # As mentioned in configure.py, Public Cloud users may leave endpoint_url entry
        # blank, hence clarifying empty endpoint_url as Public Cloud.
        # Private Cloud users must enter a valid URL.
        if not self._endpoint_url:
            self._deployment = DeploymentType.PUBLIC_CLOUD
        else:
            self._deployment = self._deduce_deployment()

    def set_endpoint_url(self, endpoint_url):
        # Update instance's URL
        if not endpoint_url:
            self._deployment = DeploymentType.PUBLIC_CLOUD
        elif endpoint_url != self._endpoint_url:
            self._endpoint_url = endpoint_url
            self._deployment = self._deduce_deployment()

    def get_deployment_type(self):
        # Returns DeploymentType as an enum constant
        return self._deployment

    def _deduce_deployment(self):
        # Following console/cdp-js/lib/appConfigBase.ts
        # Following regular expression gives a clear identification of a public cloud
        # URL including different environments (int, dev, stage, private stack, etc.)
        #
        # for eg. Private Stack - https://console.cdp-priv.mow-dev.cloudera.com
        #         service URL - https://%sapi.thunderhead-int.cloudera.com
        if re.match('.+(altus|cdp|dev|int|stage).cloudera.com', self._endpoint_url):
            return DeploymentType.PUBLIC_CLOUD
        else:
            return DeploymentType.PRIVATE_CLOUD

    # Make individual assert methods for each environment type
    def is_private_cloud(self):
        return self._deployment == DeploymentType.PRIVATE_CLOUD

    def is_public_cloud(self):
        return self._deployment == DeploymentType.PUBLIC_CLOUD

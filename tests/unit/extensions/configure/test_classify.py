# Copyright (c) 2021 Cloudera, Inc. All rights reserved.

from cdpcli.extensions.configure import classify
from cdpcli.extensions.configure.classify import DeploymentType
from tests import unittest


class TestClassifyDeployment(unittest.TestCase):

    def setUp(self):
        self.classifier = classify.ClassifyDeployment()

    def test_empty_endpoint_url(self):
        # Tests blank endpoint url entry during configure
        # Blank endpoint url indicates Public Cloud as per directives
        # provided to user in configure.py prompt text
        self.classifier.set_endpoint_url("")
        self.assertEqual(self.classifier.get_deployment_type(),
                         DeploymentType.PUBLIC_CLOUD)

    # Next two functions to test if URL entered is classified correctly
    # Tests function calls that return classified deployment type

    def test_classify_with_public_cloud_url(self):
        public_urls = [
            # Examples for service, env URLs
            # Public Cloud URLs follow the same format for dev/int/stage/prod(altus,cdp)
            "https://console.mow-dev.cloudera.com/",
            "http://console.thunderhead-int.cloudera.com/",
            "https://iamapi.thunderhead-stage.cloudera.com",
            "https://cloudera.cdp.mow-int.cloudera.com/",
            "https://cloudera.cdp.mow-int.cloudera.com/",
            # Example for Private Stack
            "https://console.cdp-priv.mow-dev.cloudera.com/cloud/environments/list"
            # Allow local testing via run.sh or run-with-backend.sh
            "http://localhost:8982"
        ]
        for url in public_urls:
            self.classifier.set_endpoint_url(url)
            self.assertEqual(self.classifier.get_deployment_type(),
                             DeploymentType.PUBLIC_CLOUD)
            self.assertEqual(self.classifier.is_public_cloud(), True)

    def test_classify_with_private_cloud_url(self):
        self.classifier.set_endpoint_url(
            "console-fake-priv.apps.cp-dev-02.kcloud.cloudera.com"
        )
        self.assertEqual(self.classifier.get_deployment_type(),
                         DeploymentType.PRIVATE_CLOUD)
        self.assertEqual(self.classifier.is_private_cloud(), True)
